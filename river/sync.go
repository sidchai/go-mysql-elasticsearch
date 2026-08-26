package river

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
	"github.com/sidchai/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-log/log"
)

const (
	fieldTypeList = "list"
	// for the mysql int type to es date type
	// set the [rule.field] created_time = ",date"
	fieldTypeDate = "date"
)

const mysqlDateFormat = "2006-01-02"

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *River
}

// OnRotate 处理 binlog 文件轮换事件。
// go-mysql v1.x+ 在首参新增 *replication.EventHeader，这里未使用。
func (h *eventHandler) OnRotate(_ *replication.EventHeader, e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

// OnTableChanged 表结构变更时触发，上游接口新增 header 首参。
func (h *eventHandler) OnTableChanged(_ *replication.EventHeader, schema, table string) error {
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	return nil
}

// OnDDL 处理 DDL 事件。
// go-mysql v1.x+ 的 canal.EventHandler 接口在第一参数新增了 *replication.EventHeader，
// 这里未使用 header，仅用于满足接口签名。
func (h *eventHandler) OnDDL(_ *replication.EventHeader, nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

// OnXID 事务提交事件，新版接口新增 header 首参。
func (h *eventHandler) OnXID(_ *replication.EventHeader, nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}

	// 首次 OnRow 抵达意味着 binlog 端到端打通，此时才合适设同步状态 1，
	// 避免与 P1-3 以前“未连上却报 1”的假阳状态冲突。
	h.r.markRunning()

	var reqs []*elastic.BulkRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
	}

	// P1-4：避免 syncCh 满后永久阻塞，ctx 取消时必须能退出。
	select {
	case h.r.syncCh <- reqs:
	case <-h.r.ctx.Done():
		return h.r.ctx.Err()
	}

	return h.r.ctx.Err()
}

// OnGTID GTID 事件。
// go-mysql v1.x+ 将原来的 mysql.GTIDSet 变更为 mysql.BinlogGTIDEvent，同时新增 header 首参。
func (h *eventHandler) OnGTID(_ *replication.EventHeader, _ mysql.BinlogGTIDEvent) error {
	return nil
}

// OnPosSynced 位点同步事件，新增 header 首参。
func (h *eventHandler) OnPosSynced(_ *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

// OnRowsQueryEvent 当 binlog_rows_query_log_events=ON 时每条 DML 返回原始 SQL，这里不需要处理，
// 返回 nil 即可。Go-mysql v1.x+ 新增接口，必须实现。
func (h *eventHandler) OnRowsQueryEvent(_ *replication.RowsQueryEvent) error {
	return nil
}

// OnTableNotFound 当 Rows Event 引用的表不存在时触发，默认忽略。
func (h *eventHandler) OnTableNotFound(_ *replication.EventHeader, _ *replication.RowsEvent) error {
	return nil
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}

func (r *River) syncLoop() {
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime.Duration
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	// P2-5：panic recover。任何意外（map nil、越界、类型断言失败）都不应该让进程裸奔，
	// 同时抓住后要设 state=0、cancel 让 main 退出，避免假运行。
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("syncLoop panic: %v\n%s", rec, debug.Stack())
			canalSyncState.Set(0)
			r.cancel()
		}
	}()

	lastSavedTime := time.Now()
	reqs := make([]*elastic.BulkRequest, 0, 1024)

	var pos mysql.Position
	var lastKnownPos mysql.Position // 记录最后一个看到的位点，退出时 final flush 补上

	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				lastKnownPos = v.pos
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []*elastic.BulkRequest:
				reqs = append(reqs, v...)
				needFlush = len(reqs) >= bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			// P1-5：退出前做 final flush。reqs 里不丢在内存，ES 幂等写入，重启后不会重复追赶。
			if len(reqs) > 0 {
				if err := r.doBulk(reqs); err != nil {
					log.Errorf("final flush bulk err: %v", err)
				} else {
					reqs = reqs[:0]
				}
			}
			if lastKnownPos.Name != "" {
				if err := r.master.SaveForce(lastKnownPos, r.syncedGTID()); err != nil {
					log.Errorf("final save position %s err: %v", lastKnownPos, err)
				}
			}
			return
		}

		if needFlush {
			if err := r.doBulk(reqs); err != nil {
				log.Errorf("do ES bulk err %v, close sync", err)
				canalSyncState.Set(0)
				r.cancel()
				return
			}
			reqs = reqs[0:0]
		}

		if needSavePos {
			if err := r.master.Save(pos, r.syncedGTID()); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				canalSyncState.Set(0)
				r.cancel()
				return
			}
		}
	}
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for _, values := range rows {
		id, err := r.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		parentID := ""
		if len(rule.Parent) > 0 {
			if parentID, err = r.getParentID(rule, values, rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		// ES 7+ 已移除 type，不再传 Type 字段
		req := &elastic.BulkRequest{Index: rule.Index, ID: id, Parent: parentID, Pipeline: rule.Pipeline}

		if action == canal.DeleteAction {
			req.Action = elastic.ActionDelete
			esDeleteNum.WithLabelValues(rule.Index).Inc()
		} else {
			r.makeInsertReqData(req, rule, values)
			esInsertNum.WithLabelValues(rule.Index).Inc()
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {
		beforeID, err := r.getDocID(rule, rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		afterID, err := r.getDocID(rule, rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		beforeParentID, afterParentID := "", ""
		if len(rule.Parent) > 0 {
			if beforeParentID, err = r.getParentID(rule, rows[i], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
			if afterParentID, err = r.getParentID(rule, rows[i+1], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		// ES 7+ 已移除 type，不再传 Type 字段
		req := &elastic.BulkRequest{Index: rule.Index, ID: beforeID, Parent: beforeParentID}

		if beforeID != afterID || beforeParentID != afterParentID {
			req.Action = elastic.ActionDelete
			reqs = append(reqs, req)

			req = &elastic.BulkRequest{Index: rule.Index, ID: afterID, Parent: afterParentID, Pipeline: rule.Pipeline}
			r.makeInsertReqData(req, rule, rows[i+1])

			esDeleteNum.WithLabelValues(rule.Index).Inc()
			esInsertNum.WithLabelValues(rule.Index).Inc()
		} else {
			if len(rule.Pipeline) > 0 {
				// Pipelines can only be specified on index action
				r.makeInsertReqData(req, rule, rows[i+1])
				// Make sure action is index, not create
				req.Action = elastic.ActionIndex
				req.Pipeline = rule.Pipeline
			} else {
				r.makeUpdateReqData(req, rule, rows[i], rows[i+1])
			}
			esUpdateNum.WithLabelValues(rule.Index).Inc()
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(time.RFC3339)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(mysqlDateFormat, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(mysqlDateFormat)
		}
	}

	return value
}

func (r *River) makeInsertReqData(req *elastic.BulkRequest, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	req.Action = elastic.ActionIndex

	// P2-1：使用 prepare 阶段预编译的 compiled 快查表，避免热路径上重复解析 FieldMapping。
	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		if m, ok := rule.compiled[c.Name]; ok {
			req.Data[m.esField] = r.getFieldValue(&c, m.fieldType, values[i])
		} else {
			req.Data[c.Name] = r.makeReqColumnData(&c, values[i])
		}
	}
}

func (r *River) makeUpdateReqData(req *elastic.BulkRequest, rule *Rule,
	beforeValues []interface{}, afterValues []interface{}) {
	req.Data = make(map[string]interface{}, len(beforeValues))

	// maybe dangerous if something wrong delete before?
	req.Action = elastic.ActionUpdate

	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		if reflect.DeepEqual(beforeValues[i], afterValues[i]) {
			//nothing changed
			continue
		}
		if m, ok := rule.compiled[c.Name]; ok {
			req.Data[m.esField] = r.getFieldValue(&c, m.fieldType, afterValues[i])
		} else {
			req.Data[c.Name] = r.makeReqColumnData(&c, afterValues[i])
		}
	}
}

// If id in toml file is none, get primary keys in one row and format them into a string, and PK must not be nil
// Else get the ID's column in one row and format them into a string
func (r *River) getDocID(rule *Rule, row []interface{}) (string, error) {
	var (
		ids []interface{}
		err error
	)
	if rule.ID == nil {
		ids, err = rule.TableInfo.GetPKValues(row)
		if err != nil {
			return "", err
		}
	} else {
		ids = make([]interface{}, 0, len(rule.ID))
		for _, column := range rule.ID {
			value, err := rule.TableInfo.GetColumnValue(column, row)
			if err != nil {
				return "", err
			}
			ids = append(ids, value)
		}
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range ids {
		if value == nil {
			return "", errors.Errorf("The %ds id or PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}

func (r *River) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}

	return fmt.Sprint(row[index]), nil
}

// doBulk 封装 ES bulk 写入与退避重试。
// 原实现有两个致命问题：
// 1. 判定写反为 `resp.Code/100 == 2 || resp.Errors`（应为 !=2），导致 5xx/4xx 被静默吞掉；
// 2. 任意错误都返回 nil，外层 syncLoop 认为成功、推进 master.info 位点，最终造成数据丢失。
// 这里重写后：
//   - 仅 2xx 且 Errors=false 才认为成功；
//   - 5xx / 网络错误 → 指数退避重试（默认 5 次，到顶后给外层 cancel）；
//   - 4xx / Errors=true 但不包含可重试状态 → 记录指标 + 上报错误（调用方选择如何处理）。
func (r *River) doBulk(reqs []*elastic.BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	maxRetry := r.c.BulkMaxRetry
	initialBackoff := r.c.BulkRetryInitialBackoff.Duration
	maxBackoff := r.c.BulkRetryMaxBackoff.Duration
	if initialBackoff <= 0 {
		initialBackoff = time.Second
	}
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	backoff := initialBackoff
	var lastErr error
	for attempt := 0; attempt <= maxRetry; attempt++ {
		start := time.Now()
		resp, err := r.es.Bulk(reqs)
		esBulkDuration.Observe(time.Since(start).Seconds())

		if err == nil && resp.Code/100 == 2 && !resp.Errors {
			return nil
		}

		if err != nil {
			lastErr = errors.Trace(err)
			log.Errorf("bulk attempt %d/%d transport err: %v, pos=%s",
				attempt+1, maxRetry+1, err, r.canal.SyncedPosition())
		} else if resp.Code/100 == 5 {
			lastErr = errors.Errorf("bulk http %d (server-side error)", resp.Code)
			log.Errorf("bulk attempt %d/%d http %d, pos=%s",
				attempt+1, maxRetry+1, resp.Code, r.canal.SyncedPosition())
		} else if resp.Code/100 != 2 {
			// 4xx 错误一般不可重试（如请求体不合法、index 设置不允许写入），直接返回
			return errors.Errorf("bulk http %d (client-side, non-retriable)", resp.Code)
		} else {
			// 2xx 但 Errors=true：部分文档出错，逐条记录指标后返回错误（不重试避免反复写那些个已成功的）
			failed := r.recordBulkErrors(resp)
			if failed > 0 {
				return errors.Errorf("bulk has %d failed items", failed)
			}
			return nil
		}

		if attempt >= maxRetry {
			break
		}
		esBulkRetryNum.Inc()
		// 退避期间响应 ctx.Done，避免退出时还在睡
		select {
		case <-time.After(backoff):
		case <-r.ctx.Done():
			return errors.Trace(r.ctx.Err())
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return errors.Annotatef(lastErr, "bulk failed after %d attempts", maxRetry+1)
}

// recordBulkErrors 遇到 ES Errors=true 时走调：逐条记录失败项到指标与日志。
// 返回失败条数，主要用于上层决策是否报错。
func (r *River) recordBulkErrors(resp *elastic.BulkResponse) int {
	failed := 0
	for i := 0; i < len(resp.Items); i++ {
		for action, item := range resp.Items[i] {
			if len(item.Error) > 0 {
				failed++
				esBulkErrorNum.WithLabelValues(action, item.Index, strconv.Itoa(item.Status)).Inc()
				log.Errorf("bulk item failed action=%s index=%s id=%s status=%d error=%s",
					action, item.Index, item.ID, item.Status, string(item.Error))
			}
		}
	}
	return failed
}

// get mysql field value and convert it to specific value to es
func (r *River) getFieldValue(col *schema.TableColumn, fieldType string, value interface{}) interface{} {
	var fieldValue interface{}
	switch fieldType {
	case fieldTypeList:
		v := r.makeReqColumnData(col, value)
		if str, ok := v.(string); ok {
			fieldValue = strings.Split(str, ",")
		} else {
			fieldValue = v
		}

	case fieldTypeDate:
		if col.Type == schema.TYPE_NUMBER {
			col.Type = schema.TYPE_DATETIME

			v := reflect.ValueOf(value)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fieldValue = r.makeReqColumnData(col, time.Unix(v.Int(), 0).Format(mysql.TimeFormat))
			}
		}
	}

	if fieldValue == nil {
		fieldValue = r.makeReqColumnData(col, value)
	}
	return fieldValue
}
