package river

import (
	"strings"

	"github.com/go-mysql-org/go-mysql/schema"
)

// compiledMapping 是 Rule.FieldMapping 预编译后的快查结构。
// 原实现在每行都要遍历整个 FieldMapping，复杂度 O(N×M)，列多或映射多时热路径很贵；
// 这里在 prepare 阶段反向索引为 O(1) map，同步路径 CPU 明显下降。
type compiledMapping struct {
	esField   string
	fieldType string
}

// Rule is the rule for how to sync data from MySQL to ES.
// If you want to sync MySQL data into elasticsearch, you must set a rule to let use know how to do it.
// The mapping rule may thi: schema + table <-> index + document type.
// schema and table is for MySQL, index and document type is for Elasticsearch.
type Rule struct {
	Schema string `toml:"schema"`
	Table  string `toml:"table"`
	Index  string `toml:"index"`
	// Deprecated: ES 7+ 已废弃 _type，ES 9 完全不接受。
	// 保留字段仅为应对历史 toml 。
	Type   string   `toml:"type"`
	Parent string   `toml:"parent"`
	ID     []string `toml:"id"`

	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	// MySQL table information
	TableInfo *schema.Table

	//only MySQL fields in filter will be synced , default sync all fields
	Filter []string `toml:"filter"`

	// Elasticsearch pipeline
	// To pre-process documents before indexing
	Pipeline string `toml:"pipeline"`

	// compiled 预编译的 MySQL→ES 字段映射表，key 为 MySQL 列名。prepare 阶段填充。
	compiled map[string]compiledMapping
	// filterSet 预计算的白名单集合；nil 代表不过滤（与原语义一致）。
	filterSet map[string]struct{}
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table

	lowerTable := strings.ToLower(table)
	r.Index = lowerTable
	r.Type = lowerTable

	r.FieldMapping = make(map[string]string)

	return r
}

func (r *Rule) prepare() error {
	if r.FieldMapping == nil {
		r.FieldMapping = make(map[string]string)
	}

	if len(r.Index) == 0 {
		r.Index = r.Table
	}

	if len(r.Type) == 0 {
		r.Type = r.Index
	}

	// ES must use a lower-case Type
	// Here we also use for Index
	r.Index = strings.ToLower(r.Index)
	r.Type = strings.ToLower(r.Type)

	r.buildLookup()
	return nil
}

// buildLookup 从 FieldMapping / Filter 预编译快查结构，热路径避免重复解析。
// 同一个 MySQL 列被映射多次时以最后一个为准（与原实现语义一致，原实现同样是后者覆盖）。
func (r *Rule) buildLookup() {
	compiled := make(map[string]compiledMapping, len(r.FieldMapping))
	for k, v := range r.FieldMapping {
		mysqlCol, esField, fieldType := splitFieldDef(k, v)
		compiled[mysqlCol] = compiledMapping{esField: esField, fieldType: fieldType}
	}
	r.compiled = compiled

	if len(r.Filter) == 0 {
		r.filterSet = nil
		return
	}
	set := make(map[string]struct{}, len(r.Filter))
	for _, f := range r.Filter {
		set[f] = struct{}{}
	}
	r.filterSet = set
}

// splitFieldDef 处理 "mysql_col" -> "es_field[,fieldType]" 的配置语法。
// 与原 River.getFieldParts 语义完全一致，只是提前干到 prepare 阶段一次性处理。
func splitFieldDef(k, v string) (mysqlCol, esField, fieldType string) {
	parts := strings.SplitN(v, ",", 2)
	mysqlCol = k
	esField = parts[0]
	if esField == "" {
		esField = mysqlCol
	}
	if len(parts) == 2 {
		fieldType = parts[1]
	}
	return
}

// CheckFilter checkers whether the field needs to be filtered.
// 使用预编译的 filterSet。默认重建一下 filterSet 以兼容测试中未调 prepare 的场景。
func (r *Rule) CheckFilter(field string) bool {
	if len(r.Filter) == 0 {
		return true
	}
	if r.filterSet == nil {
		// 服务启动后调到 prepare 一般会填充，这里是防御性 fallback
		set := make(map[string]struct{}, len(r.Filter))
		for _, f := range r.Filter {
			set[f] = struct{}{}
		}
		r.filterSet = set
	}
	_, ok := r.filterSet[field]
	return ok
}
