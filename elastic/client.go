package elastic

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/juju/errors"
)

// Client is the client to communicate with ES.
// Although there are many Elasticsearch clients with Go, I still want to implement one by myself.
// Because we only need some very simple usages.
type Client struct {
	Protocol string
	Addr     string
	User     string
	Password string

	c *http.Client
}

// ClientConfig is the configuration for the client.
// 安全与可靠性字段均为零值友好：默认 InsecureSkipTLS=false（严格校验），
// RequestTimeout=0 表示使用上层传入的默认值（现为 30s）。
type ClientConfig struct {
	HTTPS    bool
	Addr     string
	User     string
	Password string

	// CAFile 可选 CA 证书路径；不为空时会加载并仅信任该 CA。
	CAFile string
	// InsecureSkipTLS 默认 false，仅联调/自签证书临时场景可设 true。
	InsecureSkipTLS bool
	// RequestTimeout 单次 HTTP 请求超时，防止 ES 挂死时永久阻塞。
	RequestTimeout time.Duration
	// MaxIdleConnsPerHost HTTP 连接池上限，高睁 bulk 场景调高可减少连接重建。
	MaxIdleConnsPerHost int
}

// NewClient 创建 ES 客户端。
// 较之后返回 error 的原因：加载 CA 证书可能失败（文件不存在/格式错误），
// 不能静默丢揉，必须让调用方事先拿到明确错误。
func NewClient(conf *ClientConfig) (*Client, error) {
	c := new(Client)

	c.Addr = conf.Addr
	c.User = conf.User
	c.Password = conf.Password

	timeout := conf.RequestTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	maxIdle := conf.MaxIdleConnsPerHost
	if maxIdle <= 0 {
		maxIdle = 32
	}

	// Transport 参数参考 net/http 默认值但针对高 QPS bulk 场景调优，避免频繁连接重建
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   maxIdle,
		MaxConnsPerHost:       maxIdle * 2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}

	if conf.HTTPS {
		c.Protocol = "https"
		tlsCfg := &tls.Config{
			InsecureSkipVerify: conf.InsecureSkipTLS,
			MinVersion:         tls.VersionTLS12,
		}
		if conf.CAFile != "" {
			caPEM, err := os.ReadFile(conf.CAFile)
			if err != nil {
				return nil, errors.Annotatef(err, "read es ca file %s", conf.CAFile)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, errors.Errorf("invalid ca file %s", conf.CAFile)
			}
			tlsCfg.RootCAs = pool
		}
		tr.TLSClientConfig = tlsCfg
	} else {
		c.Protocol = "http"
	}

	c.c = &http.Client{Transport: tr, Timeout: timeout}
	return c, nil
}

// ResponseItem is the ES item in the response.
// Type 字段在 ES 7+ 不再返回，保留仅为反序列化兼容、老集群场景。
type ResponseItem struct {
	ID      string                 `json:"_id"`
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type,omitempty"`
	Version int                    `json:"_version"`
	Found   bool                   `json:"found"`
	Source  map[string]interface{} `json:"_source"`
}

// Response is the ES response
type Response struct {
	Code int
	ResponseItem
}

// See http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/bulk.html
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
	ActionIndex  = "index"
)

// BulkRequest is used to send multi request in batch.
type BulkRequest struct {
	Action   string
	Index    string
	Type     string
	ID       string
	Parent   string
	Pipeline string

	Data map[string]interface{}
}

func (r *BulkRequest) bulk(buf *bytes.Buffer) error {
	// ES 7+ 已移除 _type，ES 9 完全不接受，这里不再写入 metaData
	// r.Type 字段保留在结构体中仅为向后兼容，不参与请求构造
	meta := make(map[string]map[string]string)
	metaData := make(map[string]string)
	if len(r.Index) > 0 {
		metaData["_index"] = r.Index
	}

	if len(r.ID) > 0 {
		metaData["_id"] = r.ID
	}
	if len(r.Parent) > 0 {
		// ES 6+ 以 routing 替代已废弃的 _parent
		metaData["routing"] = r.Parent
	}
	if len(r.Pipeline) > 0 {
		metaData["pipeline"] = r.Pipeline
	}

	meta[r.Action] = metaData

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	buf.Write(data)
	buf.WriteByte('\n')

	switch r.Action {
	case ActionDelete:
		//nothing to do
	case ActionUpdate:
		doc := map[string]interface{}{
			"doc": r.Data,
		}
		data, err = json.Marshal(doc)
		if err != nil {
			return errors.Trace(err)
		}

		buf.Write(data)
		buf.WriteByte('\n')
	default:
		//for create and index
		data, err = json.Marshal(r.Data)
		if err != nil {
			return errors.Trace(err)
		}

		buf.Write(data)
		buf.WriteByte('\n')
	}

	return nil
}

// BulkResponse is the response for the bulk request.
type BulkResponse struct {
	Code   int
	Took   int  `json:"took"`
	Errors bool `json:"errors"`

	Items []map[string]*BulkResponseItem `json:"items"`
}

// BulkResponseItem is the item in the bulk response.
// Type 字段在 ES 7+ 不再返回，保留仅为反序列化兼容。
type BulkResponseItem struct {
	Index   string          `json:"_index"`
	Type    string          `json:"_type,omitempty"`
	ID      string          `json:"_id"`
	Version int             `json:"_version"`
	Status  int             `json:"status"`
	Error   json.RawMessage `json:"error"`
	Found   bool            `json:"found"`
}

// MappingResponse is the response for the mapping request.
type MappingResponse struct {
	Code    int
	Mapping Mapping
}

// Mapping represents ES mapping.
type Mapping map[string]struct {
	Mappings map[string]struct {
		Properties map[string]struct {
			Type   string      `json:"type"`
			Fields interface{} `json:"fields"`
		} `json:"properties"`
	} `json:"mappings"`
}

// DoRequest sends a request with body to ES.
// 针对 ES 8/9 兼容模式处理：
//   - Accept 与 Content-Type 必须成对使用 vendor type 且 compatible-with 版本一致，
//     ES 9.x 强制校验（仅一边带 compatible-with 会返回 media_type_header_exception，
//     报错原文：A compatible version is required on both Content-Type and Accept headers）
//   - bulk 端点：Content-Type 用 application/vnd.elasticsearch+x-ndjson;compatible-with=8
//   - 其他端点：Content-Type 用 application/vnd.elasticsearch+json;compatible-with=8
//
// compatible-with=8 在 ES 9 表示"按 ES 8 响应格式返回"（保留 fork 仓库的多版本兼容设计），
// 在 ES 8 等同于原生格式（同版本兼容标签 no-op），所以 ES 8/9 集群都能工作。
//
// 形参命名 reqURL 而非 url，避免遮蔽 import 的 net/url 包。
func (c *Client) DoRequest(method string, reqURL string, body *bytes.Buffer) (*http.Response, error) {
	req, err := http.NewRequest(method, reqURL, body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// bulk 端点固定后缀 /_bulk（可能为 /_bulk 或 /{index}/_bulk），
	// 用 url.Parse 取 Path 段判断，防止 query string（如 ?refresh=true）误判
	contentType := "application/vnd.elasticsearch+json;compatible-with=8"
	if parsed, perr := url.Parse(reqURL); perr == nil && strings.HasSuffix(parsed.Path, "/_bulk") {
		contentType = "application/vnd.elasticsearch+x-ndjson;compatible-with=8"
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/vnd.elasticsearch+json;compatible-with=8")
	if len(c.User) > 0 && len(c.Password) > 0 {
		req.SetBasicAuth(c.User, c.Password)
	}
	resp, err := c.c.Do(req)

	return resp, err
}

// Do sends the request with body to ES.
func (c *Client) Do(method string, url string, body map[string]interface{}) (*Response, error) {
	bodyData, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	buf := bytes.NewBuffer(bodyData)
	if body == nil {
		buf = bytes.NewBuffer(nil)
	}

	resp, err := c.DoRequest(method, url, buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	ret := new(Response)
	ret.Code = resp.StatusCode

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret.ResponseItem)
	}

	return ret, errors.Trace(err)
}

// DoBulk sends the bulk request to the ES.
func (c *Client) DoBulk(url string, items []*BulkRequest) (*BulkResponse, error) {
	var buf bytes.Buffer

	for _, item := range items {
		if err := item.bulk(&buf); err != nil {
			return nil, errors.Trace(err)
		}
	}

	resp, err := c.DoRequest("POST", url, &buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	ret := new(BulkResponse)
	ret.Code = resp.StatusCode

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret)
	}

	return ret, errors.Trace(err)
}

// CreateMapping creates a ES mapping.
func (c *Client) CreateMapping(index string, docType string, mapping map[string]interface{}) error {
	reqURL := fmt.Sprintf("%s://%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("HEAD", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	// if index doesn't exist, will get 404 not found, create index first
	if r.Code == http.StatusNotFound {
		_, err = c.Do("PUT", reqURL, nil)

		if err != nil {
			return errors.Trace(err)
		}
	} else if r.Code != http.StatusOK {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}

	// ES 7+ typeless API：/{index}/_mapping，docType 入参保留仅为签名兼容
	_ = docType
	reqURL = fmt.Sprintf("%s://%s/%s/_mapping", c.Protocol, c.Addr,
		url.QueryEscape(index))

	_, err = c.Do("PUT", reqURL, mapping)
	return errors.Trace(err)
}

// GetMapping gets the mapping.
// docType 入参保留仅为签名兼容，ES 7+ 已移除 type
func (c *Client) GetMapping(index string, docType string) (*MappingResponse, error) {
	_ = docType
	reqURL := fmt.Sprintf("%s://%s/%s/_mapping", c.Protocol, c.Addr,
		url.QueryEscape(index))
	buf := bytes.NewBuffer(nil)
	resp, err := c.DoRequest("GET", reqURL, buf)

	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := new(MappingResponse)
	err = json.Unmarshal(data, &ret.Mapping)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret.Code = resp.StatusCode
	return ret, errors.Trace(err)
}

// DeleteIndex deletes the index.
func (c *Client) DeleteIndex(index string) error {
	reqURL := fmt.Sprintf("%s://%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("DELETE", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Get gets the item by id.
// docType 入参保留仅为签名兼容，ES 7+ URL 统一为 /{index}/_doc/{id}
func (c *Client) Get(index string, docType string, id string) (*Response, error) {
	_ = docType
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	return c.Do("GET", reqURL, nil)
}

// Update creates or updates the data.
// docType 入参保留仅为签名兼容
func (c *Client) Update(index string, docType string, id string, data map[string]interface{}) error {
	_ = docType
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	r, err := c.Do("PUT", reqURL, data)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusCreated {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Exists checks whether id exists or not.
// docType 入参保留仅为签名兼容
func (c *Client) Exists(index string, docType string, id string) (bool, error) {
	_ = docType
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	r, err := c.Do("HEAD", reqURL, nil)
	if err != nil {
		return false, err
	}

	return r.Code == http.StatusOK, nil
}

// Delete deletes the item by id.
// docType 入参保留仅为签名兼容
func (c *Client) Delete(index string, docType string, id string) error {
	_ = docType
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	r, err := c.Do("DELETE", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Bulk sends the bulk request.
// only support parent in 'Bulk' related apis
func (c *Client) Bulk(items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/_bulk", c.Protocol, c.Addr)

	return c.DoBulk(reqURL, items)
}

// IndexBulk sends the bulk request for index.
func (c *Client) IndexBulk(index string, items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/_bulk", c.Protocol, c.Addr,
		url.QueryEscape(index))

	return c.DoBulk(reqURL, items)
}

// IndexTypeBulk sends the bulk request for index.
// ES 7+ typeless：docType 入参保留仅为签名兼容，实际走 /{index}/_bulk
func (c *Client) IndexTypeBulk(index string, docType string, items []*BulkRequest) (*BulkResponse, error) {
	_ = docType
	reqURL := fmt.Sprintf("%s://%s/%s/_bulk", c.Protocol, c.Addr,
		url.QueryEscape(index))

	return c.DoBulk(reqURL, items)
}
