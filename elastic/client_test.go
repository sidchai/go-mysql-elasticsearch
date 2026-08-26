package elastic

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/pingcap/check"
)

var host = flag.String("host", "127.0.0.1", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")

func Test(t *testing.T) {
	TestingT(t)
}

type elasticTestSuite struct {
	c *Client
}

var _ = Suite(&elasticTestSuite{})

func (s *elasticTestSuite) SetUpSuite(c *C) {
	cfg := new(ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = ""
	cfg.Password = ""
	client, err := NewClient(cfg)
	c.Assert(err, IsNil)
	s.c = client
}

func (s *elasticTestSuite) TearDownSuite(c *C) {

}

func makeTestData(arg1 string, arg2 string) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = arg1
	m["content"] = arg2

	return m
}

func (s *elasticTestSuite) TestSimple(c *C) {
	index := "dummy"
	docType := "blog"

	//key1 := "name"
	//key2 := "content"

	err := s.c.Update(index, docType, "1", makeTestData("abc", "hello world"))
	c.Assert(err, IsNil)

	exists, err := s.c.Exists(index, docType, "1")
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)

	r, err := s.c.Get(index, docType, "1")
	c.Assert(err, IsNil)
	c.Assert(r.Code, Equals, 200)
	c.Assert(r.ID, Equals, "1")

	err = s.c.Delete(index, docType, "1")
	c.Assert(err, IsNil)

	exists, err = s.c.Exists(index, docType, "1")
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, false)

	items := make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionIndex
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		items[i] = req
	}

	resp, err := s.c.IndexTypeBulk(index, docType, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Code, Equals, 200)
	c.Assert(resp.Errors, Equals, false)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionDelete
		req.ID = id
		items[i] = req
	}

	resp, err = s.c.IndexTypeBulk(index, docType, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Code, Equals, 200)
	c.Assert(resp.Errors, Equals, false)
}

// this requires a parent setting in _mapping
func (s *elasticTestSuite) TestParent(c *C) {
	index := "dummy"
	docType := "comment"
	ParentType := "parent"

	mapping := map[string]interface{}{
		docType: map[string]interface{}{
			"_parent": map[string]string{"type": ParentType},
		},
	}
	err := s.c.CreateMapping(index, docType, mapping)
	c.Assert(err, IsNil)

	items := make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionIndex
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		req.Parent = "1"
		items[i] = req
	}

	resp, err := s.c.IndexTypeBulk(index, docType, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Code, Equals, 200)
	c.Assert(resp.Errors, Equals, false)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Index = index
		req.Type = docType
		req.Action = ActionDelete
		req.ID = id
		req.Parent = "1"
		items[i] = req
	}
	resp, err = s.c.Bulk(items)
	c.Assert(err, IsNil)
	c.Assert(resp.Code, Equals, 200)
	c.Assert(resp.Errors, Equals, false)
}

// TestDoRequestContentType 回归测试：ES 9.x 强制要求 Accept 与 Content-Type 必须成对
// 使用 vendor type 且 compatible-with 版本一致，否则返回 media_type_header_exception (HTTP 400)。
//
// bulk 端点用 ndjson 变体（多 JSON 以 \n 分隔），其他端点用普通 json 变体。
// 真实复现：ES 9.4.1 拒绝 Accept=...compatible-with=8 + Content-Type=application/x-ndjson 组合，
// 报错原文：A compatible version is required on both Content-Type and Accept headers
// if either one has requested a compatible version and the compatible versions must match.
//
// 不进 elasticTestSuite（pingcap/check 集成 suite 需要真 ES），改用标准 testing +
// httptest 本地 mock，CI 可直接跑无需外部依赖。
func TestDoRequestContentType(t *testing.T) {
	const (
		acceptHeader = "application/vnd.elasticsearch+json;compatible-with=8"
		ctBulk       = "application/vnd.elasticsearch+x-ndjson;compatible-with=8"
		ctJSON       = "application/vnd.elasticsearch+json;compatible-with=8"
	)

	cases := []struct {
		name    string
		urlPath string
		wantCT  string
	}{
		{"bulk endpoint", "/_bulk", ctBulk},
		{"index bulk endpoint", "/myindex/_bulk", ctBulk},
		{"bulk with query string", "/_bulk?refresh=true", ctBulk},
		{"single doc endpoint", "/myindex/_doc/1", ctJSON},
		{"mapping endpoint", "/myindex/_mapping", ctJSON},
		// index 名包含 bulk 不应该误判（HasSuffix 比 Contains 更严格）
		{"index name contains bulk", "/_bulk_logs/_doc/1", ctJSON},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotCT, gotAccept string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotCT = r.Header.Get("Content-Type")
				gotAccept = r.Header.Get("Accept")
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			client, err := NewClient(&ClientConfig{Addr: strings.TrimPrefix(srv.URL, "http://")})
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}

			if _, err := client.DoRequest("POST", srv.URL+tc.urlPath, bytes.NewBuffer(nil)); err != nil {
				t.Fatalf("DoRequest: %v", err)
			}

			if gotCT != tc.wantCT {
				t.Errorf("urlPath=%s Content-Type want %q got %q", tc.urlPath, tc.wantCT, gotCT)
			}
			// Accept 头所有端点都必须固定，且与 Content-Type 的 compatible-with 版本匹配，
			// 否则 ES 9 会拒绝（media_type_header_exception）
			if gotAccept != acceptHeader {
				t.Errorf("urlPath=%s Accept want %q got %q", tc.urlPath, acceptHeader, gotAccept)
			}
		})
	}
}
