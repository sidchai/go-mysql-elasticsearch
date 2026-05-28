package river

import (
	"testing"
)

// TestRulePrepareCompiledMapping 验证 P2-1：prepare 后 compiled 应建立 MySQL→ES 反向索引
func TestRulePrepareCompiledMapping(t *testing.T) {
	r := &Rule{
		Schema: "iot_cloud",
		Table:  "api_request_record",
		Index:  "api_request_record",
		FieldMapping: map[string]string{
			"account_id":   "accountId",
			"device_no":    "deviceNo",
			"created_at":   "createdAt,date",
			"empty_target": "", // 空目标 → 用 mysql 列名
		},
	}
	if err := r.prepare(); err != nil {
		t.Fatalf("prepare err: %v", err)
	}

	cases := []struct {
		col       string
		wantES    string
		wantType  string
		wantFound bool
	}{
		{"account_id", "accountId", "", true},
		{"device_no", "deviceNo", "", true},
		{"created_at", "createdAt", "date", true},
		{"empty_target", "empty_target", "", true}, // 空目标 fallback 到 mysql 名
		{"not_mapped", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.col, func(t *testing.T) {
			m, ok := r.compiled[tc.col]
			if ok != tc.wantFound {
				t.Errorf("compiled[%q] found = %v, want %v", tc.col, ok, tc.wantFound)
				return
			}
			if !ok {
				return
			}
			if m.esField != tc.wantES {
				t.Errorf("compiled[%q].esField = %q, want %q", tc.col, m.esField, tc.wantES)
			}
			if m.fieldType != tc.wantType {
				t.Errorf("compiled[%q].fieldType = %q, want %q", tc.col, m.fieldType, tc.wantType)
			}
		})
	}
}

// TestCheckFilter 验证 P2-2：filterSet O(1) 查找语义与原线性扫描一致
func TestCheckFilter(t *testing.T) {
	t.Run("empty_filter_passes_all", func(t *testing.T) {
		r := &Rule{}
		if !r.CheckFilter("any_field") {
			t.Error("empty filter should pass all fields")
		}
	})

	t.Run("whitelist_via_filterSet", func(t *testing.T) {
		r := &Rule{Filter: []string{"id", "device_no"}}
		_ = r.prepare() // 走完 prepare 后 filterSet 已建好
		if !r.CheckFilter("id") {
			t.Error("id should pass")
		}
		if !r.CheckFilter("device_no") {
			t.Error("device_no should pass")
		}
		if r.CheckFilter("password") {
			t.Error("password should be filtered out")
		}
	})

	t.Run("filterSet_lazy_build", func(t *testing.T) {
		// 跳过 prepare 直接用 CheckFilter，验证防御性 fallback
		r := &Rule{Filter: []string{"a", "b"}}
		if !r.CheckFilter("a") {
			t.Error("lazy build: a should pass")
		}
		if r.CheckFilter("c") {
			t.Error("lazy build: c should be filtered")
		}
	})
}

// TestSplitFieldDef 验证字段定义解析逻辑（与原 getFieldParts 等价）
func TestSplitFieldDef(t *testing.T) {
	cases := []struct {
		k, v       string
		wantMysql  string
		wantES     string
		wantType   string
	}{
		{"account_id", "accountId", "account_id", "accountId", ""},
		{"created_at", "createdAt,date", "created_at", "createdAt", "date"},
		{"empty", "", "empty", "empty", ""},                  // 空目标 → fallback
		{"comma_only", ",date", "comma_only", "comma_only", "date"}, // 空 ES 名 + 类型
	}
	for _, tc := range cases {
		t.Run(tc.k, func(t *testing.T) {
			m, e, ft := splitFieldDef(tc.k, tc.v)
			if m != tc.wantMysql || e != tc.wantES || ft != tc.wantType {
				t.Errorf("splitFieldDef(%q,%q) = (%q,%q,%q), want (%q,%q,%q)",
					tc.k, tc.v, m, e, ft, tc.wantMysql, tc.wantES, tc.wantType)
			}
		})
	}
}
