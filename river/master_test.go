package river

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

const testGTID = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"

func TestMasterInfo_SaveForceLoad_RoundtripGTID(t *testing.T) {
	dir := t.TempDir()
	m, err := loadMasterInfo(dir, false)
	if err != nil {
		t.Fatalf("loadMasterInfo: %v", err)
	}

	pos := mysql.Position{Name: "mysql-bin.000123", Pos: 456}
	if err := m.SaveForce(pos, testGTID); err != nil {
		t.Fatalf("SaveForce: %v", err)
	}

	loaded, err := loadMasterInfo(dir, false)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := loaded.Position()
	if got.Name != pos.Name || got.Pos != pos.Pos {
		t.Errorf("position = %v, want %v", got, pos)
	}
	if loaded.GTIDSet() != testGTID {
		t.Errorf("gtid = %q, want %q", loaded.GTIDSet(), testGTID)
	}
}

func TestMasterInfo_LoadLegacyFile_NoGTIDKey(t *testing.T) {
	dir := t.TempDir()
	legacy := "bin_name = \"mysql-bin.000001\"\nbin_pos = 4\n"
	if err := os.WriteFile(filepath.Join(dir, "master.info"), []byte(legacy), 0644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	m, err := loadMasterInfo(dir, false)
	if err != nil {
		t.Fatalf("loadMasterInfo: %v", err)
	}
	if m.Position().Name != "mysql-bin.000001" || m.Position().Pos != 4 {
		t.Errorf("legacy position not loaded: %+v", m.Position())
	}
	if m.GTIDSet() != "" {
		t.Errorf("legacy gtid_set should be empty, got %q", m.GTIDSet())
	}
}

func TestMasterInfo_SaveEmptyGTID_KeepsPrevious(t *testing.T) {
	m := &masterInfo{GTID: testGTID}
	if err := m.Save(mysql.Position{Name: "mysql-bin.000002", Pos: 8}, ""); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if m.GTIDSet() != testGTID {
		t.Errorf("empty gtid wiped previous set: %q", m.GTIDSet())
	}
	if m.Position().Name != "mysql-bin.000002" {
		t.Errorf("position not updated: %v", m.Position())
	}
}

func TestResolveStartGTID_UsesSavedSet(t *testing.T) {
	r := &River{
		c:      &Config{Flavor: "mysql"},
		master: &masterInfo{GTID: testGTID},
	}
	gset, err := r.resolveStartGTID()
	if err != nil {
		t.Fatalf("resolveStartGTID: %v", err)
	}
	want, err := mysql.ParseGTIDSet("mysql", testGTID)
	if err != nil {
		t.Fatalf("ParseGTIDSet: %v", err)
	}
	if !gset.Equal(want) {
		t.Errorf("gset = %q, want %q", gset.String(), want.String())
	}
}

func TestResolveStartGTID_InvalidSavedSet(t *testing.T) {
	r := &River{
		c:      &Config{Flavor: "mysql"},
		master: &masterInfo{GTID: "not-a-gtid"},
	}
	_, err := r.resolveStartGTID()
	if err == nil {
		t.Fatal("expected parse error")
	}
}
