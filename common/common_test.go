package common

import "testing"

func TestGetPostgresPath(t *testing.T) {
	path := Which("postgres")
	if string(path) != "/usr/local/pgsql/bin/postgres" {
		t.Fatalf("Error to find postgres path: %s", path)
	}
}
func TestGetNonexistingBin(t *testing.T) {
	path := Which("postg")
	if len(path) != 0 {
		t.Fatal("Error get non existing bin")
	}
}
