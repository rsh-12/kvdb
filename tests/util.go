package tests

import (
	"kvdb/internal/util"
	"os"
	"path/filepath"
)

func ClearTestData() {
	os.RemoveAll(filepath.Join(util.GetProjectDir(), "test_data"))
}
