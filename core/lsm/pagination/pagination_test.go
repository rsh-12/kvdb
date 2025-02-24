package pagination_test

import (
	"kvdb/core/lsm"
	"kvdb/core/lsm/pagination"
	"kvdb/internal/config"
	"kvdb/internal/util"
	"kvdb/tests"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaginate(t *testing.T) {
	setUp := func() *lsm.LSMTree {
		os.Setenv("CONFIG_PATH", filepath.Join(util.GetProjectDir(), "config/test.yaml"))
		cfg := config.MustLoad()
		cfg.SetThreshold(2)

		lsm := lsm.NewLSMTree(cfg)
		lsm.Put("level", "info")
		lsm.Put("profile", "dev")
		return lsm
	}

	t.Run("get up to 10 records", func(t *testing.T) {
		lsm := setUp()

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assert.Equal(t, 2, len(items))
		assert.Equal(t, "level", items[0].Key)
		assert.Equal(t, "info", items[0].Value)
		assert.Equal(t, "profile", items[1].Key)
		assert.Equal(t, "dev", items[1].Value)
	})

	t.Run("skip 1 record", func(t *testing.T) {
		lsm := setUp()

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 1})

		assert.Nil(t, err)
		assert.Equal(t, 1, len(items))
		assert.Equal(t, "profile", items[0].Key)
		assert.Equal(t, "dev", items[0].Value)
	})

	t.Run("get records from memtable and sstables", func(t *testing.T) {
		lsm := setUp()
		lsm.Put("scheduling.enabled", "false")

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assert.Equal(t, 3, len(items))
		assert.Equal(t, "level", items[0].Key)
		assert.Equal(t, "info", items[0].Value)
		assert.Equal(t, "profile", items[1].Key)
		assert.Equal(t, "dev", items[1].Value)
		assert.Equal(t, "scheduling.enabled", items[2].Key)
		assert.Equal(t, "false", items[2].Value)
	})

	t.Run("handle deleted in memtable value", func(t *testing.T) {
		lsm := setUp()
		lsm.Delete("level")

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assert.Equal(t, 1, len(items))
	})

	tests.ClearTestData()
}
