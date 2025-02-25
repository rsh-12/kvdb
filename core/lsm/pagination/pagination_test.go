package pagination_test

import (
	"kvdb/core/lsm"
	"kvdb/core/lsm/pagination"
	"kvdb/internal/config"
	"kvdb/internal/util"
	"kvdb/tests"
	"kvdb/types"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaginate(t *testing.T) {
	setUp := func(threshold int) *lsm.LSMTree {
		os.Setenv("CONFIG_PATH", filepath.Join(util.GetProjectDir(), "config/test.yaml"))

		cfg := config.MustLoad()
		cfg.SetThreshold(threshold)

		lsm := lsm.NewLSMTree(cfg)
		lsm.Put("level", "info")
		lsm.Put("profile", "dev")
		return lsm
	}

	assertItems := func(t testing.TB, items []types.Item, expectedItems []types.Item) {
		t.Helper()

		assert.Equal(t, len(expectedItems), len(items))
		for i, expected := range expectedItems {
			assert.Equal(t, expected.Key, items[i].Key)
			assert.Equal(t, expected.Value, items[i].Value)
		}
	}

	t.Run("get up to 10 records", func(t *testing.T) {
		lsm := setUp(2)

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
		})
	})

	t.Run("skip 1 record", func(t *testing.T) {
		lsm := setUp(2)

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 1})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "profile", Value: "dev"},
		})
	})

	t.Run("get records from memtable and sstables", func(t *testing.T) {
		lsm := setUp(2)
		lsm.Put("scheduling.enabled", "false")

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
			{Key: "scheduling.enabled", Value: "false"},
		})
	})

	t.Run("handle deleted in memtable value", func(t *testing.T) {
		lsm := setUp(2)
		lsm.Delete("level")

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "profile", Value: "dev"},
		})
	})

	tests.ClearTestData()
}
