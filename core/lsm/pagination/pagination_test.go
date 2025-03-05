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
	setUp := func(threshold int, items []types.Item) *lsm.LSMTree {
		os.Setenv("CONFIG_PATH", filepath.Join(util.GetProjectDir(), "config/test.yaml"))

		cfg := config.MustLoad()
		cfg.SetThreshold(threshold)

		lsm := lsm.NewLSMTree(cfg)
		for _, item := range items {
			lsm.Put(item.Key, item.Value)
		}
		return lsm
	}

	assertItems := func(t testing.TB, items []types.Item, expectedItems []types.Item) {
		t.Helper()

		assert.Equal(t, len(expectedItems), len(items))
		for i, expected := range expectedItems {
			assert.Equal(t, expected.Key, items[i].Key)
			assert.Equal(t, expected.Value, items[i].Value)
		}

		tests.ClearTestData()
	}

	t.Run("get up to 10 records", func(t *testing.T) {
		lsm := setUp(2, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
		})

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
		})
	})

	t.Run("skip 1 record", func(t *testing.T) {
		lsm := setUp(2, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
		})

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 1})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "profile", Value: "dev"},
		})
	})

	t.Run("get records from memtable and sstables", func(t *testing.T) {
		lsm := setUp(2, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
			{Key: "scheduling.enabled", Value: "false"},
		})

		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
			{Key: "scheduling.enabled", Value: "false"},
		})
	})

	t.Run("handle deleted in memtable value", func(t *testing.T) {
		lsm := setUp(2, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
		})

		lsm.Delete("level")
		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "profile", Value: "dev"},
		})
	})

	t.Run("handle updated in memtable value", func(t *testing.T) {
		lsm := setUp(2, []types.Item{
			{Key: "level", Value: "info"},
			{Key: "profile", Value: "dev"},
		})

		lsm.Put("level", "warn")
		items, err := pagination.Paginate(lsm, pagination.Page{Limit: 10, Offset: 0})

		assert.Nil(t, err)
		assertItems(t, items, []types.Item{
			{Key: "level", Value: "warn"},
			{Key: "profile", Value: "dev"},
		})
	})

	tests.ClearTestData()
}
