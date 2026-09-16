package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestShardsForKeyspace_SiblingKeyspacePrefix covers the case where a database
// name ("trengo") shares a prefix with a sibling keyspace ("trengo_etl").
// "show vitess_shards like \"%trengo%\"" returned rows for both keyspaces, and
// TrimPrefix left "trengo_etl/-" in the shard list because that row does not
// start with "trengo/".
func TestShardsForKeyspace_SiblingKeyspacePrefix(t *testing.T) {
	rows := []string{
		"trengo/-",
		"trengo_etl/-",
	}

	t.Run("trengo returns only its own shard, not the sibling's", func(t *testing.T) {
		got := shardsForKeyspace("trengo", rows)
		assert.Equal(t, []string{"-"}, got)
		assert.NotContains(t, got, "trengo_etl/-", "the sibling keyspace's shard leaked in")
		for _, s := range got {
			assert.NotContains(t, s, "/", "shard name still has a keyspace prefix: %q", s)
		}
	})

	t.Run("trengo_etl returns only its own shard, prefix stripped", func(t *testing.T) {
		got := shardsForKeyspace("trengo_etl", rows)
		assert.Equal(t, []string{"-"}, got)
		assert.NotContains(t, got, "trengo/-", "the unsharded sibling keyspace's shard leaked in")
		for _, s := range got {
			assert.NotContains(t, s, "/", "shard name still has a keyspace prefix: %q", s)
		}
	})

	t.Run("unknown keyspace returns nothing", func(t *testing.T) {
		assert.Empty(t, shardsForKeyspace("other", rows))
	})

	t.Run("malformed rows without a separator are skipped", func(t *testing.T) {
		assert.Empty(t, shardsForKeyspace("trengo", []string{"trengo", ""}))
	})

	t.Run("sharded sibling is not mixed into an unsharded prefix keyspace", func(t *testing.T) {
		shardedRows := []string{
			"test/-",
			"test_sharded/-80",
			"test_sharded/80-",
		}
		assert.Equal(t, []string{"-"}, shardsForKeyspace("test", shardedRows))
		assert.Equal(t, []string{"-80", "80-"}, shardsForKeyspace("test_sharded", shardedRows))
	})
}
