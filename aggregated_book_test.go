package match

import (
	"errors"
	"testing"

	"github.com/quagmt/udecimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/0x5487/matching-engine/protocol"
)

func TestAggregatedBook_Replay(t *testing.T) {
	ab := NewAggregatedBook()
	require.NotNil(t, ab)

	price1 := udecimal.MustParse("100.5")
	size1 := udecimal.MustParse("1.5")
	size2 := udecimal.MustParse("2.0")
	price2 := udecimal.MustParse("101.0")

	// Helper to check depth
	checkDepth := func(side Side, price, expectedSize udecimal.Decimal) {
		t.Helper()
		s, err := ab.Depth(side, price)
		require.NoError(t, err)
		assert.True(
			t,
			s.Equal(expectedSize),
			"expected %s, got %s for price %s",
			expectedSize.String(),
			s.String(),
			price.String(),
		)
	}

	// 1. Open
	logOpen := &OrderBookLog{
		SeqID: 1,
		Type:  protocol.LogTypeOpen,
		Side:  Buy,
		Price: price1,
		Size:  size1,
	}
	err := ab.Replay(logOpen)
	require.NoError(t, err)
	checkDepth(Buy, price1, size1)
	assert.Equal(t, uint64(1), ab.SequenceID())

	// 2. Amend
	logAmend := &OrderBookLog{
		SeqID:    2,
		Type:     protocol.LogTypeAmend,
		Side:     Buy,
		OldPrice: price1,
		OldSize:  size1,
		Price:    price2,
		Size:     size2,
	}
	err = ab.Replay(logAmend)
	require.NoError(t, err)
	checkDepth(Buy, price1, udecimal.Zero) // Old should be 0
	checkDepth(Buy, price2, size2)         // New should be size2
	assert.Equal(t, uint64(2), ab.SequenceID())

	// 3. Match
	// Suppose a Taker Sell matches against the Maker Buy
	logMatch := &OrderBookLog{
		SeqID: 3,
		Type:  protocol.LogTypeMatch,
		Side:  Sell, // Taker side
		Price: price2,
		Size:  udecimal.MustParse("0.5"),
	}
	err = ab.Replay(logMatch)
	require.NoError(t, err)
	// The taker is Sell, so it matches against Maker Buy. Maker Buy depth should decrease.
	checkDepth(Buy, price2, udecimal.MustParse("1.5"))
	assert.Equal(t, uint64(3), ab.SequenceID())

	// 4. Cancel
	logCancel := &OrderBookLog{
		SeqID: 4,
		Type:  protocol.LogTypeCancel,
		Side:  Buy,
		Price: price2,
		Size:  udecimal.MustParse("1.5"),
	}
	err = ab.Replay(logCancel)
	require.NoError(t, err)
	checkDepth(Buy, price2, udecimal.Zero)
	assert.Equal(t, uint64(4), ab.SequenceID())

	// 5. Reject, Admin, User (Should only increase sequence)
	logReject := &OrderBookLog{SeqID: 5, Type: protocol.LogTypeReject}
	err = ab.Replay(logReject)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), ab.SequenceID())

	logAdmin := &OrderBookLog{SeqID: 6, Type: protocol.LogTypeAdmin}
	err = ab.Replay(logAdmin)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), ab.SequenceID())

	// 6. Deduplication (Same sequence ID)
	err = ab.Replay(logAdmin)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), ab.SequenceID())
}

func TestAggregatedBook_Replay_GapDetection(t *testing.T) {
	ab := NewAggregatedBook()

	// Initial replay
	err := ab.Replay(&OrderBookLog{SeqID: 1, Type: protocol.LogTypeAdmin})
	require.NoError(t, err)

	// Gap detected, but no RebuildFunc
	err = ab.Replay(&OrderBookLog{SeqID: 3, Type: protocol.LogTypeAdmin})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OnRebuild callback not set")

	// Add RebuildFunc
	rebuildCalled := false
	ab.OnRebuild = func() (*Snapshot, error) {
		rebuildCalled = true
		return &Snapshot{
			SequenceID: 2,
			Asks:       []*protocol.DepthItem{},
			Bids:       []*protocol.DepthItem{},
		}, nil
	}

	// Retry, still leaves gap since snapshot only goes up to 2, and we replay 4 (expected 3)
	// Actually, wait, if snapshot returns SequenceID=2, and we replay SeqID=3...
	// Snapshot=2 -> ab.seqID=2
	// log=3 -> 3 <= 2+1 (which is 3), so it's applied correctly!

	rebuildCalled = false
	ab.OnRebuild = func() (*Snapshot, error) {
		rebuildCalled = true
		return &Snapshot{
			SequenceID: 2,
		}, nil
	}
	err = ab.Replay(&OrderBookLog{SeqID: 3, Type: protocol.LogTypeAdmin})
	require.NoError(t, err)
	assert.True(t, rebuildCalled)
	assert.Equal(t, uint64(3), ab.SequenceID())

	// Trigger gap with failed rebuild
	ab.OnRebuild = func() (*Snapshot, error) {
		return nil, errors.New("rebuild failed error")
	}
	err = ab.Replay(&OrderBookLog{SeqID: 5, Type: protocol.LogTypeAdmin})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rebuild failed error")

	// Trigger gap, rebuild successful but gap still exists
	ab.OnRebuild = func() (*Snapshot, error) {
		return &Snapshot{
			SequenceID: 3,
		}, nil
	}
	err = ab.Replay(&OrderBookLog{SeqID: 6, Type: protocol.LogTypeAdmin})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sequence gap still exists after rebuild")
}

func TestAggregatedBook_Replay_NilLog(t *testing.T) {
	ab := NewAggregatedBook()
	err := ab.Replay(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "log is nil")
}

func TestAggregatedBook_Replay_DeduplicateAfterRebuild(t *testing.T) {
	ab := NewAggregatedBook()
	ab.OnRebuild = func() (*Snapshot, error) {
		return &Snapshot{
			SequenceID: 5,
		}, nil
	}

	ab.seqID.Store(1)

	// Log has SeqID = 3, which is a gap from 1.
	// Rebuild brings us to 5.
	// Since 3 <= 5, the log should be deduplicated (ignored), and error should be nil.
	err := ab.Replay(&OrderBookLog{SeqID: 3, Type: protocol.LogTypeAdmin})
	require.NoError(t, err)
	assert.Equal(t, uint64(5), ab.SequenceID())
}
