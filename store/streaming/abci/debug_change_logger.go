package abci

import (
	"context"
	"encoding/hex"

	abci "github.com/cometbft/cometbft/abci/types"

	storetypes "cosmossdk.io/store/types"
)

// DebugChangeLogger implements storetypes.ABCIListener and logs a per-block summary
// of KV-store changes at commit time using the app logger at Debug level.
type DebugChangeLogger struct{}

var _ storetypes.ABCIListener = (*DebugChangeLogger)(nil)

func (d *DebugChangeLogger) ListenFinalizeBlock(ctx context.Context, _ abci.RequestFinalizeBlock, _ abci.ResponseFinalizeBlock) error {
	// No-op: we only care about commit-time change sets
	return nil
}

func (d *DebugChangeLogger) ListenCommit(ctx context.Context, _ abci.ResponseCommit, changeSet []*storetypes.StoreKVPair) error {
	// sdk.Context is attached by BaseApp
	sdkCtx := ctx.(storetypes.Context)
	if len(changeSet) == 0 {
		return nil
	}

	// Build a detailed list of all K/V changes in this block.
	type kvlog struct {
		Store string `json:"store"`
		Op    string `json:"op"` // set | delete
		Key   string `json:"key"`
		Value string `json:"value,omitempty"`
		Size  int    `json:"size"` // bytes for key+value on set, key only on delete
	}

	changes := make([]kvlog, 0, len(changeSet))
	totalBytes := 0
	for _, c := range changeSet {
		if c.Delete {
			changes = append(changes, kvlog{
				Store: c.StoreKey,
				Op:    "delete",
				Key:   hex.EncodeToString(c.Key),
				Size:  len(c.Key),
			})
			totalBytes += len(c.Key)
			continue
		}

		b := len(c.Key) + len(c.Value)
		changes = append(changes, kvlog{
			Store: c.StoreKey,
			Op:    "set",
			Key:   hex.EncodeToString(c.Key),
			Value: hex.EncodeToString(c.Value),
			Size:  b,
		})
		totalBytes += b
	}

	// Log full change set at commit time.
	sdkCtx.Logger().Debug("store change set",
		"height", sdkCtx.BlockHeight(),
		"count", len(changes),
		"bytes", totalBytes,
		"changes", changes,
	)

	return nil
}
