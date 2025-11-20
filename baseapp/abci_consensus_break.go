//go:build consensus_break_test

package baseapp

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	abci "github.com/cometbft/cometbft/api/cometbft/abci/v1"
)

func (app *BaseApp) FinalizeBlock(req *abci.RequestFinalizeBlock) (res *abci.ResponseFinalizeBlock, err error) {
	defer func() {
		if res == nil {
			return
		}
		// call the streaming service hooks with the FinalizeBlock messages
		for _, streamingListener := range app.streamingManager.ABCIListeners {
			if err := streamingListener.ListenFinalizeBlock(app.finalizeBlockState.Context(), *req, *res); err != nil {
				app.logger.Error("ListenFinalizeBlock listening hook failed", "height", req.Height, "err", err)
			}
		}
	}()

	if app.optimisticExec.Initialized() {
		// check if the hash we got is the same as the one we are executing
		aborted := app.optimisticExec.AbortIfNeeded(req.Hash)
		// Wait for the OE to finish, regardless of whether it was aborted or not
		res, err = app.optimisticExec.WaitResult()

		// only return if we are not aborting
		if !aborted {
			if res != nil {
				appHash := app.workingHash()

				// CONSENSUS BREAK TEST: Inject consensus breaking for the OE path.
				res.AppHash = injectConsensusBreak(app, appHash, req.Height, "Optimistic Execution path")
			}

			return res, err
		}

		// if it was aborted, we need to reset the state
		app.finalizeBlockState = nil
		app.optimisticExec.Reset()
	}

	// if no OE is running, just run the block (this is either a block replay or a OE that got aborted)
	res, err = app.internalFinalizeBlock(context.Background(), req)
	if res != nil {
		appHash := app.workingHash()

		// CONSENSUS BREAK TEST: Inject consensus breaking for the non-OE/aborted OE path.
		res.AppHash = injectConsensusBreak(app, appHash, req.Height, "Normal/Aborted OE path")
	}

	return res, err
}

// injectConsensusBreak is a test helper function to inject non-determinism
// into the app hash, causing a consensus failure at configurable intervals.
// Environment variables: ENABLE_CONSENSUS_BREAK=true, CONSENSUS_BREAK_INTERVAL=N (default: 10)
func injectConsensusBreak(app *BaseApp, appHash []byte, height int64, path string) []byte {
	if os.Getenv("ENABLE_CONSENSUS_BREAK") != "true" {
		return appHash
	}

	interval := int64(10)
	if envInterval := os.Getenv("CONSENSUS_BREAK_INTERVAL"); envInterval != "" {
		if parsed, err := strconv.ParseInt(envInterval, 10, 64); err == nil && parsed > 0 {
			interval = parsed
		}
	}

	if height > 0 && height%interval == 0 {
		timeNano := time.Now().UnixNano()
		timeBytes := []byte(fmt.Sprintf("%d", timeNano))
		modifiedHash := append(appHash, timeBytes...)

		app.logger.Error(
			"consensus break injected for testing",
			"path", path,
			"height", height,
			"interval", interval,
			"original_hash", fmt.Sprintf("%X", appHash),
			"modified_hash", fmt.Sprintf("%X", modifiedHash),
			"time_injected", timeNano,
		)

		return modifiedHash
	}

	return appHash
}
