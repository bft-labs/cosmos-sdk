package cmd

import (
	"context"

	cmtcli "github.com/cometbft/cometbft/libs/cli"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"
	serverconfig "github.com/cosmos/cosmos-sdk/server/config"
)

// Execute executes the root command of an application. It handles creating a
// server context object with the appropriate server and client objects injected
// into the underlying stdlib Context. It also handles adding core CLI flags,
// specifically the logging flags. It returns an error upon execution failure.
func Execute(rootCmd *cobra.Command, envPrefix, defaultHome string) error {
	// Create and set a client.Context on the command's Context. During the pre-run
	// of the root command, a default initialized client.Context is provided to
	// seed child command execution with values such as AccountRetriever, Keyring,
	// and a CometBFT RPC. This requires the use of a pointer reference when
	// getting and setting the client.Context. Ideally, we utilize
	// https://github.com/spf13/cobra/pull/1118.
	ctx := CreateExecuteContext(context.Background())

	memCfg := serverconfig.DefaultConfig().MemLogger

	rootCmd.PersistentFlags().String(flags.FlagLogLevel, zerolog.InfoLevel.String(), "The logging level (trace|debug|info|warn|error|fatal|panic|disabled or '*:<level>,<key>:<level>')")
	// NOTE: The default logger is only checking for the "json" value, any other value will default to plain text.
	rootCmd.PersistentFlags().String(flags.FlagLogFormat, "plain", "The logging format (json|plain)")
	rootCmd.PersistentFlags().Bool(flags.FlagLogNoColor, false, "Disable colored logs")
	// Experimental: in-memory compressing logger (similar to CometBFT memlog)
	rootCmd.PersistentFlags().Bool(server.FlagMemLogEnabled, memCfg.Enabled, "Use in-memory compressing logger (experimental)")
	rootCmd.PersistentFlags().Bool(server.FlagMemLogFilter, memCfg.Filter, "Apply allow-list filtering to memlogger output")
	rootCmd.PersistentFlags().String(server.FlagMemLogInterval, memCfg.Interval, "Memlogger flush interval (e.g. \"2s\"); 0 disables time-based flushing")
	rootCmd.PersistentFlags().Int(server.FlagMemLogMemoryLimit, memCfg.MemoryBytes, "Max uncompressed bytes to buffer before flushing to WAL (0 disables size trigger)")
	rootCmd.PersistentFlags().String(server.FlagMemLogOutputDir, memCfg.Dir, "Root directory for memlogger WAL files (defaults to app home)")

	executor := cmtcli.PrepareBaseCmd(rootCmd, envPrefix, defaultHome)
	return executor.ExecuteContext(ctx)
}

// CreateExecuteContext returns a base Context with server and client context
// values initialized.
func CreateExecuteContext(ctx context.Context) context.Context {
	srvCtx := server.NewDefaultContext()
	ctx = context.WithValue(ctx, client.ClientContextKey, &client.Context{})
	ctx = context.WithValue(ctx, server.ServerContextKey, srvCtx)

	return ctx
}
