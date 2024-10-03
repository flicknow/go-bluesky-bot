package cmd

import (
	"context"
	"fmt"
	"os"

	cli "github.com/urfave/cli/v2"
)

type cliCtxWrapper struct {
	*cli.Context
}

func (ctx *cliCtxWrapper) Value(key any) any {
	if key, ok := key.(string); ok {
		return ctx.Context.Value(key)
	} else {
		return nil
	}
}

func ToContext(ctx *cli.Context) context.Context {
	return &cliCtxWrapper{ctx}
}

var WithDebug = []cli.Flag{
	&cli.StringSliceFlag{
		Name:    "debug",
		Usage:   "enable debugging logs",
		EnvVars: []string{"GO_BLUESKY_DEBUG"},
	},
}

func DebuggingEnabled(ctxRaw interface{}, pkg string) bool {
	ctx, ok := ctxRaw.(context.Context)
	if !ok {
		clictx, ok := ctxRaw.(*cli.Context)
		if ok {
			ctx = ToContext(clictx)
		} else {
			return false
		}
	}

	debugPkgs, ok := ctx.Value("debug").(cli.StringSlice)
	if !ok {
		return false
	}

	for _, debugPkg := range debugPkgs.Value() {
		if debugPkg == pkg {
			return true
		}
	}

	return false
}

var WithClient = CombineFlags(
	&cli.BoolFlag{
		Name:  "dry-run",
		Usage: "do not publish to Bluesky",
		Value: false,
	},
	&cli.StringFlag{
		Name:    "auth",
		Usage:   "path to save ATP auth",
		Value:   fmt.Sprintf("%s/.bsky.auth", os.Getenv("HOME")),
		EnvVars: []string{"ATP_AUTH_FILE"},
	},
	&cli.StringFlag{
		Name:    "pds-host",
		Usage:   "method, hostname, and port of PDS instance",
		Value:   "https://bsky.social",
		EnvVars: []string{"ATP_PDS_HOST"},
	},
	&cli.StringFlag{
		Name:     "username",
		Usage:    "bluesky username",
		Value:    "",
		EnvVars:  []string{"GO_BLUESKY_USERNAME"},
		Required: true,
	},
	&cli.StringFlag{
		Name:     "password",
		Usage:    "bluesky password",
		Value:    "",
		EnvVars:  []string{"GO_BLUESKY_PASSWORD"},
		Required: true,
	},
	WithDebug,
)

var WithDb = CombineFlags(
	&cli.StringFlag{
		Name:    "db-dir",
		Usage:   "db dir path",
		Value:   fmt.Sprintf("%s/.bsky/db", os.Getenv("HOME")),
		EnvVars: []string{"GO_BLUESKY_DB_DIR"},
	},
	&cli.IntFlag{
		Name:    "db-actor-cache-size",
		Usage:   "db actor cache size",
		Value:   500000,
		EnvVars: []string{"GO_BLUESKY_ACTOR_CACHE_SIZE"},
	},
	&cli.IntFlag{
		Name:    "db-follow-cache-size",
		Usage:   "db follow cache size",
		Value:   50000,
		EnvVars: []string{"GO_BLUESKY_FOLLOW_CACHE_SIZE"},
	},
	&cli.IntFlag{
		Name:    "db-label-cache-size",
		Usage:   "db label cache size",
		Value:   50000,
		EnvVars: []string{"GO_BLUESKY_LABEL_CACHE_SIZE"},
	},
	&cli.IntFlag{
		Name:    "db-mmap-size",
		Usage:   "db mmap size",
		Value:   0,
		EnvVars: []string{"GO_BLUESKY_DB_MMAP_SIZE"},
	},
	&cli.StringFlag{
		Name:    "db-synchronous-mode",
		Usage:   "db synchronous mode",
		Value:   "NORMAL",
		EnvVars: []string{"GO_BLUESKY_DB_SYNCHRONOUS_MODE"},
	},
	&cli.IntFlag{
		Name:    "db-wal-autocheckpoint",
		Usage:   "db wal autocheckpoint",
		Value:   0,
		EnvVars: []string{"GO_BLUESKY_DB_WAL_AUTOCHECKPOINT"},
	},
	&cli.BoolFlag{
		Name:    "extended-indexing",
		Usage:   "index likes and reposts and interaction counts",
		Value:   false,
		EnvVars: []string{"GO_BLUESKY_EXTENDED_INDEXING"},
	},
	&cli.Int64Flag{
		Name:     "slow-query-threshold-ms",
		Usage:    "slow query threshold ms",
		Value:    int64(1000),
		EnvVars:  []string{"GO_BLUESKY_SLOW_QUERY_THRESHOLD_MS"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "signing-key",
		Usage:    "signing key for labeler",
		EnvVars:  []string{"GO_BLUESKY_SIGNING_KEY_HEX"},
		Required: true,
	},
	WithDebug,
)

var WithIndexer = CombineFlags(
	&cli.StringFlag{
		Name:    "bgs-host",
		Usage:   "method, hostname, and port of BGS instance",
		Value:   "https://bsky.network",
		EnvVars: []string{"ATP_BGS_HOST"},
	},
	&cli.StringFlag{
		Name:    "cursor",
		Usage:   "path to cursor",
		Value:   fmt.Sprintf("%s/.bsky.cursor", os.Getenv("HOME")),
		EnvVars: []string{"GO_BLUESKY_CURSOR"},
	},
	&cli.StringFlag{
		Name:    "mod-host",
		Usage:   "method, hostname, and port of moderation instance",
		Value:   "https://mod.bsky.app",
		EnvVars: []string{"GO_BLUESKY_MOD_HOST"},
	},
	&cli.StringFlag{
		Name:    "mod-cursor",
		Usage:   "path to cursor for moderation service",
		Value:   fmt.Sprintf("%s/.bsky.mod.cursor", os.Getenv("HOME")),
		EnvVars: []string{"GO_BLUESKY_MOD_CURSOR"},
	},
	&cli.Int64Flag{
		Name:    "keep-days",
		Usage:   "number of days of data to keep",
		Value:   60,
		EnvVars: []string{"GO_BLUESKY_KEEP_DAYS"},
	},
	&cli.IntFlag{
		Name:    "prune-chunk",
		Usage:   "size of chunk to use for pruning",
		Value:   30,
		EnvVars: []string{"GO_BLUESKY_PRUNE_CHUNK"},
	},
	&cli.Int64Flag{
		Name:    "label-tick-minutes",
		Usage:   "number of minutes for each label tick period",
		Value:   1,
		EnvVars: []string{"GO_BLUESKY_LABEL_TICK_MINUTES"},
	},
	&cli.Int64Flag{
		Name:    "pruner-tick-minutes",
		Usage:   "number of minutes for each label pruning period",
		Value:   1,
		EnvVars: []string{"GO_BLUESKY_PRUNER_TICK_MINUTES"},
	},
	WithDebug,
	WithDb,
	WithClient,
)

var WithServer = CombineFlags(
	&cli.StringFlag{
		Name:    "listen",
		Usage:   "listen address for server",
		Value:   ":8080",
		EnvVars: []string{"GO_BLUESKY_LISTEN"},
	},
	&cli.Int64Flag{
		Name:    "max-web-connections",
		Usage:   "max db connections used by web processes",
		Value:   int64(5),
		EnvVars: []string{"GO_BLUESKY_MAX_WEB_CONNECTIONS"},
	},
	&cli.BoolFlag{
		Name:    "enable-follow-lewds-feed",
		Usage:   "enable follow lewds feed",
		Value:   false,
		EnvVars: []string{"GO_BLUESKY_ENABLE_FOLLOW_LEWDS_FEED"},
	},
	&cli.StringFlag{
		Name:    "pinned-post",
		Usage:   "pin a post to all feeds",
		Value:   "",
		EnvVars: []string{"GO_BLUESKY_PINNED_POST"},
	},
	WithDebug,
	WithClient,
	WithIndexer,
)

func uniqueFlags(flags []cli.Flag) []cli.Flag {
	results := make([]cli.Flag, 0, len(flags))
	seen := make(map[cli.Flag]bool)

	for _, flag := range flags {
		if seen[flag] {
			continue
		}
		results = append(results, flag)
		seen[flag] = true
	}

	return results
}

func CombineFlags(flags ...any) []cli.Flag {
	results := make([]cli.Flag, 0, len(flags))
	for _, maybeFlag := range flags {
		if flag, ok := maybeFlag.(cli.Flag); ok {
			results = append(results, flag)
			continue
		} else if flags, ok := maybeFlag.([]cli.Flag); ok {
			for _, flag := range flags {
				results = append(results, flag)
				continue
			}
		}
	}
	return uniqueFlags(results)
}
