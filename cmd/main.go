package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/flicknow/go-bluesky-bot/blueskybot"
	cli "github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "blueskybot",
		Usage:   "simple bluesky bot",
		Version: "0.0.1",
	}
	app.Commands = []*cli.Command{
		blueskybot.BlockCmd,
		blueskybot.BlueskyBot,
		blueskybot.DelayCmd,
		blueskybot.DehydrateCmd,
		blueskybot.DumpCmd,
		blueskybot.FirstCmd,
		blueskybot.IndexCmd,
		blueskybot.IndexFollowsCmd,
		blueskybot.LookupCmd,
		blueskybot.MigrateCmd,
		blueskybot.MigrateListCmd,
		blueskybot.PruneCmd,
		blueskybot.ServerCmd,
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGUSR1)
		buf := make([]byte, 1<<20)
		for {
			<-sigs
			stacklen := runtime.Stack(buf, true)
			log.Printf("=== received SIGUSR1 ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		}
	}()

	err := app.Run(os.Args)
	if (err != nil) && (!errors.Is(err, context.Canceled)) {
		log.Panicf("%s", err)
	}
}
