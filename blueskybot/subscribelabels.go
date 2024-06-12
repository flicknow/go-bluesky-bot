package blueskybot

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/flicknow/go-bluesky-bot/pkg/firehose"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	cli "github.com/urfave/cli/v2"
)

var SubscribeLabelsCmd = &cli.Command{
	Name: "subscribe-labels",
	Action: func(cctx *cli.Context) error {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
		defer stop()

		endpoint := cctx.Args().First()
		subscriber := firehose.NewSubscriber(endpoint, "")
		ch, err := subscriber.Start(ctx)
		if err != nil {
			return err
		}

		for evt := range ch {
			err := processEvent(evt)
			if err != nil {
				return err
			}
		}

		return nil
	},
}

func processEvent(evt *firehose.SubscriberEvent) error {
	if evt.Error != nil {
		return evt.Error
	}

	header := evt.Header
	if header.Op != events.EvtKindMessage {
		return fmt.Errorf("unexpected header op: %d", header.Op)
	}

	switch header.MsgType {
	case "#info":
		var info comatproto.LabelSubscribeLabels_Info
		if err := info.UnmarshalCBOR(evt.Body); err != nil {
			return fmt.Errorf("error unmarshalling label #info: %w", err)
		}
		fmt.Println(utils.Dump(&firehose.LabelerEvent{Info: &info, Type: firehose.EvtKindLabelerInfo}))
	case "#labels":
		var labels comatproto.LabelSubscribeLabels_Labels
		if err := labels.UnmarshalCBOR(evt.Body); err != nil {
			return fmt.Errorf("error unmarshalling #label: %w", err)
		}
		fmt.Println(utils.Dump(&firehose.LabelerEvent{Labels: &labels, Seq: labels.Seq, Type: firehose.EvtKindLabel}))
	}

	return nil
}
