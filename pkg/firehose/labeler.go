package firehose

import (
	"context"
	"fmt"
	"log"
	"net/url"

	"errors"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
)

var DefaultLabelerHost = "https://mod.bsky.app"

const (
	EvtKindLabel       = "#labels"
	EvtKindLabelerInfo = "#info"
)

type LabelerEvent struct {
	Labels *comatproto.LabelSubscribeLabels_Labels
	Info   *comatproto.LabelSubscribeLabels_Info
	Error  error
	Seq    int64
	Type   string
}

type LabelerFirehose struct {
	s *Subscriber
}

func NewLabelerFirehose(ctx context.Context) *LabelerFirehose {
	modHost, ok := ctx.Value("mod-host").(string)
	if !ok {
		modHost = DefaultLabelerHost
	}

	url, err := url.Parse(modHost)
	if err != nil {
		panic(err)
	}

	scheme := url.Scheme

	addr := ""
	if scheme == "http" {
		addr = "ws://"
	} else if scheme == "https" {
		addr = "wss://"
	} else {
		log.Panicf("unsupported scheme %s for labeler %s", scheme, modHost)
	}
	addr = addr + url.Host + "/xrpc/com.atproto.label.subscribeLabels"

	cursorPath, _ := ctx.Value("mod-cursor").(string)

	return &LabelerFirehose{
		s: NewSubscriber(addr, cursorPath),
	}
}

func (l *LabelerFirehose) Ack(seq int64) {
	l.s.Ack(seq)
}

func (l *LabelerFirehose) proxyStream(sCh <-chan *SubscriberEvent) <-chan *LabelerEvent {
	lCh := make(chan *LabelerEvent, ChannelBuffer)

	lastSeq := l.s.cursor
	go func() {
		var sEvt *SubscriberEvent
		var lEvt *LabelerEvent
		var seq int64
		for sEvt = range sCh {

			lEvt = l.processSubscriberEvent(sEvt)
			if lEvt == nil {
				continue
			}

			seq = lEvt.Seq
			if (lastSeq == 0) && (seq != 0) {
				lastSeq = seq
			}

			if (seq != 0) && ((seq - lastSeq) > 1000) {
				err := fmt.Errorf("%w: skipped too many seqs: went from %d to %d (%d)", ErrFatal, lastSeq, seq, seq-lastSeq)
				lEvt = &LabelerEvent{Error: err, Type: EvtKindError}
			}

			lCh <- lEvt

			if seq != 0 {
				lastSeq = seq
			}

			if (lEvt.Error != nil) && (errors.Is(lEvt.Error, ErrFatal)) {
				break
			}
		}

		close(lCh)
	}()

	return lCh
}

func (l *LabelerFirehose) Start(ctx context.Context) (<-chan *LabelerEvent, error) {
	sCh, err := l.s.Start(ctx)
	if err != nil {
		return nil, err
	}

	return l.proxyStream(sCh), nil
}

func (l *LabelerFirehose) Stop() {
	l.s.Stop()
}

func (l *LabelerFirehose) Restart(ctx context.Context) (<-chan *LabelerEvent, error) {
	sCh, err := l.s.Restart(ctx)
	if err != nil {
		return nil, err
	}

	return l.proxyStream(sCh), nil
}

func (l *LabelerFirehose) processSubscriberEvent(sEvt *SubscriberEvent) *LabelerEvent {
	if sEvt.Error != nil {
		return &LabelerEvent{Error: sEvt.Error, Type: EvtKindError}
	}

	header := sEvt.Header
	if header.Op != events.EvtKindMessage {
		err := fmt.Errorf("unexpected header op: %d", header.Op)
		return &LabelerEvent{Error: err, Type: EvtKindError}
	}

	switch header.MsgType {
	case "#info":
		var evt comatproto.LabelSubscribeLabels_Info
		if err := evt.UnmarshalCBOR(sEvt.Body); err != nil {
			return &LabelerEvent{Error: fmt.Errorf("error unmarshalling label #info: %w", err), Type: EvtKindError}
		}
		return &LabelerEvent{Info: &evt, Type: EvtKindLabelerInfo}
	case "#labels":
		var evt comatproto.LabelSubscribeLabels_Labels
		if err := evt.UnmarshalCBOR(sEvt.Body); err != nil {
			return &LabelerEvent{Error: fmt.Errorf("error unmarshalling #label: %w", err), Type: EvtKindError}
		}
		return &LabelerEvent{Labels: &evt, Seq: evt.Seq, Type: EvtKindLabel}
	}

	return nil
}
