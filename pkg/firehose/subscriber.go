package firehose

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
)

var ChannelBuffer = 100
var ErrFatal = errors.New("FATAL subscriber error")

const (
	EvtKindError = "#error"
)

type SubscriberEvent struct {
	Header *events.EventHeader
	Body   io.Reader
	Error  error
	Type   string
}

type Subscriber struct {
	addr         string
	con          *websocket.Conn
	conCtx       context.Context
	conCtxCancel context.CancelFunc
	cursor       int64
	cursorPath   string
}

func NewSubscriber(addr string, cursorPath string) *Subscriber {
	return &Subscriber{
		addr:       addr,
		cursorPath: cursorPath,
	}
}

func (s *Subscriber) Ack(seq int64) {
	if seq > s.cursor {
		s.cursor = seq

		if (seq % 1000) == 0 {
			s.saveCursor()
		}
	}
}

func (s *Subscriber) loadCursor() error {
	p := s.cursorPath
	if p == "" {
		return nil
	}

	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}

	cursor, err := strconv.ParseInt(strings.TrimSuffix(string(b), "\n"), 10, 64)
	if err != nil {
		return err
	}

	s.cursor = cursor

	return nil
}

func (s *Subscriber) saveCursor() error {
	p := s.cursorPath
	if p == "" {
		return nil
	}

	c := s.cursor
	if c == 0 {
		return nil
	}

	return utils.WriteFile(p, []byte(fmt.Sprintf("%d", c)))
}

func (s *Subscriber) CloseConnection() {
	if s.con != nil {
		con := s.con
		s.con = nil
		con.Close()
	}
	err := s.saveCursor()
	if err != nil {
		fmt.Printf("ERROR saving cursor %d to %s: %+v\n", s.cursor, s.cursorPath, err)
	} else {
		fmt.Printf("SUCCESS saved cursor %d to %s\n", s.cursor, s.cursorPath)
	}
}

func (s *Subscriber) Start(ctx context.Context) (<-chan *SubscriberEvent, error) {
	if (s.cursorPath != "") && (s.cursor == 0) {
		err := s.loadCursor()
		if err != nil {
			return nil, err
		}
	}

	ch := make(chan *SubscriberEvent, ChannelBuffer)

	if s.con == nil {
		conCtx, cancel := context.WithCancel(ctx)
		s.conCtx = conCtx
		s.conCtxCancel = cancel

		con, err := s.startStream(ctx, ch)
		if err != nil {
			return nil, err
		}
		s.con = con
	}

	return ch, nil
}

func (s *Subscriber) Stop() {
	s.CloseConnection()
}

func (s *Subscriber) Restart(ctx context.Context) (<-chan *SubscriberEvent, error) {
	conCtxCancel := s.conCtxCancel
	if conCtxCancel != nil {
		conCtxCancel()
	}
	if s.con != nil {
		s.con.Close()
		s.con = nil
	}
	return s.Start(ctx)
}

func (s *Subscriber) startStream(ctx context.Context, ch chan *SubscriberEvent) (*websocket.Conn, error) {
	addr := s.addr
	c := s.cursor
	d := websocket.DefaultDialer
	if c != 0 {
		addr = fmt.Sprintf("%s?cursor=%d", addr, c)
	}

	con, _, err := d.Dial(addr, http.Header{})
	if err != nil {
		return nil, err
	}

	go func() {
		var err error
		for s.con != nil {
			err = s.consumeStream(ctx, con, ch)

			if err != nil {
				ch <- &SubscriberEvent{Error: fmt.Errorf("%w: %w", ErrFatal, err)}
				break
			}
		}
		close(ch)
	}()
	return con, nil
}

func (s *Subscriber) consumeStream(ctx context.Context, con *websocket.Conn, ch chan *SubscriberEvent) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		t := time.NewTicker(time.Second * 30)
		defer t.Stop()

		var err error
		for {
			select {
			case <-t.C:
				if err = con.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second*10)); err != nil {
					log.Printf("failed to ping: %s", err)
					cancel()
				}
			case <-ctx.Done():
				con.Close()
				return
			}
		}
	}()

	con.SetPingHandler(func(message string) error {
		err := con.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(time.Second*60))
		if err == websocket.ErrCloseSent {
			return nil
		} else if e, ok := err.(net.Error); ok && e.Temporary() {
			return nil
		}
		return err
	})

	con.SetPongHandler(func(_ string) error {
		if err := con.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
			log.Printf("failed to set read deadline: %s", err)
		}

		return nil
	})

	var mt int
	var r io.Reader
	var err error

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		mt, r, err = con.NextReader()
		if err != nil {
			return err
		}

		switch mt {
		default:
			return fmt.Errorf("expected binary message from subscription endpoint")
		case websocket.BinaryMessage:
			// ok
		}

		var header events.EventHeader
		if err = header.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("reading header: %w", err)
		}

		if header.Op == events.EvtKindErrorFrame {
			var errframe events.ErrorFrame
			if err := errframe.UnmarshalCBOR(r); err != nil {
				return err
			}

			if errframe.Error != "" {
				err = errors.New(errframe.Error)
			}
			if errframe.Message != "" {
				if err != nil {
					err = fmt.Errorf("%w: %s", err, errframe.Message)
				} else {
					err = errors.New(errframe.Message)
				}
			}

			ch <- &SubscriberEvent{Error: err, Type: EvtKindError}
		} else {
			buffer := &bytes.Buffer{}
			_, err := io.Copy(buffer, r)
			if err != nil {
				return err
			}

			ch <- &SubscriberEvent{Header: &header, Body: buffer, Type: header.MsgType}
		}
	}
}
