package blueskybot

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/flicknow/go-bluesky-bot/pkg/client"
	"github.com/flicknow/go-bluesky-bot/pkg/cmd"
	"github.com/flicknow/go-bluesky-bot/pkg/dbx"
	"github.com/flicknow/go-bluesky-bot/pkg/indexer"
	"github.com/flicknow/go-bluesky-bot/pkg/ticker"
	"github.com/flicknow/go-bluesky-bot/pkg/utils"
	"github.com/gorilla/websocket"
	cli "github.com/urfave/cli/v2"
	"golang.org/x/sync/semaphore"
)

var upgrader = websocket.Upgrader{}
var ErrUnauthorized = fmt.Errorf("ERROR: UNAUTHORIZED")
var LabelerDid = "did:plc:jcce2sa3fgue4wiocvf7e7xj"

type Server struct {
	Indexer               *indexer.Indexer
	enableFollowLewdsFeed bool
	pinnedPost            string
	server                *http.Server
	sem                   *semaphore.Weighted
	tickermu              sync.Mutex
	tickers               map[*ticker.Ticker]bool
}

func (s *Server) Serve() error {
	return s.server.ListenAndServe()
}

func (s *Server) addTicker(d time.Duration) *ticker.Ticker {
	s.tickermu.Lock()
	defer s.tickermu.Unlock()

	t := ticker.NewTicker(d)
	s.tickers[t] = true

	return t
}

func (s *Server) removeTicker(t *ticker.Ticker) {
	s.tickermu.Lock()
	defer s.tickermu.Unlock()

	delete(s.tickers, t)
}

func (s *Server) stopTickers() {
	s.tickermu.Lock()
	defer s.tickermu.Unlock()

	tickers := s.tickers
	for t := range tickers {
		t.Stop()
		delete(tickers, t)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.stopTickers()
	return s.server.Shutdown(ctx)
}

type feedPost struct {
	Post string `json:"post"`
}

type feedResponse struct {
	Cursor string     `json:"cursor"`
	Feed   []feedPost `json:"feed"`
}

type describeFeedGeneratorFeed struct {
	Uri string `json:"uri"`
}

type describeFeedGeneratorResponse struct {
	Did   string                      `json:"did"`
	Feeds []describeFeedGeneratorFeed `json:"feeds"`
}

func BadRequest(w http.ResponseWriter) {
	w.WriteHeader(400)
}

func ISE(w http.ResponseWriter) {
	w.WriteHeader(500)
}

func Unauthorized(w http.ResponseWriter) {
	w.WriteHeader(401)
}

func writeCorsHeaders(w http.ResponseWriter, r *http.Request) {

	w.Header().Add("Access-Control-Allow-Origin", r.Header.Get("Origin"))
	w.Header().Add("Access-Control-Allow-Methods", "GET")
	w.Header().Add("Access-Control-Allow-Headers", "*")
}

func parseFeedAtUri(uri string) string {
	if len(uri) <= 5 {
		return ""
	}

	parts := strings.SplitN(uri[5:], "/", 3)
	if parts == nil {
		return ""
	} else if len(parts) != 3 {
		return ""
	}

	return parts[2]
}

func (s *Server) generateFeed(w http.ResponseWriter, indexer *indexer.Indexer, did string, label string, compoundCursor string, limitString string, pinnedPost string) {
	if label == "" {
		BadRequest(w)
		return
	}

	if compoundCursor != "" {
		pinnedPost = ""
	}

	var err error
	cacheAge := 15
	var cursor int64 = 0
	parts := strings.SplitN(compoundCursor, "::", 2)
	cursorIsTimestamp := true
	if (parts != nil) && (len(parts) == 2) {
		if parts[1][0] == 'P' {
			parts[0] = parts[1][1:]
			cursorIsTimestamp = false
		}
		cursor, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			log.Printf("WARNING could not parse time part of cursor %s from %s: %+v\n", parts[0], compoundCursor, err)
			err = nil
		} else {
			cacheAge = 600
		}
	} else {
		cursorIsTimestamp = false
		cursor = dbx.SQLiteMaxInt
	}

	limit := 25
	if limitString != "" {
		var err error
		limit, err = strconv.Atoi(limitString)
		if err != nil {
			log.Printf("%+v\n", err)
			BadRequest(w)
			return
		}
		if limit > 25 {
			limit = 25
		}
	}

	if limit <= 1 {
		pinnedPost = ""
	}
	if (pinnedPost != "") && (limit > 1) {
		limit = limit - 1
	}

	var posts []*dbx.PostRow
	vary := ""
	err = dbx.RetryDbIsLocked(func() error {
		err = s.sem.Acquire(context.Background(), 1)
		defer s.sem.Release(1)
		if err != nil {
			return err
		}

		if cursorIsTimestamp {
			postid, err := indexer.Db.Posts.SelectPostIdByEpochAndRkey(cursor, parts[1])
			if err != nil {
				return err
			}
			if postid != 0 {
				cursor = postid
			}
		}

		if label == "lewds" {
			posts = make([]*dbx.PostRow, 0)
			posts, err = indexer.Db.SelectPostsByLabels(cursor, limit, "underwear", "nudity", "porn", "sexual")
		} else if label == "f-lewds" {
			if did == "" {
				return ErrUnauthorized
			}
			if s.enableFollowLewdsFeed {
				posts, err = indexer.Db.SelectPostsByLabelsFollowed(cursor, limit, did, "underwear", "nudity", "porn", "sexual")
				vary = "authorization"
			} else {
				posts = make([]*dbx.PostRow, 0)
			}
		} else if label == "mark" {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectMark(cursor, limit, did)
			vary = "authorization"
		} else if label == "allmentions" {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectMentions(cursor, limit, did)
			vary = "authorization"
		} else if label == "f-allmentions" {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectMentionsFollowed(cursor, limit, did)
			vary = "authorization"
		} else if label == "bangers" {
			posts, err = indexer.Db.SelectBangers(cursor, limit)
		} else if label == "birthdays" {
			posts, err = indexer.Db.SelectBirthdays(cursor, limit)
		} else if label == "f-birthdays" {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectBirthdaysFollowed(cursor, limit, did)
			vary = "authorization"
		} else if label == "dms" {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectDms(cursor, limit, did)
			vary = "authorization"
		} else if label == "mentions" {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectMentions(cursor, limit, did)
			vary = "authorization"
		} else if label == "f-mentions" {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectMentionsFollowed(cursor, limit, did)
			vary = "authorization"
		} else if label == "noskies" {
			posts, err = indexer.Db.SelectPostsByLabels(cursor, limit, "newskie")
		} else if label == "quotes" {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectQuotes(cursor, limit, did)
			vary = "authorization"
		} else if label == "firehose" {
			posts, err = indexer.Db.SelectLatestPosts(cursor, limit)
		} else if (len(label) > 2) && (label[:2] == "f-") {
			if did == "" {
				return ErrUnauthorized
			}
			posts, err = indexer.Db.SelectPostsByLabelsFollowed(cursor, limit, did, label[2:])
			vary = "authorization"
		} else {
			posts, err = indexer.Db.SelectPostsByLabels(cursor, limit, label)
		}

		return err
	})()
	if (err != nil) && errors.Is(err, ErrUnauthorized) {
		Unauthorized(w)
		return
	}
	if err != nil {
		log.Printf("%+v\n", err)
		ISE(w)
		return
	}
	if posts == nil {
		w.WriteHeader(404)
		return
	}

	feed := &feedResponse{}
	if len(posts) > 0 {
		last := posts[len(posts)-1]
		feed.Cursor = fmt.Sprintf("%d::P%d", last.CreatedAt, last.PostId)
	}

	feed.Feed = make([]feedPost, 0)
	if pinnedPost != "" {
		feed.Feed = append(feed.Feed, feedPost{Post: pinnedPost})
	}

	for _, post := range posts {
		feed.Feed = append(feed.Feed, feedPost{Post: post.Uri})
	}

	b, err := json.Marshal(feed)
	if err != nil {
		log.Printf("%+v\n", err)
		ISE(w)
		return
	}

	if label == "firehose" {
		w.Header().Add("cache-control", "no-cache")
	} else {
		if vary != "" {
			w.Header().Add("vary", vary)
		}
		w.Header().Add("cache-control", fmt.Sprintf("public, max-age=%d", cacheAge))
	}

	w.Header().Add("content-type", "application/json; charset=utf-8")
	w.WriteHeader(200)
	w.Write(b)
}

type subJwt struct {
	Iss string `json:"iss"`
	Aud string `json:"aud"`
	Exp int64  `json:"exp"`
}

func getDidFromRequest(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", nil
	}

	parts := strings.Split(header, " ")
	if len(parts) != 2 {
		return "", nil
	}
	if strings.ToLower(parts[0]) != "bearer" {
		return "", nil
	}

	parts = strings.Split(parts[1], ".")
	if len(parts) != 3 {
		return "", nil
	}

	padded := parts[1] + strings.Repeat("=", ((4-(len(parts[1])%4))%4))
	b, err := base64.StdEncoding.DecodeString(padded)
	if err != nil {
		return "", fmt.Errorf("could not parse did from header. got header=%s, part=%s, err=%w", header, parts[1], err)
	}

	jwt := &subJwt{}
	err = json.Unmarshal(b, jwt)
	if err != nil {
		return "", err
	}

	return jwt.Iss, nil
}

func (s *Server) writeSubscribeLabels(c *websocket.Conn, labels []*dbx.CustomLabel) error {
	subscribeLabels := &atproto.LabelSubscribeLabels_Labels{}
	subscribeLabels.Seq = labels[len(labels)-1].CustomLabelId
	subscribeLabels.Labels = make([]*atproto.LabelDefs_Label, 0)

	for _, label := range labels {
		r := bytes.NewReader(label.Cbor)
		subscribeLabel := &atproto.LabelDefs_Label{}
		err := subscribeLabel.UnmarshalCBOR(r)
		if err != nil {
			return err
		}

		subscribeLabels.Labels = append(subscribeLabels.Labels, subscribeLabel)
	}

	buf := new(bytes.Buffer)
	header := events.EventHeader{
		Op:      events.EvtKindMessage,
		MsgType: "#labels",
	}
	err := header.MarshalCBOR(buf)
	if err != nil {
		return err
	}

	err = subscribeLabels.MarshalCBOR(buf)
	if err != nil {
		return err
	}

	return c.WriteMessage(websocket.BinaryMessage, buf.Bytes())
}

type plcDirectoryRecord struct {
	AlsoKnownAs []string `json:"alsoKnownAs"`
	Services    []struct {
		ServiceEndpoint string `json:"serviceEndpoint"`
	} `json:"service"`
}

func lookupPlcDirectoryRecord(did string) (*plcDirectoryRecord, error) {
	url := fmt.Sprintf("https://plc.directory/%s", did)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	record := &plcDirectoryRecord{}
	body, err := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, record); err != nil {
		return nil, fmt.Errorf("error parsing plc directory record: %w\n%s", err, string(body))
	}

	return record, nil
}

func lookupPds(did string) (string, error) {
	record, err := lookupPlcDirectoryRecord(did)
	if err != nil {
		return "", err
	}

	if (record.Services == nil) || (len(record.Services) != 1) {
		return "", fmt.Errorf("could not find service record from %+v", record)
	}

	return record.Services[0].ServiceEndpoint, nil
}

func lookupDidByDns(handle string) (string, error) {
	domain := fmt.Sprintf("_atproto.%s", handle)
	records, err := net.LookupTXT(domain)
	if err != nil {
		return "", fmt.Errorf("could not lookup txt record for %s: %w", domain, err)
	}

	for _, record := range records {
		if (len(record) > 4) && (record[:4] == "did=") {
			return record[4:], nil
		}
	}

	return "", fmt.Errorf("could not find did record for domain %s. found %s", domain, strings.Join(records, ", "))
}

func lookupDidByHttps(handle string) (string, error) {
	url := fmt.Sprintf("https://%s/.well-known/atproto-did", handle)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	did := string(body)
	if resp.StatusCode == 200 {
		if (len(did) > 4) && (did[:4] == "did:") {
			return did, nil
		} else {
			return "", fmt.Errorf("could not parse did for %s from https verification. got %s which is not a did", handle, did)
		}
	} else {
		return "", fmt.Errorf("could not lookup did for %s from https verification. %s returned %s", handle, url, resp.Status)
	}
}

func lookupDid(handle string) (string, error) {
	length := len(handle)
	if (length > 4) && (handle[:4] == "did:") {
		return handle, nil
	}

	if (length > 12) && (handle[(length-12):] == ".bsky.social") {
		return lookupDidByHttps(handle)
	}

	dns, dnsErr := lookupDidByDns(handle)
	if (dnsErr == nil) && (dns != "") {
		return dns, nil
	}

	https, httpsErr := lookupDidByHttps(handle)
	if (httpsErr == nil) && (https != "") {
		return https, nil
	}

	return "", fmt.Errorf("could not lookup did for %s:\n%s\n", dnsErr, httpsErr)
}

func NewServer(ctx context.Context, indexer *indexer.Indexer) *Server {
	addr, _ := ctx.Value("listen").(string)
	maxConn, _ := ctx.Value("max-web-connections").(int64)
	pinnedPost, _ := ctx.Value("pinned-post").(string)

	at := func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s\n", r.Method, r.URL.String())

		re := regexp.MustCompile(`^(/at:?/+)`)
		path := re.ReplaceAllString(r.URL.Path, "")
		did := utils.ParseDid("at://" + path)
		if did == "" {
			BadRequest(w)
			return
		}

		parts := strings.SplitN(path, "/", 3)
		if len(parts) != 3 {
			BadRequest(w)
			return
		}

		client := indexer.Client
		actor, err := client.GetActor(did)
		if err != nil {
			log.Printf("ERROR getting actor %s: %+v\n", did, err)
			ISE(w)
			return
		} else if actor == nil {
			log.Printf("ERROR did %s does not exist\n", did)
			BadRequest(w)
			return
		}

		w.Header().Add("cache-control", "public, max-age=600")
		w.Header().Add("Location", fmt.Sprintf("https://bsky.app/profile/%s/post/%s\n", actor.Handle, parts[2]))
		w.WriteHeader(301)
		return
	}
	pds := func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s\n", r.Method, r.URL.String())

		handle := r.URL.Path[5:]
		did := handle
		if did == "YOUR-HANDLE-HERE" {
			w.Header().Add("cache-control", "public, max-age=600")
			w.Header().Add("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(200)
			w.Write([]byte("Please replace YOUR-HANDLE-HERE in the address bar with your Bluesky handle\n"))
			return
		}

		did, err := lookupDid(did)
		if err != nil {
			log.Printf("%+v\n", err)
			w.Header().Add("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("%s does not appear to be a valid Bluesky handle\n", handle)))
			return
		}

		pds, err := lookupPds(did)
		if err != nil {
			log.Printf("%+v\n", err)
			http.NotFound(w, r)
			return
		}
		if (len(pds) > 8) && (pds[:8] == "https://") {
			pds = pds[8:]
		}

		w.Header().Add("cache-control", "public, max-age=30")
		w.Header().Add("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(200)
		w.Write([]byte(fmt.Sprintf("%s is in %s!\n", handle, pds)))
	}

	s := &Server{
		Indexer:    indexer,
		pinnedPost: pinnedPost,
		sem:        semaphore.NewWeighted(maxConn),
		tickermu:   sync.Mutex{},
		tickers:    make(map[*ticker.Ticker]bool),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s\n", r.Method, r.URL.String())
		if r.Method == "OPTIONS" {
			writeCorsHeaders(w, r)
			w.WriteHeader(200)
			return
		}
	})
	mux.HandleFunc("/at/", at)
	mux.HandleFunc("/at:/", at)
	mux.HandleFunc("/pds/", pds)
	mux.HandleFunc("/skychat/", func(w http.ResponseWriter, r *http.Request) {
		url := r.URL.String()
		log.Printf("%s %s\n", r.Method, url)

		if len(url) < 33 {
			log.Printf("invalid length: %d\n", len(url))
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("%s does not look like a valid bsky.app post url\n", url[9:])))
			return

		}
		if url[:33] != "/skychat/https:/bsky.app/profile/" {
			log.Printf("invalid prefix: %s\n", url[:3])
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("%s does not look like a valid bsky.app post url\n", url[9:])))
			return
		}

		parts := strings.Split(url[33:], "/")
		if len(parts) != 3 {
			log.Printf("invalid suffix: %s\n", url[32:])
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("%s does not look like a valid bsky.app post url\n", url[9:])))
			return
		}

		did, err := lookupDid(parts[0])
		if err != nil {
			log.Printf("%+v\n", err)
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("%s does not look like a valid bsky.app post url\n", url[9:])))
			return
		}

		w.Header().Add("cache-control", "public, max-age=600")
		w.Header().Add("Location", fmt.Sprintf("https://skychat.social/#https://bsky.app/profile/%s/post/%s\n", did, parts[2]))
		w.WriteHeader(301)
		return

	})
	mux.HandleFunc("/did/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s\n", r.Method, r.URL.String())

		handle := r.URL.Path[5:]

		did, err := lookupDid(handle)
		if err != nil {
			log.Printf("%+v\n", err)
			w.Header().Add("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("%s does not appear to be a valid Bluesky handle\n", handle)))
			return
		}

		w.Header().Add("cache-control", "public, max-age=30")
		w.Header().Add("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(200)
		w.Write([]byte(did))
	})
	mux.HandleFunc("/quotes/", func(w http.ResponseWriter, r *http.Request) {
		url := r.URL.String()
		log.Printf("%s %s\n", r.Method, url)

		tmpl, err := template.New("quotes.html").Parse(QuotesHtmlTemplateText)
		if err != nil {
			log.Printf("error processing quotes html template: %s\n", err)
			ISE(w)
			return
		}

		if url == "/quotes/" {
			w.Header().Add("cache-control", "public, max-age=600")
			w.Header().Add("content-type", "text/html; charset=utf-8")
			w.WriteHeader(200)

			tmplParams := &QuotesHtmlParams{Form: true}
			err := tmpl.Execute(w, tmplParams)
			if err != nil {
				log.Printf("error processing quotes html template: %s\n", err)
				ISE(w)
			}

			return
		}

		tmplParams := &QuotesHtmlParams{Url: fmt.Sprintf("https://%s", url[15:])}

		if len(url) < 32 {
			log.Printf("invalid length: %d\n", len(url))
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/html; charset=utf-8")
			w.WriteHeader(400)

			tmplParams.Error = fmt.Sprintf("%s does not look like a valid bsky.app post url\n", url[9:])
			err := tmpl.Execute(w, tmplParams)
			if err != nil {
				log.Printf("error processing quotes html template: %s\n", err)
				ISE(w)
			}

			return
		}
		if url[:32] != "/quotes/https:/bsky.app/profile/" {
			log.Printf("invalid prefix: %s\n", url[:3])
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/html; charset=utf-8")
			w.WriteHeader(400)

			tmplParams.Error = fmt.Sprintf("%s does not look like a valid bsky.app post url\n", url[9:])
			err := tmpl.Execute(w, tmplParams)
			if err != nil {
				log.Printf("error processing quotes html template: %s\n", err)
				ISE(w)
			}

			return
		}

		parts := strings.Split(url[32:], "/")
		if len(parts) != 3 {
			log.Printf("invalid suffix: %s\n", url[32:])
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/html; charset=utf-8")
			w.WriteHeader(400)

			tmplParams.Error = fmt.Sprintf("%s does not look like a valid bsky.app post url\n", url[9:])
			err := tmpl.Execute(w, tmplParams)
			if err != nil {
				log.Printf("error processing quotes html template: %s\n", err)
				ISE(w)
			}

			return
		}

		did, err := lookupDid(parts[0])
		if err != nil {
			log.Printf("%+v\n", err)
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/html; charset=utf-8")
			w.WriteHeader(400)

			tmplParams.Error = fmt.Sprintf("%s does not look like a valid bsky.app post url\n", url[9:])
			err := tmpl.Execute(w, tmplParams)
			if err != nil {
				log.Printf("error processing quotes html template: %s\n", err)
				ISE(w)
			}

			return
		}

		aturi := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", did, parts[2])
		quotes, err := s.Indexer.Db.SelectQuotesForUri(aturi)
		if err != nil {
			log.Printf("%+v\n", err)
			w.Header().Add("cache-control", "public, max-age=30")
			w.Header().Add("content-type", "text/html; charset=utf-8")
			w.WriteHeader(400)

			tmplParams.Error = err.Error()
			err := tmpl.Execute(w, tmplParams)
			if err != nil {
				log.Printf("error processing quotes html template: %s\n", err)
				ISE(w)
			}

			return
		}

		quoteUris := make([]string, 0, len(quotes))
		for _, quote := range quotes {
			did := utils.ParseDid(quote.Uri)
			rkey := utils.ParseRkey(quote.Uri)
			uri := fmt.Sprintf("https://bsky.app/profile/%s/post/%s", did, rkey)
			quoteUris = append(quoteUris, uri)
		}

		w.Header().Add("cache-control", "public, max-age=30")
		w.Header().Add("content-type", "text/html; charset=utf-8")
		w.WriteHeader(200)

		tmplParams.Posts = quoteUris
		err = tmpl.Execute(w, tmplParams)
		if err != nil {
			log.Printf("error processing quotes html template: %s\n", err)
			ISE(w)
		}
	})
	mux.HandleFunc("/xrpc/com.atproto.label.subscribeLabels", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s\n", r.Method, r.URL.String())

		query := r.URL.Query()
		cursorStr := query.Get("cursor")

		var cursor int64 = 0
		var err error
		if cursorStr != "" {
			cursor, err = strconv.ParseInt(cursorStr, 10, 64)
			if err != nil {
				log.Printf("WARNING could not parse time from cursor %s: %+v\n", cursorStr, err)
				err = nil
			}
		}

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer c.Close()

		c.SetPingHandler(func(message string) error {
			err := c.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(time.Second*60))
			if err == websocket.ErrCloseSent {
				return nil
			} else if e, ok := err.(net.Error); ok && e.Temporary() {
				return nil
			}
			return err
		})

		c.SetPongHandler(func(message string) error {
			if err := c.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
				log.Printf("failed to set read deadline: %s", err)
			}

			return nil
		})

		go func() {
			for {
				_, _, err := c.ReadMessage()
				if err != nil {
					fmt.Printf("failed to read message from client: %s\n", err)
					cancel()
					return
				}
			}
		}()

		limit := 25
		db := s.Indexer.Db

		lastId, err := db.SelectLastCustomLabelId()
		if err != nil {
			fmt.Printf("SelectLastCustomLabelId err: %+v\n", err)
			return
		}

		if (cursor == 0) || (cursor > lastId) {
			cursor = lastId - 1
		}

		ticker := s.addTicker(30 * time.Second)
		defer s.removeTicker(ticker)

	BACKFILL:
		for len(s.tickers) != 0 {
			labels, err := db.SelectCustomLabels(cursor, limit)
			if err != nil {
				fmt.Printf("SelectCustomAccountLabels err: %+v\n", err)
				return
			}
			if len(labels) == 0 {
				break BACKFILL
			}

			cursor = labels[len(labels)-1].CustomLabelId

			err = s.writeSubscribeLabels(c, labels)
			if err != nil {
				fmt.Printf("writeSubscribeLabels err: %+v\n", err)
				return
			}

			if len(labels) < limit {
				break BACKFILL
			}
		}
		if len(s.tickers) == 0 {
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for {
					if err := c.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second)); err != nil {
						fmt.Printf("failed to ping client: %s\n", err)
						return
					}

					labels, err := db.SelectCustomLabels(cursor, 25)
					if err != nil {
						fmt.Printf("SelectCustomLabels err: %+v\n", err)
						return
					}
					if len(labels) == 0 {
						break
					}

					cursor = labels[len(labels)-1].CustomLabelId

					err = s.writeSubscribeLabels(c, labels)
					if err != nil {
						fmt.Printf("writeSubscribeLabels err: %+v\n", err)
						return
					}

					if len(labels) < limit {
						break
					}
				}

			}
		}
	})

	mux.HandleFunc("/xrpc/app.bsky.feed.describeFeedGenerator", func(w http.ResponseWriter, r *http.Request) {
		writeCorsHeaders(w, r)

		response := describeFeedGeneratorResponse{Did: "did:web:flicknow.xyz"}
		response.Feeds = []describeFeedGeneratorFeed{
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/allmentions"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/f-allmentions"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/ceusemlimites"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/dms"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/firehose"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/first20"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/bangers"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/birthdays"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/f-birthdays"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/gmgn"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/f-gmgn"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/lewds"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/f-lewds"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/mark"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/mentions"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/f-mentions"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/newskies"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/noskies"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/quotes"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/rembangs"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/renewskies"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/f-renewskies"},
			{Uri: "at://did:web:flicknow.xyz/app.bsky.feed.generator/rude"},
		}

		b, err := json.Marshal(response)
		if err != nil {
			log.Printf("%+v\n", err)
			ISE(w)
			return
		}

		w.Header().Add("cache-control", "public, max-age=300")
		w.Header().Add("content-type", "application/json; charset=utf-8")
		w.WriteHeader(200)
		w.Write(b)

	})
	mux.HandleFunc("/xrpc/app.bsky.feed.getFeedSkeleton", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		writeCorsHeaders(w, r)

		did, err := getDidFromRequest(r)
		if err != nil {
			log.Printf("jwt error: %+v\n", err)
		}

		start := time.Now().UTC()
		defer func() {
			delta := time.Now().UTC().Sub(start).Seconds()
			if did != "" {
				log.Printf("%s %s (%s) %vs\n", r.Method, r.URL.String(), did, delta)
			} else {
				log.Printf("%s %s %vs\n", r.Method, r.URL.String(), delta)
			}
		}()

		feed := query.Get("feed")

		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}

		label := parseFeedAtUri(feed)
		if (label == "newskies") || (label == "newsky") {
			label = "newskie"
		}
		if label == "renewskies" {
			label = "renewskie"
		}

		s.generateFeed(w, indexer, did, label, query.Get("cursor"), query.Get("limit"), pinnedPost)
	})
	mux.HandleFunc("/xrpc/app.bsky.unspecced.getPopular", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		writeCorsHeaders(w, r)

		log.Printf("%s %s\n", r.Method, r.URL.String())
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}

		did, err := getDidFromRequest(r)
		if err != nil {
			log.Printf("jwt error: %+v\n", err)
		}

		s.generateFeed(w, indexer, did, query.Get("label"), query.Get("cursor"), query.Get("limit"), pinnedPost)
	})

	s.server = &http.Server{Addr: addr, Handler: mux}
	return s
}

var ServerCmd = &cli.Command{
	Name:  "server",
	Flags: cmd.WithServer,
	Action: func(cctx *cli.Context) error {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
		defer stop()

		client, err := client.NewClient(cctx)
		if err != nil {
			return err
		}

		indexer, err := indexer.NewIndexer(cmd.ToContext(cctx), client)
		if err != nil {
			return err
		}

		server := NewServer(cmd.ToContext(cctx), indexer)
		go server.Serve()
		defer server.Shutdown(context.Background())

		<-ctx.Done()
		return ctx.Err()
	},
}

type QuotesHtmlParams struct {
	Error string
	Form  bool
	Posts []string
	Url   string
}

var QuotesHtmlTemplateText = `
<!doctype html>
<html lang="en">
	<head>
		<meta charset="utf-8" />
		<meta name="viewport" content="width=device-width, initial-scale=1.0" />
		<title>Bluesky quotes</title>
		<style>
			:root {
				--background-primary: #fafafa;
				--background-secondary: #e5e5e5;
				--text-primary: #000000;
				--text-link: #1d4ed8;
				--divider: #c8c8c8;
			}

			@media (prefers-color-scheme: dark) {
				:root {
					--background-primary: #0a0a0a;
					--background-secondary: #171717;
					--text-primary: #ffffff;
					--text-link: #60a5fa;
					--divider: #404040;
				}
			}

			html {
				background: var(--background-primary);
				color: var(--text-primary);
				color-scheme: light dark;
				font-size: 14px;
				line-height: 1.25rem;
				font-family:
					system-ui,
					-apple-system,
					BlinkMacSystemFont,
					'Segoe UI',
					Roboto,
					Oxygen,
					Ubuntu,
					Cantarell,
					'Open Sans',
					'Helvetica Neue',
					sans-serif;
			}

			body {
				margin: 24px auto;
				padding: 0 16px;
				max-width: 680px;
			}

			h1,
			h2,
			h3,
			h4,
			h5,
			h6,
			p {
				margin-block-start: 1.1rem;
				margin-block-end: 1.1rem;
			}

			h1 {
				font-size: 1.25rem;
			}
			h2 {
				font-size: 1.125rem;
			}

			a {
				color: var(--text-link);
			}

			pre {
				border-radius: 4px;
				background: var(--background-secondary);
				padding: 8px;
				overflow-x: auto;
				font-size: 12px;
			}

			bluesky-post {
				--font-size: 16px;
				margin: 16px auto;
			}

			.bluesky-post-fallback {
				margin: 16px 0;
				border-left: 3px solid var(--divider);
				padding: 4px 8px;
			}
			.bluesky-post-fallback p {
				margin: 0 0 8px 0;
			}
		</style>
		<script type="module" src="https://esm.sh/bluesky-post-embed@~0.1.0"></script>
		<script type="module">
			const dark = matchMedia('(prefers-color-scheme: dark)');

			const update_theme = () => {
				const is_dark = dark.matches;

				for (const node of document.querySelectorAll('bluesky-post')) {
					node.setAttribute('theme', !is_dark ? 'light' : 'dark');
				}
			};

			update_theme();
			dark.addEventListener('change', update_theme);
		</script>
	</head>
	<body>
		{{ if .Form }}
		<form method="post" id="quotes">
			<label for="url">Bluesky Post URL:</label>
			<input type="text" id="url" name="url">
			<button type="submit">Lookup Quotes!</button>
		</form>
		<script>
			const formElement = document.forms['quotes'];

			function lookupQuotes (e) {
				e.preventDefault();

				const data = new FormData(e.target);
				const url = data.get("url");

				formElement.action = document.location.href + url
				formElement.removeEventListener('submit', lookupQuotes);
				formElement.submit();
			}

			formElement.addEventListener('submit', lookupQuotes);
		</script>
		{{ else }}
		{{ if .Url }}
		<h1>Quote Posts for {{.Url}}</h1>
		{{ end }}
		{{ if .Error }}
		<div>
			Error looking up quotes:
			<pre>{{ .Error }}</pre>
		</div>
		{{ end }}
		{{ if .Posts }}
		{{ range .Posts }}
		<bluesky-post src="{{ . }}"></bluesky-post>
		{{ end }}
		{{ else if not .Error }}
		<div>No quotes found yet</div>
		{{ end }}
		{{ end }}
	</body>
</html>
`
