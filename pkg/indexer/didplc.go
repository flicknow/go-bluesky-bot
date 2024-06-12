package indexer

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type didPlcRecord struct {
	Did       string `json:"did"`
	CreatedAt string `json:"createdAt"`
}

func LookupDidPlcCreatedAt(did string) (int64, error) {
	url := fmt.Sprintf("https://plc.directory/%s/log/audit", did)
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return 0, nil
	}

	records := make([]*didPlcRecord, 0)
	body, err := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &records); err != nil {
		return 0, fmt.Errorf("error parsing plc directory record: %w\n%s", err, string(body))
	}

	createdAt := records[0].CreatedAt
	t, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return 0, fmt.Errorf("error parsing createdAt %s for plc did %s: %w", createdAt, did, err)
	}

	return t.UTC().Unix(), nil
}
