package lokiclient

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"

	"github.com/techquest-tech/gobatch"
	"go.uber.org/zap"
)

const (
	LokiPushURI = "/loki/api/v1/push"
	// GzipEnabled = true
)

var log = zap.L().With(zap.String("component", "lokiclient"))

var reg = regexp.MustCompile("[^a-zA-Z0-9_]")

func deleteInvalidChar(key string) string {
	return reg.ReplaceAllString(key, "_")
}

type PushConfig struct {
	URL      string
	User     string
	Password string
	Interval string
	Batch    uint //Batch Size
	Retry    uint
	Gzip     bool
}

type PushBody struct {
	Streams []PushItem `json:"streams"`
}

type PushItem struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func NewPushItem(labs map[string]string, lines ...string) PushItem {
	item := PushItem{
		Stream: map[string]string{},
		Values: [][]string{},
	}
	for k, v := range labs {
		item.Stream[deleteInvalidChar(k)] = v
	}

	now := fmt.Sprintf("%d", time.Now().UnixNano())
	for _, l := range lines {
		line := []string{now, l}
		item.Values = append(item.Values, line)
	}
	return item
}

// the real func to push data to loki
func (c PushConfig) lokiJob(ctx context.Context, queue []interface{}) error {
	items := PushBody{
		Streams: []PushItem{},
	}
	for _, q := range queue {
		item, ok := q.(PushItem)
		if !ok {
			// log.Fatalf("Data type error, %T", item)
			log.Error("Data type error, expected PushItem",
				zap.String("dataType", fmt.Sprint("%%", q)),
				zap.Any("data", q),
			)
		}
		items.Streams = append(items.Streams, item)
	}

	rawBody, err := json.Marshal(items)

	if c.Gzip {
		buf := bytes.Buffer{}
		zw := gzip.NewWriter(&buf)
		_, err := zw.Write(rawBody)
		if err != nil {
			return err
		}
		err = zw.Close()
		if err != nil {
			return err
		}
		// log.Info("req body raw/zipped size: ", rawSize, "/", buf.Len())
		rawBody = buf.Bytes()
	}

	if err != nil {
		log.Error("marshal request body failed. err ", zap.Error(err))
	}
	url := c.URL + LokiPushURI

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(rawBody))
	if err != nil {
		log.Error("create http request failed. err ", zap.Error(err))
	}

	req.Header.Add("Content-Type", "application/json")
	if c.Gzip {
		req.Header.Add("Content-Encoding", "gzip")
	}

	if c.User != "" {
		req.SetBasicAuth(c.User, c.Password)
	}

	client := http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		// log.Errorf("request to %s failed. err %v", url, err)
		log.Error("request Loki server failed.", zap.String("url", url), zap.Error(err))
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// log.Error("read http resp failed. err :", err)
		log.Error("read resp failed.", zap.Error(err))
		return err
	}

	switch {
	case resp.StatusCode < 200, resp.StatusCode > 300:
		err = fmt.Errorf("http return error, status code = %d, %s. %s", resp.StatusCode, resp.Status, string(body))
		// log.Error(err)
		log.Error("loki server reply none 200", zap.Int("statusCode", resp.StatusCode),
			zap.String("status", resp.Status))
		return err
	default:
		log.Debug("post to loki done.", zap.ByteString("resp", body))
		return nil
	}
}

func (c PushConfig) NewClient(ctx context.Context) (chan interface{}, error) {
	timeout, err := time.ParseDuration(c.Interval)
	if err != nil {
		return nil, err
	}

	b := gobatch.NewBatcher(c.lokiJob)
	b.MaxRetry = c.Retry
	b.BatchSize = c.Batch
	b.MaxWait = timeout
	return b.Start(ctx)
}
