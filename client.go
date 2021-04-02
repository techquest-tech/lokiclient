package lokiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/techquest-tech/gobatch"
)

const (
	LokiURI = "/loki/api/v1/push"
)

var log = logrus.WithField("component", "lokiClient")

type Config struct {
	URL      string
	Interval string
	Batch    uint //Batch Size
	Retry    uint
}

type ReqBody struct {
	Streams []Item `json:"streams"`
}

type Item struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func LokiItem(labs map[string]string, lines ...string) Item {
	item := Item{
		Stream: labs,
		Values: [][]string{},
	}

	now := fmt.Sprintf("%d", time.Now().UnixNano())
	for _, l := range lines {
		line := []string{now, l}
		item.Values = append(item.Values, line)
	}
	return item
}

//the real func to push data to loki
func (c Config) lokiJob(ctx context.Context, queue []interface{}) error {
	items := ReqBody{
		Streams: []Item{},
	}
	for _, q := range queue {
		item, ok := q.(Item)
		if !ok {
			log.Fatalf("Data type error, %T", item)
		}
		items.Streams = append(items.Streams, item)
	}

	rawBody, err := json.Marshal(items)

	if err != nil {
		log.Fatal("marshal request body failed. err ", err)
	}
	url := c.URL + LokiURI
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(rawBody))
	if err != nil {
		log.Fatal("create http request failed. err ", err)
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Encoding", "gzip")

	client := http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Errorf("request to %s failed. err %v", url, err)
		return err
	}

	switch {
	case resp.StatusCode < 200, resp.StatusCode > 300:
		err = fmt.Errorf("http return error, status code = %d, %s", resp.StatusCode, resp.Status)
		log.Error(err)
		return err
	default:
		log.Info("post to loki done.")
		return nil
	}
}

func (c Config) NewClient(ctx context.Context) (chan interface{}, error) {
	b := gobatch.NewBatcher(c.lokiJob)
	b.MaxRetry = c.Retry
	b.BatchSize = c.Batch
	b.MaxWait, _ = time.ParseDuration(c.Interval)
	return b.Start(ctx)
}
