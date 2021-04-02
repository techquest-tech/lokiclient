package lokiclient_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/techquest-tech/lokiclient"
)

func TestClient(t *testing.T) {
	c := lokiclient.PushConfig{
		URL:      "http://127.0.0.1:3100",
		Interval: "1s",
		Batch:    99,
		Retry:    5,
	}
	ctx, canel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer canel()

	batch, err := c.NewClient(ctx)
	assert.Nil(t, err)

	labs := map[string]string{
		"abc    ": "1234",
		"index":   "debug",
		"seq-now": fmt.Sprintf("%d", time.Now().UnixNano()),
		// "&1234": "should not ok",
	}

	for i := 1; i <= 100; i++ {
		batch <- lokiclient.NewPushItem(labs, fmt.Sprintf("it is %d message", i))
	}

	time.Sleep(12 * time.Second)
}
