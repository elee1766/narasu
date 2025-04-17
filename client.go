package narasu

import (
	"context"
	"fmt"
	"time"
)

type ClientOption func(c *Client) *Client

type Client struct {
	store     Store
	maxOldAge time.Duration
}

func WithStore(store Store) ClientOption {
	return func(c *Client) *Client {
		c.store = store
		return c
	}
}
func WithMaxOldAge(maxOldAge time.Duration) ClientOption {
	return func(c *Client) *Client {
		c.maxOldAge = maxOldAge
		return c
	}
}

func NewClient(options ...ClientOption) (*Client, error) {
	c := &Client{
		maxOldAge: 60 * time.Second,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		c = option(c)
	}
	if c.maxOldAge < time.Second {
		return nil, fmt.Errorf("max old age must be greater than 1 second")
	}
	if c.store == nil {
		return nil, fmt.Errorf("store must be set")
	}
	return c, nil
}

func (c *Client) IncrKey(ctx context.Context, bucket string, amount int, cur time.Time) (int, error) {
	return c.store.IncrKey(ctx, bucket, amount, cur)
}

func (c *Client) Window(ctx context.Context, bucket string, from time.Time, to time.Time) (int, error) {
	xs, err := c.store.GetKeys(ctx, bucket, from, to)
	if err != nil {
		return 0, err
	}
	var sum int
	for _, v := range xs {
		sum += v
	}
	return sum, nil
}

func (c *Client) Cleanup(ctx context.Context, from time.Time) error {
	err := c.store.Cleanup(ctx, from, c.maxOldAge)
	if err != nil {
		return err
	}
	return nil
}
