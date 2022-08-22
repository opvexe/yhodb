/*
Copyright 2022 The Workpieces LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bolt

import (
	"context"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"time"
)

var (
	bucketBucket = []byte("bucketsv1")
)

// Client is a client for the boltDB data store.
type Client struct {
	Path string
	db   *bolt.DB
	log  *zap.Logger
}

// NewClient returns an instance of a Client.
func NewClient(log *zap.Logger) *Client {
	return &Client{
		log: log,
	}
}

// DB returns the clients DB.
func (c *Client) DB() *bolt.DB {
	return c.db
}

func (c *Client) Open(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(c.Path), 0700); err != nil {
		return fmt.Errorf("unable to create directory %s: %v", c.Path, err)
	}

	if _, err := os.Stat(c.Path); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Open database file.
	db, err := bolt.Open(c.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		// Hack to give a slightly nicer error message for a known failure mode when bolt calls
		// mmap on a file system that doesn't support the MAP_SHARED option.
		//
		// See: https://github.com/boltdb/bolt/issues/272
		// See: https://stackoverflow.com/a/18421071
		if err.Error() == "invalid argument" {
			return fmt.Errorf("unable to open boltdb: mmap of %q may not support the MAP_SHARED option", c.Path)
		}

		return fmt.Errorf("unable to open boltdb: %w", err)
	}

	c.db = db

	if err := c.initialize(ctx); err != nil {
		return err
	}

	c.log.Info("Resources opened", zap.String("path", c.Path))
	return err
}

// initialize creates Buckets that are missing
func (c *Client) initialize(ctx context.Context) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {

		// TODO bucket list to add.
		bkts := [][]byte{
			bucketBucket,
		}

		for _, bktName := range bkts {
			if _, err := tx.CreateBucketIfNotExists(bktName); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// Close the connection to the bolt database
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
