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
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	"io/ioutil"
	"os"
	"testing"
)

func NewTestKVStore(t *testing.T) *KVStore {
	f, err := ioutil.TempFile("", "influxdata-platform-bolt-")
	assert.NoError(t, err)
	defer f.Close()

	path := f.Name()
	s := NewKVStore(zaptest.NewLogger(t), path, WithNoSync)
	err = s.Open(context.TODO())
	assert.NoError(t, err)

	defer os.Remove(path)
	return s
}
