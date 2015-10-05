//  Copyright (c) 2015 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package objectserver

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteReadMetadata(t *testing.T) {

	data := map[string]string{
		strings.Repeat("la", 5):    strings.Repeat("la", 30),
		strings.Repeat("moo", 500): strings.Repeat("moo", 300),
	}
	testFile, err := ioutil.TempFile("/tmp", "backend_test")
	defer testFile.Close()
	defer os.Remove(testFile.Name())
	assert.Equal(t, err, nil)
	WriteMetadata(testFile.Fd(), data)
	checkData := map[string]string{
		strings.Repeat("la", 5):    strings.Repeat("la", 30),
		strings.Repeat("moo", 500): strings.Repeat("moo", 300),
	}
	readData, err := ReadMetadata(testFile.Name())
	assert.Equal(t, err, nil)
	assert.True(t, reflect.DeepEqual(checkData, readData))

	readData, err = ReadMetadata(testFile.Fd())
	assert.Equal(t, err, nil)
	assert.True(t, reflect.DeepEqual(checkData, readData))
}
