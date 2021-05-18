/*
   Copyright The containerd Authors.

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

package ioutil

import (
	"os"

	"github.com/pkg/errors"
)

// LimitedRead reads at max `readLimitBytes` bytes from the file at path `filePath`. If the file has
// more than `readLimitBytes` bytes of data then first `readLimitBytes` will be returned.
func LimitedRead(filePath string, readLimitBytes int64) ([]byte, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "limited read failed to open file")
	}
	defer f.Close()
	if fi, err := f.Stat(); err == nil {
		if fi.Size() < readLimitBytes {
			readLimitBytes = fi.Size()
		}
		buf := make([]byte, readLimitBytes)
		_, err := f.Read(buf)
		if err != nil {
			return []byte{}, errors.Wrap(err, "limited read failed during file read")
		}
		return buf, nil
	}
	return []byte{}, errors.Wrap(err, "limited read failed during file stat")
}
