package testutil

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
	"os"
	"time"
	"io/ioutil"
	"path"
	"path/filepath"
)

func bindata_read(data []byte, name string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, gz)
	clErr := gz.Close()

	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}
	if clErr != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type asset struct {
	bytes []byte
	info  os.FileInfo
}

type bindata_file_info struct {
	name string
	size int64
	mode os.FileMode
	modTime time.Time
}

func (fi bindata_file_info) Name() string {
	return fi.name
}
func (fi bindata_file_info) Size() int64 {
	return fi.size
}
func (fi bindata_file_info) Mode() os.FileMode {
	return fi.mode
}
func (fi bindata_file_info) ModTime() time.Time {
	return fi.modTime
}
func (fi bindata_file_info) IsDir() bool {
	return false
}
func (fi bindata_file_info) Sys() interface{} {
	return nil
}

var _assets_origins_csv = []byte("\x1f\x8b\x08\x00\x00\x09\x6e\x88\x00\xff\xb4\x57\x4d\x4f\xdc\x30\x10\xbd\xf3\x2b\x2c\xce\x86\x5e\xaa\xde\xdb\x22\x54\xa4\x7e\x48\x2d\x9c\xab\xd9\x64\x76\xb1\x94\xd8\x5b\xdb\x01\xb6\xbf\xbe\x63\xc7\x09\xf9\x98\x24\x66\xa5\x5e\x40\xf1\xbc\x37\xfb\xe6\x79\x3c\x4e\x4a\x53\x83\xd2\x12\xb5\x57\xfe\x24\xc1\x7b\xab\x76\x8d\xc7\xdf\x69\xbd\x5f\x90\x4f\x50\x35\xfd\x72\x7c\xb8\x30\x56\x1d\x94\x76\xd7\x01\xe4\xa4\x86\x1a\xe5\xe0\x2f\x1b\xae\x60\x87\x95\x94\xdf\x97\xe2\xa5\x29\xa4\x7c\xd0\xea\x4f\x83\x22\xac\x08\xb3\x17\xfe\x11\x45\xab\x4f\x28\x1d\x9f\x5a\x15\xd7\x6c\x06\x7f\x3a\xa2\xec\x02\xe1\xc1\x49\x47\x35\xe8\x03\x8b\x2e\xc0\x96\x4a\x43\x15\x8a\xef\xe2\xaf\x6b\x8a\xc8\x46\xf3\x4a\x9b\x28\x92\x7e\xcf\xce\x9c\x48\x45\x0e\x2a\xe6\x01\xe9\xdf\xd7\x65\x44\xb4\xe3\x4b\x53\x83\xbe\xb2\x08\x25\xec\x2a\x14\x31\x14\x7c\x01\x2d\x54\xd9\x1a\x33\xb5\x22\xd1\xa3\x17\x7c\xf9\x09\xc1\xd5\xbf\x62\x5a\x62\xa5\xda\x73\x0d\x8b\x55\xf4\xfb\xcb\x05\x53\xde\x1b\x53\x34\x35\x55\x04\x5e\x19\xcd\xe1\xe2\x9f\x11\xaa\x6b\x90\x25\x27\x22\x63\xc5\x87\x18\x1f\xba\xb0\xa4\xbf\xdb\xef\x3d\x54\x6e\x0a\x68\xf3\x0f\xfb\x82\x05\xa4\x2a\x1f\x96\x11\xf1\x97\x6e\x2b\x38\x38\xf1\xfc\x88\x54\x97\xa5\xe2\x94\x13\xfd\x31\x14\xf4\xd0\xfe\x82\x80\xc2\x1a\xe7\xda\x93\x41\xbe\x87\xb3\x01\x0b\x27\xa3\x4d\xce\x9c\x0c\x8b\x7b\x16\x7a\xce\xb1\x68\x99\x6b\x36\x8d\x6c\x9e\x9d\xbf\x35\x70\x72\xee\x73\x1e\x3a\xba\x78\x79\x4f\x5d\x31\x58\x4e\x27\xe6\x75\xa0\xf5\x0e\xf7\x4b\xef\xe2\x58\x13\x47\x50\xd6\x11\x55\x8b\x1d\xb9\xec\x9c\x29\x14\x78\x2c\x85\x37\x21\x41\x6a\xb3\xcb\x35\x01\x59\x4e\x8f\x18\xe7\x18\x3e\x4a\xb0\xe6\x7b\x17\x5b\x69\xd0\x0e\xb2\xd6\xa2\x1d\xe6\x3f\x35\x69\x3f\x50\xe7\xe6\xed\x8c\xa9\x10\xa6\x13\xa1\x23\x9c\xe3\x5d\xc7\xe5\x6d\x9b\xb1\x25\x77\x03\x0c\xb3\x6e\x31\x92\xaf\x3f\xde\x40\x69\xbb\xf8\x06\xb5\xf1\x64\x9c\xd1\x15\xb5\xb0\x46\xb6\x5b\x07\xcd\x8a\x36\xb4\x2a\x78\x6a\xd5\x93\xf0\x2a\xdc\xa0\x7a\xd0\xb6\xe2\x8e\x4e\x81\x70\xcd\xce\x21\xd5\xad\xbd\x68\x93\x84\xdd\x4b\x5c\x19\xc7\xe9\xd1\xe2\x93\x32\x0d\xed\xb0\xaa\xaa\x98\xba\xf1\xb4\x6b\x5e\x15\x50\x91\x10\x8b\xde\x42\x41\xe8\xc1\x39\x18\xd7\x43\x57\xd6\x89\x75\x2d\x04\xf2\x39\xc9\xb7\x6f\x6f\x22\xb5\xb7\x44\x32\xee\x2f\x5a\x23\x8c\x15\xb5\xb1\xbc\x79\x6e\xe6\xde\xc8\xb1\x0b\xee\x5a\x64\x2b\x9b\xdc\x2f\x2b\xf8\x54\xd5\xaf\x6c\x42\xac\xa8\x85\x8b\x00\x9b\xaa\x52\xda\xb3\x92\x68\x7d\x13\x99\xc4\xdc\x69\x8f\x07\xb4\x9b\xf0\x28\xe5\xc3\xfb\xab\x9d\xf2\x74\x9a\x3d\x2b\xa7\x59\xd2\xd3\xcc\x05\x31\xd8\x7e\x08\x39\x75\xd0\xb4\x1f\xbc\x34\x86\x38\xd4\xd6\x2c\x89\x0b\xc3\x84\x15\x17\x02\xdb\xd8\x24\xee\xd3\x64\x24\x2d\xe2\xa3\xa6\x84\x66\xf5\xec\x2b\x03\xbc\x5b\x31\x92\x81\x4e\x92\x6e\x33\xe1\x43\x97\x62\x9c\x95\x15\x66\x07\xab\x2a\x04\xb6\xb1\x49\xd3\x7d\x16\x38\x2a\x0a\x50\x56\x09\x5d\x9d\xac\x90\xe1\x95\xba\x84\x4c\x32\x7e\xe2\x1e\x2d\xea\x62\xaa\x65\x4e\x88\x52\x7a\xf8\xf8\xce\xef\xc9\x75\xbc\xd6\xa4\x7f\xe1\x0d\x7a\xd9\x04\x76\xee\x58\xd0\x8e\xe6\xe8\xf0\x6d\x77\x89\x12\x85\x7d\x14\x31\x4c\x23\x9a\x76\x0d\x5f\x8e\xa0\x4b\x17\x34\x86\x91\x5d\x34\xd6\x86\x89\xee\x5f\x93\x6e\x2b\xde\x78\x53\x59\xa2\xbd\xed\xca\x5d\xca\xc2\x5f\xbe\x09\xad\xcd\x33\xeb\x2e\xad\x6f\x43\xbb\xcf\xcc\x1c\x6c\xbe\xb1\xd4\xa0\x33\x47\xe7\xf9\x18\x4b\x03\xd3\x79\xa8\x8f\xdb\xec\xb3\x9c\x9d\xa7\x59\xb5\x36\x7d\xc4\xf3\xdf\xdf\x21\x94\x45\xe8\xbf\xd9\xb2\x19\xd9\x4e\x4f\x5f\x10\xd7\x92\xe6\x75\x30\x4b\x3d\xcb\x6b\x36\xd3\xd8\xee\x7f\x01\x00\x00\xff\xff\x43\x7f\xc8\xe3\x54\x11\x00\x00")

func assets_origins_csv_bytes() ([]byte, error) {
	return bindata_read(
		_assets_origins_csv,
		"assets/origins.csv",
	)
}

func assets_origins_csv() (*asset, error) {
	bytes, err := assets_origins_csv_bytes()
	if err != nil {
		return nil, err
	}

	info := bindata_file_info{name: "assets/origins.csv", size: 4436, mode: os.FileMode(420), modTime: time.Unix(1435253092, 0)}
	a := &asset{bytes: bytes, info:  info}
	return a, nil
}

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func Asset(name string) ([]byte, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("Asset %s can't read by error: %v", name, err)
		}
		return a.bytes, nil
	}
	return nil, fmt.Errorf("Asset %s not found", name)
}

// MustAsset is like Asset but panics when Asset would return an error.
// It simplifies safe initialization of global variables.
func MustAsset(name string) []byte {
	a, err := Asset(name)
	if (err != nil) {
		panic("asset: Asset(" + name + "): " + err.Error())
	}

	return a
}

// AssetInfo loads and returns the asset info for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func AssetInfo(name string) (os.FileInfo, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("AssetInfo %s can't read by error: %v", name, err)
		}
		return a.info, nil
	}
	return nil, fmt.Errorf("AssetInfo %s not found", name)
}

// AssetNames returns the names of the assets.
func AssetNames() []string {
	names := make([]string, 0, len(_bindata))
	for name := range _bindata {
		names = append(names, name)
	}
	return names
}

// _bindata is a table, holding each asset generator, mapped to its name.
var _bindata = map[string]func() (*asset, error){
	"assets/origins.csv": assets_origins_csv,
}

// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"}
// AssetDir("data/img") would return []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") would return an error
// AssetDir("") will return []string{"data"}.
func AssetDir(name string) ([]string, error) {
	node := _bintree
	if len(name) != 0 {
		cannonicalName := strings.Replace(name, "\\", "/", -1)
		pathList := strings.Split(cannonicalName, "/")
		for _, p := range pathList {
			node = node.Children[p]
			if node == nil {
				return nil, fmt.Errorf("Asset %s not found", name)
			}
		}
	}
	if node.Func != nil {
		return nil, fmt.Errorf("Asset %s not found", name)
	}
	rv := make([]string, 0, len(node.Children))
	for name := range node.Children {
		rv = append(rv, name)
	}
	return rv, nil
}

type _bintree_t struct {
	Func func() (*asset, error)
	Children map[string]*_bintree_t
}
var _bintree = &_bintree_t{nil, map[string]*_bintree_t{
	"assets": &_bintree_t{nil, map[string]*_bintree_t{
		"origins.csv": &_bintree_t{assets_origins_csv, map[string]*_bintree_t{
		}},
	}},
}}

// Restore an asset under the given directory
func RestoreAsset(dir, name string) error {
        data, err := Asset(name)
        if err != nil {
                return err
        }
        info, err := AssetInfo(name)
        if err != nil {
                return err
        }
        err = os.MkdirAll(_filePath(dir, path.Dir(name)), os.FileMode(0755))
        if err != nil {
                return err
        }
        err = ioutil.WriteFile(_filePath(dir, name), data, info.Mode())
        if err != nil {
                return err
        }
        err = os.Chtimes(_filePath(dir, name), info.ModTime(), info.ModTime())
        if err != nil {
                return err
        }
        return nil
}

// Restore assets under the given directory recursively
func RestoreAssets(dir, name string) error {
        children, err := AssetDir(name)
        if err != nil { // File
                return RestoreAsset(dir, name)
        } else { // Dir
                for _, child := range children {
                        err = RestoreAssets(dir, path.Join(name, child))
                        if err != nil {
                                return err
                        }
                }
        }
        return nil
}

func _filePath(dir, name string) string {
        cannonicalName := strings.Replace(name, "\\", "/", -1)
        return filepath.Join(append([]string{dir}, strings.Split(cannonicalName, "/")...)...)
}

