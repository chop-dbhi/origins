// Origins is an open source bi-temporal database for storing and retrieving
// facts about the state of things. It supports "time-travel" queries,
// aggregate views, and change detection.
//
// The primary interface is the CLI which can be installed by running:
//
//     go get -u github.com/chop-dbhi/origins/cmd/origins
//
// This package defines some of the primitive structures and algorithms for
// manipulating, reading, and writing facts and is used to build higher level
// client APIs.
package origins

const Version = "0.1"
