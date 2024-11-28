package benchmark

import "fmt"

type operation struct {
	input  interface{}
	output interface{}
	// timestamps
	start int64
	end   int64
}

func (a operation) String() string {
	return fmt.Sprintf("{input=%v, output=%v, start=%d, end=%d}", a.input, a.output, a.start, a.end)
}

// sort operations by invocation time
type byTime []*operation

func (a byTime) Len() int           { return len(a) }
func (a byTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTime) Less(i, j int) bool { return a[i].start < a[j].start }
