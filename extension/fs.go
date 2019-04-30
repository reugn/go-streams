package ext

import (
	"bufio"
	"os"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type FileSource struct {
	fileName string
	in       chan interface{}
}

func NewFileSource(fileName string) *FileSource {
	source := &FileSource{fileName, make(chan interface{})}
	source.init()
	return source
}

func (fs *FileSource) init() {
	go func() {
		file, err := os.Open(fs.fileName)
		streams.Check(err)
		defer file.Close()
		reader := bufio.NewReader(file)
		for {
			l, isPrefix, err := reader.ReadLine()
			if err != nil {
				close(fs.in)
				break
			}
			var msg string
			if isPrefix {
				msg = string(l[:])
			} else {
				msg = string(l[:]) + "\n"
			}
			fs.in <- msg
		}
	}()
}

func (fs *FileSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(fs, _flow)
	return _flow
}

func (fs *FileSource) Out() <-chan interface{} {
	return fs.in
}

//-------------------------------------------------------------------
type FileSink struct {
	fileName string
	in       chan interface{}
}

func NewFileSink(fileName string) *FileSink {
	sink := &FileSink{fileName, make(chan interface{})}
	sink.init()
	return sink
}

func (fs *FileSink) init() {
	go func() {
		file, err := os.OpenFile(fs.fileName, os.O_CREATE|os.O_WRONLY, 0600)
		streams.Check(err)
		defer file.Close()
		for elem := range fs.in {
			_, err = file.WriteString(elem.(string))
			streams.Check(err)
		}
	}()
}

func (fs *FileSink) In() chan<- interface{} {
	return fs.in
}
