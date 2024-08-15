package sinks

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
)

type StdoutConfig struct {
	// DeDot all labels and annotations in the event. For both the event and the involvedObject
	DeDot  bool                   `yaml:"deDot"`
	Layout map[string]interface{} `yaml:"layout"`
}

func (f *StdoutConfig) Validate() error {
	return nil
}

type Stdout struct {
	writer  io.Writer
	encoder *json.Encoder
	cfg     *StdoutConfig
}

func NewStdoutSink(config *StdoutConfig) (*Stdout, error) {
	writer := bufio.NewWriter(os.Stdout)

	return &Stdout{
		writer:  writer,
		encoder: json.NewEncoder(writer),
		cfg:     config,
	}, nil
}

func (f *Stdout) Close() {
	if bufWriter, ok := f.writer.(*bufio.Writer); ok {
		bufWriter.Flush()
	}
}

func (f *Stdout) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	if f.cfg.DeDot {
		de := ev.DeDot()
		ev = &de
	}

	var err error
	if f.cfg.Layout == nil {
		err = f.encoder.Encode(ev)
	} else {
		res, convErr := convertLayoutTemplate(f.cfg.Layout, ev)
		if convErr != nil {
			return convErr
		}
		err = f.encoder.Encode(res)
	}

	if err != nil {
		log.Printf("Error encoding or sending event: %v", err)
		return err
	}

	if bufWriter, ok := f.writer.(*bufio.Writer); ok {
		if flushErr := bufWriter.Flush(); flushErr != nil {
			log.Printf("Error flushing buffer: %v", flushErr)
			return flushErr
		}
	}

	return nil
}
