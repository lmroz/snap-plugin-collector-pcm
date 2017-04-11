/*
http://www.apache.org/licenses/LICENSE-2.0.txt

Copyright 2017 Intel Corporation

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

package parser

import (
	"bufio"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"

	"math"

	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

// Specifies how many fields should be ignored before getting useful data
const ignoreFirstNFields = 2

type (
	Key struct {
		Component, MetricName string
	}

	Values map[Key]float64

	ValuesOrError struct {
		Values Values
		Error  error
	}

	Parser interface {
		GetKeys(ctx context.Context) ([]Key, error)
		GetValues(ctx context.Context) (Values, error)
	}
)

func RunParser(reader io.Reader) Parser {
	p := &pcmParser{
		source: reader,
		mutex:  new(sync.RWMutex),
	}
	go p.run()

	sig := make(chan struct{})
	go func() {
		for {
			if p.isReady {
				close(sig)
				return
			}
		}
	}()

	for {
		select {
		case <-time.After(time.Second * 2):
			log.Fatal("timed out starting parser")
		case <-sig:
			return p
		}
	}
}

func (p *pcmParser) GetKeys(ctx context.Context) ([]Key, error) {
	sig := make(chan struct{})
	go withRLock(p.mutex, func() {
		close(sig)
	})

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-sig:
			return p.keys, nil
		}
	}
}

func (p *pcmParser) GetValues(ctx context.Context) (Values, error) {
	sig := make(chan struct{})
	go withRLock(p.mutex, func() {
		close(sig)
	})

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-sig:
			var sVal ValuesOrError
			withRLock(p.mutex, func() {
				sVal = p.values
			})
			return sVal.Values, sVal.Error
		}
	}
}

type pcmParser struct {
	source  io.Reader
	isReady bool
	keys    []Key
	values  ValuesOrError
	mutex   *sync.RWMutex
}

func (p *pcmParser) run() {
	scanner := bufio.NewScanner(p.source)
	var first, second, current []string
	line := 0
	for scanner.Scan() {
		withLock(p.mutex, func() {
			defer func() { p.isReady = true }()
			line++
			if first == nil {
				first = splitLine(scanner.Text())
				const want = ignoreFirstNFields + 1
				if len(first) < want {
					log.WithFields(log.Fields{
						"block":    "header",
						"line":     line,
						"function": "run",
					}).Fatalf("first line should have at least %v columns separated by ';', got: %v", want, len(first))
				}
				fillHeader(first)
				return // returns from anonymous function called by withLock
			}
			if second == nil {
				second = splitLine(scanner.Text())
				if len(first) != len(second) {
					log.WithFields(log.Fields{
						"block":    "data",
						"line":     line,
						"function": "run",
					},
					).Fatalf("headers' lines should have equal number of columns: got %v and %v", len(first), len(second))
				}

				first = first[ignoreFirstNFields:]
				second = second[ignoreFirstNFields:]

				p.keys = make([]Key, len(first))
				for i, topHeader := range first {
					p.keys[i] = Key{Component: topHeader, MetricName: second[i]}
				}

				return //returns from anonymous function called by withLock
			}

			current = splitLine(scanner.Text())
			if len(first)+ignoreFirstNFields != len(current) {
				log.WithFields(log.Fields{
					"block":    "header",
					"line":     line,
					"function": "run",
				},
				).Fatalf("headers' and data records should have equal number of columns, got: %v and %v", len(first)+ignoreFirstNFields, len(current))
			}
			current = current[ignoreFirstNFields:]

			vals := ValuesOrError{Values: Values{}}
			for i, field := range current {

				k := Key{Component: first[i], MetricName: second[i]}
				if strings.ToLower(field) == "n/a" {
					//TODO: make sure this is desired, maybe entry should be just missing
					vals.Values[k] = math.NaN()
				} else {
					if strings.HasSuffix(field, "%") {
						field = strings.TrimSpace(strings.TrimSuffix(field, "%"))
					}
					v, err := strconv.ParseFloat(field, 64)
					if err == nil {
						vals.Values[k] = v
					} else {
						vals = ValuesOrError{Error: errors.Wrapf(err, "parsing %v = %v failed", k, field)}
						break
					}
				}
			}
			p.values = vals
		})
	}

}

func fillHeader(headerRef []string) {
	for i := 1; i < len(headerRef); i++ {
		if len(headerRef[i]) == 0 {
			headerRef[i] = headerRef[i-1]
		}
	}
}

func splitLine(line string) []string {
	line = strings.TrimSuffix(line, ";")
	split := strings.Split(line, ";")
	for i, field := range split {
		split[i] = strings.TrimSpace(field)
	}

	return split
}

func withRLock(rwMutex *sync.RWMutex, f func()) {
	rwMutex.RLock()
	defer rwMutex.RUnlock()
	f()
}

func withLock(mutex *sync.RWMutex, f func()) {
	mutex.Lock()
	defer mutex.Unlock()
	f()
}
