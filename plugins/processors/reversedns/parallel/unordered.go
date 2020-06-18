package parallel

import (
	"sync"

	"github.com/influxdata/telegraf"
)

type Unordered struct {
	wg       sync.WaitGroup
	acc      telegraf.Accumulator
	fn       func(telegraf.Metric) []telegraf.Metric
	inQueue  chan telegraf.Metric
	outQueue chan telegraf.Metric
}

func NewUnordered(
	acc telegraf.Accumulator,
	fn func(telegraf.Metric) []telegraf.Metric,
	workerCount int,
) *Unordered {
	p := &Unordered{
		acc:      acc,
		inQueue:  make(chan telegraf.Metric, workerCount),
		outQueue: make(chan telegraf.Metric, workerCount),
		fn:       fn,
	}

	// start workers
	p.wg.Add(1)
	go func() {
		p.startWorkers(workerCount)
		p.wg.Done()
	}()

	// start one to read the output
	p.wg.Add(1)
	go func() {
		p.readQueue()
		p.wg.Done()
	}()
	return p
}

func (p *Unordered) startWorkers(count int) {
	wg := sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			for metric := range p.inQueue {
				for _, m := range p.fn(metric) {
					p.outQueue <- m
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	close(p.outQueue)
}

func (p *Unordered) readQueue() {
	for m := range p.outQueue {
		if m != nil {
			p.acc.AddMetric(m)
		}
	}
}

func (p *Unordered) Stop() {
	close(p.inQueue)
	p.wg.Wait()
}

func (p *Unordered) Enqueue(m telegraf.Metric) {
	p.inQueue <- m
}
