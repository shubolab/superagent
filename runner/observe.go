package runner

import (
	"context"
	"encoding/json"
	"io"

	"github.com/shubolab/superagent/internal/contract"
)

type queuedEvent struct {
	event contract.Event
	bytes uint64
}

// observationBuffer is accessed under the Run state lock after admission.
type observationBuffer struct {
	id       contract.RunID
	capacity uint64
	bytes    uint64
	seq      uint64
	queue    []queuedEvent
	changed  chan struct{}
	used     bool
	overflow bool
	discard  bool
}

func newObservationBuffer(id contract.RunID, capacity uint64) observationBuffer {
	return observationBuffer{id: id, capacity: capacity, changed: make(chan struct{})}
}

func (b *observationBuffer) publish(event contract.Event) {
	if b.discard || b.overflow {
		return
	}
	event.RunID = b.id
	event.Seq = b.seq + 1
	available := b.capacity - b.bytes
	if uint64(len(b.id)) > available || uint64(len(event.TextDelta)) > available ||
		(event.Message != nil && uint64(len(event.Message.Text)) > available) {
		b.overflow = true
		b.signal()
		return
	}
	encoded, _ := json.Marshal(event)
	size := uint64(len(encoded))
	if size > available {
		b.overflow = true
		b.signal()
		return
	}
	b.seq = event.Seq
	b.queue = append(b.queue, queuedEvent{event: event, bytes: size})
	b.bytes += size
	b.signal()
}

func (b *observationBuffer) pop() contract.Event {
	first := b.queue[0]
	b.bytes -= first.bytes
	b.queue[0] = queuedEvent{}
	b.queue = b.queue[1:]
	if len(b.queue) == 0 {
		b.queue = nil
	}
	return first.event
}

func (b *observationBuffer) abandon() {
	if !b.discard {
		b.discard = true
		b.queue = nil
		b.bytes = 0
		b.signal()
	}
}

func (b *observationBuffer) signal() {
	close(b.changed)
	b.changed = make(chan struct{})
}

type observer struct {
	run    *state
	ctx    context.Context
	stop   func() bool
	closed bool
}

func (s *state) observe(ctx context.Context) (*observer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.events.used {
		return nil, contract.ErrObservationAlreadyUsed
	}
	s.events.used = true
	o := &observer{run: s, ctx: ctx}
	o.stop = context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.events.abandon()
	})
	return o, nil
}

func (o *observer) Recv() (contract.Event, error) {
	for {
		event, err, changed := o.next()
		if changed == nil {
			if err != nil {
				o.stop()
			}
			return event, err
		}
		select {
		case <-changed:
		case <-o.ctx.Done():
		case <-o.run.done:
		}
	}
}

func (o *observer) next() (contract.Event, error, <-chan struct{}) {
	s := o.run
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case o.closed:
		return contract.Event{}, contract.ErrStreamClosed, nil
	case o.ctx.Err() != nil:
		s.events.abandon()
		return contract.Event{}, o.ctx.Err(), nil
	case s.checkpointErr != nil:
		return contract.Event{}, s.checkpointErr, nil
	case len(s.events.queue) > 0:
		return s.events.pop(), nil, nil
	case s.events.overflow:
		return contract.Event{}, contract.ErrObserveOverflow, nil
	case !s.finishedAt.IsZero():
		return contract.Event{}, io.EOF, nil
	default:
		return contract.Event{}, nil, s.events.changed
	}
}

func (o *observer) Close() error {
	s := o.run
	s.mu.Lock()
	defer s.mu.Unlock()
	o.closed = true
	o.stop()
	s.events.abandon()
	return nil
}
