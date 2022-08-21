/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package peer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	TypePlainTrafficShaper    = "plain"
	TypeSamplingTrafficShaper = "sampling"
)

type TrafficShaper interface {
	Start()
	Stop()
	AddTask(taskID string, ptc *peerTaskConductor)
	RemoveTask(taskID string)
	WaitN(ctx context.Context, n int, taskID string) error
}

func NewTrafficShaper(totalRateLimit rate.Limit, ptm *peerTaskManager, trafficShaperType string) TrafficShaper {
	var ts TrafficShaper
	switch trafficShaperType {
	case TypeSamplingTrafficShaper:
		ts = NewSamplingTrafficShaper(totalRateLimit, ptm)
	case TypePlainTrafficShaper:
		ts = NewPlainTrafficShaper(totalRateLimit, ptm)
	default:
		ts = NewPlainTrafficShaper(totalRateLimit, ptm)
	}
	return ts
}

type plainTrafficShaper struct {
	*rate.Limiter
}

func NewPlainTrafficShaper(totalRateLimit rate.Limit, _ *peerTaskManager) TrafficShaper {
	return &plainTrafficShaper{Limiter: rate.NewLimiter(totalRateLimit, int(totalRateLimit))}
}

func (ts *plainTrafficShaper) Start() {
}

func (ts *plainTrafficShaper) Stop() {
}

func (ts *plainTrafficShaper) AddTask(_ string, _ *peerTaskConductor) {
}

func (ts *plainTrafficShaper) RemoveTask(_ string) {
}

func (ts *plainTrafficShaper) WaitN(ctx context.Context, n int, _ string) error {
	return ts.Limiter.WaitN(ctx, n)
}

type taskEntry struct {
	ptc           *peerTaskConductor
	usedBandwidth atomic.Int64
	needBandwidth int64
}

type samplingTrafficShaper struct {
	sync.Mutex
	*rate.Limiter
	ptm    *peerTaskManager
	tasks  map[string]*taskEntry
	stopCh chan struct{}
}

func NewSamplingTrafficShaper(totalRateLimit rate.Limit, ptm *peerTaskManager) TrafficShaper {
	return &samplingTrafficShaper{
		Limiter: rate.NewLimiter(totalRateLimit, int(totalRateLimit)),
		ptm:     ptm,
		tasks:   make(map[string]*taskEntry),
		stopCh:  make(chan struct{}),
	}
}

func (ts *samplingTrafficShaper) Start() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ts.updateLimit()
			case <-ts.stopCh:
				return
			}
		}
	}()
}

func (ts *samplingTrafficShaper) Stop() {
	close(ts.stopCh)
}

func (ts *samplingTrafficShaper) updateLimit() {
	var totalRemainingLength int64
	ts.Lock()
	defer ts.Unlock()
	// compute overall remaining length of all tasks
	ts.ptm.runningPeerTasks.Range(func(key, value any) bool {
		ptc := value.(*peerTaskConductor)
		remainingLength := ptc.contentLength.Load() - ptc.completedLength.Load()
		var needBandwidth int64
		if !ptc.limiter.Allow() {
			// case 1: bandwidth is fully used
			needBandwidth = remainingLength
		} else {
			// case 2: bandwidth is not fully used
			usedBandwidth := ts.tasks[key.(string)].usedBandwidth.Load()
			if usedBandwidth < remainingLength {
				needBandwidth = usedBandwidth
			} else {
				needBandwidth = remainingLength
			}
		}
		ts.tasks[key.(string)].needBandwidth = needBandwidth
		totalRemainingLength += needBandwidth
		return true
	})
	// allocate bandwidth for tasks based on their remaining length
	ts.ptm.runningPeerTasks.Range(func(key, value any) bool {
		ptc := value.(*peerTaskConductor)
		limit := float64(ts.Limit()) * (float64(ts.tasks[key.(string)].needBandwidth) / float64(totalRemainingLength))
		ptc.limiter.SetLimit(rate.Limit(limit))
		ptc.limiter.SetBurst(int(limit))
		ts.tasks[key.(string)].usedBandwidth.Store(0)
		return true
	})
}

func (ts *samplingTrafficShaper) AddTask(taskID string, ptc *peerTaskConductor) {
	ts.Lock()
	defer ts.Unlock()
	ts.tasks[taskID] = &taskEntry{ptc: ptc}
	ratio := ts.Limit() / (ts.Limit() + ptc.limiter.Limit())
	ts.ptm.runningPeerTasks.Range(func(key, value any) bool {
		ptc := value.(*peerTaskConductor)
		newLimit := ratio * ts.tasks[key.(string)].ptc.limiter.Limit()
		ptc.limiter.SetLimit(newLimit)
		ptc.limiter.SetBurst(int(newLimit))
		return true
	})
}

func (ts *samplingTrafficShaper) RemoveTask(taskID string) {
	ts.Lock()
	defer ts.Unlock()
	limit := ts.tasks[taskID].ptc.limiter.Limit()
	delete(ts.tasks, taskID)
	ratio := ts.Limit() / (ts.Limit() - limit)
	ts.ptm.runningPeerTasks.Range(func(key, value any) bool {
		ptc := value.(*peerTaskConductor)
		newLimit := ratio * ts.tasks[key.(string)].ptc.limiter.Limit()
		ptc.limiter.SetLimit(newLimit)
		ptc.limiter.SetBurst(int(newLimit))
		return true
	})
}

func (ts *samplingTrafficShaper) WaitN(ctx context.Context, n int, taskID string) error {
	if err := ts.Limiter.WaitN(ctx, n); err != nil {
		return err
	}
	ts.Lock()
	ts.tasks[taskID].usedBandwidth.Add(int64(n))
	ts.Unlock()
	return nil
}
