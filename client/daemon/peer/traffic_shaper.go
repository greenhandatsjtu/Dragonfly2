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
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/time/rate"
)

const (
	TypePlainTrafficShaper    = "plain"
	TypeSamplingTrafficShaper = "sampling"
)

// TrafficShaper allocates bandwidth for running tasks dynamically
type TrafficShaper interface {
	// Start starts the TrafficShaper
	Start()
	// Stop stops the TrafficShaper
	Stop()
	// AddTask starts managing the new task
	AddTask(taskID string, ptc *peerTaskConductor)
	// RemoveTask removes completed task
	RemoveTask(taskID string)
	// Record records task's used bandwidth
	Record(taskID string, n int)
}

func NewTrafficShaper(totalRateLimit rate.Limit, trafficShaperType string) TrafficShaper {
	var ts TrafficShaper
	switch trafficShaperType {
	case TypeSamplingTrafficShaper:
		ts = NewSamplingTrafficShaper(totalRateLimit)
	case TypePlainTrafficShaper:
		ts = NewPlainTrafficShaper(totalRateLimit)
	default:
		ts = NewPlainTrafficShaper(totalRateLimit)
	}
	return ts
}

type plainTrafficShaper struct {
}

func NewPlainTrafficShaper(_ rate.Limit) TrafficShaper {
	return &plainTrafficShaper{}
}

func (ts *plainTrafficShaper) Start() {
}

func (ts *plainTrafficShaper) Stop() {
}

func (ts *plainTrafficShaper) AddTask(_ string, _ *peerTaskConductor) {
}

func (ts *plainTrafficShaper) RemoveTask(_ string) {
}

func (ts *plainTrafficShaper) Record(_ string, _ int) {
}

type taskEntry struct {
	ptc           *peerTaskConductor
	usedBandwidth *atomic.Int64
	needBandwidth int64
	needUpdate    bool
}

type samplingTrafficShaper struct {
	sync.Mutex
	totalRateLimit rate.Limit
	tasks          map[string]*taskEntry
	stopCh         chan struct{}
}

func NewSamplingTrafficShaper(totalRateLimit rate.Limit) TrafficShaper {
	return &samplingTrafficShaper{
		totalRateLimit: totalRateLimit,
		tasks:          make(map[string]*taskEntry),
		stopCh:         make(chan struct{}),
	}
}

func (ts *samplingTrafficShaper) Start() {
	go func() {
		// update bandwidth of all running tasks every second
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

// updateLimit updates every task's limit every second
func (ts *samplingTrafficShaper) updateLimit() {
	var totalNeedBandwidth int64
	ts.Lock()
	defer ts.Unlock()
	// compute overall remaining length of all tasks
	for _, te := range ts.tasks {
		var needBandwidth int64
		if !te.needUpdate {
			// if this task is added within 1 second, don't update its limit this time
			te.needUpdate = true
			needBandwidth = int64(te.ptc.limiter.Limit())
		} else {
			if !te.ptc.limiter.Allow() {
				// case 1: bandwidth is fully used
				needBandwidth = int64(te.ptc.limiter.Limit())
			} else {
				// case 2: bandwidth is not fully used
				needBandwidth = te.usedBandwidth.Load()
			}
			if contentLength := te.ptc.contentLength.Load(); contentLength > 0 {
				remainingLength := contentLength - te.ptc.completedLength.Load()
				if remainingLength < needBandwidth {
					needBandwidth = remainingLength
				}
			}
		}
		te.needBandwidth = needBandwidth
		totalNeedBandwidth += needBandwidth
		te.usedBandwidth.Store(0)
	}

	// allocate bandwidth for tasks based on their remaining length
	for _, te := range ts.tasks {
		limit := float64(ts.totalRateLimit) * (float64(te.needBandwidth) / float64(totalNeedBandwidth))
		te.ptc.limiter.SetLimit(rate.Limit(limit))
	}
}

func (ts *samplingTrafficShaper) AddTask(taskID string, ptc *peerTaskConductor) {
	ts.Lock()
	defer ts.Unlock()
	ts.tasks[taskID] = &taskEntry{ptc: ptc, usedBandwidth: atomic.NewInt64(0)}
	ratio := ts.totalRateLimit / (ts.totalRateLimit + ptc.limiter.Limit())
	// reduce all running tasks' bandwidth
	for _, te := range ts.tasks {
		newLimit := ratio * te.ptc.limiter.Limit()
		te.ptc.limiter.SetLimit(newLimit)
	}
}

func (ts *samplingTrafficShaper) RemoveTask(taskID string) {
	ts.Lock()
	defer ts.Unlock()
	limit := ts.tasks[taskID].ptc.limiter.Limit()
	delete(ts.tasks, taskID)
	ratio := ts.totalRateLimit / (ts.totalRateLimit - limit)
	// increase all running tasks' bandwidth
	for _, te := range ts.tasks {
		newLimit := ratio * te.ptc.limiter.Limit()
		te.ptc.limiter.SetLimit(newLimit)
	}
}

func (ts *samplingTrafficShaper) Record(taskID string, n int) {
	ts.Lock()
	ts.tasks[taskID].usedBandwidth.Add(int64(n))
	ts.Unlock()
}
