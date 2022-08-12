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
	"golang.org/x/time/rate"
	"sync"
	"time"
)

type TrafficShaper interface {
	Start()
	Stop()
	WaitN(ctx context.Context, n int, taskID string, ptc *peerTaskConductor) error
}

type taskEntry struct {
	ptc           *peerTaskConductor
	usedBandwidth int
}

type trafficShaper struct {
	sync.Mutex
	*rate.Limiter
	tasks  map[string]*taskEntry
	stopCh chan struct{}
}

func NewTrafficShaper(totalRateLimit rate.Limit) TrafficShaper {
	return &trafficShaper{
		Limiter: rate.NewLimiter(totalRateLimit, int(totalRateLimit)),
		tasks:   make(map[string]*taskEntry),
		stopCh:  make(chan struct{}),
	}
}

func (ts *trafficShaper) Start() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ts.Lock()
				var totalRemainingLength int64
				// compute overall remaining length of all tasks
				for _, task := range ts.tasks {
					remainingLength := task.ptc.completedLength.Load() - task.ptc.completedLength.Load()
					totalRemainingLength += remainingLength
				}
				// allocate bandwidth for tasks based on their remaining length
				for taskID, task := range ts.tasks {
					remainingLength := task.ptc.completedLength.Load() - task.ptc.completedLength.Load()
					limit := float64(remainingLength) / float64(totalRemainingLength)
					task.ptc.limiter.SetLimit(rate.Limit(limit))
					task.ptc.limiter.SetBurst(int(limit))
					delete(ts.tasks, taskID)
				}
				ts.Unlock()
			case <-ts.stopCh:
				return
			}
		}
	}()
}

func (ts *trafficShaper) Stop() {
	close(ts.stopCh)
}

func (ts *trafficShaper) WaitN(ctx context.Context, n int, taskID string, ptc *peerTaskConductor) error {
	if err := ts.Limiter.WaitN(ctx, n); err != nil {
		return err
	}
	ts.Lock()
	if _, ok := ts.tasks[taskID]; !ok {
		// new task
		ts.tasks[taskID] = &taskEntry{ptc: ptc, usedBandwidth: n}
		totalRateLimit := float64(ts.Limit())
		ratio := totalRateLimit - float64(ptc.limiter.Limit())/totalRateLimit
		// scale all tasks' bandwidth
		for _, task := range ts.tasks {
			limit := ratio * float64(task.ptc.limiter.Limit())
			task.ptc.limiter.SetLimit(rate.Limit(limit))
			task.ptc.limiter.SetBurst(int(limit))
		}
	} else {
		// record used bandwidth
		ts.tasks[taskID].usedBandwidth += n
	}
	ts.Unlock()
	return nil
}
