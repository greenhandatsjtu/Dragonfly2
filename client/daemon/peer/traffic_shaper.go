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
	"time"

	"golang.org/x/time/rate"
)

type TrafficShaper interface {
	Start()
	Stop()
	UpdateLimit()
	WaitN(ctx context.Context, n int, taskID string, ptc *peerTaskConductor) error
}

type taskEntry struct {
	ptc           *peerTaskConductor
	usedBandwidth int
}

type trafficShaper struct {
	sync.Mutex
	*rate.Limiter
	ptm    *peerTaskManager
	tasks  map[string]*taskEntry
	stopCh chan struct{}
}

func NewTrafficShaper(totalRateLimit rate.Limit, ptm *peerTaskManager) TrafficShaper {
	return &trafficShaper{
		Limiter: rate.NewLimiter(totalRateLimit, int(totalRateLimit)),
		ptm:     ptm,
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
				ts.UpdateLimit()
			case <-ts.stopCh:
				return
			}
		}
	}()
}

func (ts *trafficShaper) Stop() {
	close(ts.stopCh)
}

func (ts *trafficShaper) UpdateLimit() {
	var totalRemainingLength int64
	// compute overall remaining length of all tasks
	ts.ptm.runningPeerTasks.Range(func(key, value any) bool {
		ptc := value.(*peerTaskConductor)
		remainingLength := ptc.contentLength.Load() - ptc.completedLength.Load()
		totalRemainingLength += remainingLength
		return true
	})
	// allocate bandwidth for tasks based on their remaining length
	ts.ptm.runningPeerTasks.Range(func(key, value any) bool {
		ptc := value.(*peerTaskConductor)
		remainingLength := ptc.contentLength.Load() - ptc.completedLength.Load()
		limit := float64(ts.Limit()) * float64(remainingLength) / float64(totalRemainingLength)
		ptc.limiter.SetLimit(rate.Limit(limit))
		ptc.limiter.SetBurst(int(limit))
		return true
	})
}

func (ts *trafficShaper) WaitN(ctx context.Context, n int, taskID string, ptc *peerTaskConductor) error {
	if err := ts.Limiter.WaitN(ctx, n); err != nil {
		return err
	}
	ts.Lock()
	if _, ok := ts.tasks[taskID]; !ok {
		ts.tasks[taskID] = &taskEntry{ptc: ptc, usedBandwidth: n}
	} else {
		ts.tasks[taskID].usedBandwidth += n
	}
	ts.Unlock()
	return nil
}
