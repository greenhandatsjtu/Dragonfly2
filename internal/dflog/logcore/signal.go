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

package logcore

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var (
	levels []zap.AtomicLevel
	level  = zapcore.InfoLevel
)

func startLoggerSignalHandler() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)

	go func() {
		for {
			select {
			case <-signals:
				level--
				if level < zapcore.DebugLevel {
					level = zapcore.FatalLevel
				}

				// use fmt.Printf print change log level event when log level is greater than info level
				fmt.Printf("change log level to %s\n", level.String())
				logger.Infof("change log level to %s", level.String())
				for _, l := range levels {
					l.SetLevel(level)
				}
			}
		}
	}()
}