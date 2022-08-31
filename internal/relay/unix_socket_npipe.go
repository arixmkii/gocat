// Copyright 2018 SumUp Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package relay

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/palantir/stacktrace"
	"github.com/sumup-oss/go-pkgs/logger"
)

type UnixSocketNPipe struct {
	AbstractDuplexRelay
}

func NewUnixSocketNPipe(
	logger logger.Logger,
	healthCheckInterval time.Duration,
	unixSocketPath,
	pipePath string,
	bufferSize int,
) (*UnixSocketNPipe, error) {
	// FIXME pipePathValudate

	_, err := os.Stat(unixSocketPath)
	if os.IsNotExist(err) {
		return nil, stacktrace.Propagate(err, "could not stat %s", unixSocketPath)
	}

	return &UnixSocketNPipe{
		AbstractDuplexRelay{
			healthCheckInterval: healthCheckInterval,
			logger:              logger,
			bufferSize:          bufferSize,
			sourceName:          "unix socket",
			destinationName:     "NPipe connection",
			destinationAddr:     pipePath,
			dialSourceConn: func(ctx context.Context) (net.Conn, error) {
				dialer := &net.Dialer{}
				// NOTE: This is a streaming unix domain socket
				// equivalent of `sock.STREAM`.
				conn, err := dialer.DialContext(ctx, "unix", unixSocketPath)
				if err != nil {
					return nil, stacktrace.Propagate(
						err,
						"failed to dial unix address: %s",
						unixSocketPath,
					)
				}

				return conn, nil
			},
			listenTargetConn: func(ctx context.Context) (net.Listener, error) {
				listener, err := winio.ListenPipe(pipePath, nil)
				if err != nil {
					return nil, stacktrace.Propagate(
						err,
						"failed to listen at NPipe address: %s",
						pipePath,
					)
				}
				return listener, nil
			},
		},
	}, nil
}
