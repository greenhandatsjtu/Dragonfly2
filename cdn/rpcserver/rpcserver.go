/*
 *     Copyright 2020 The Dragonfly Authors
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

package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/cdn/constants"
	"d7y.io/dragonfly/v2/cdn/supervisor"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	cdnserver "d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
)

var tracer = otel.Tracer("cdn-server")

type Server struct {
	*grpc.Server
	config  Config
	service supervisor.CDNService
}

// New returns a new Manager Object.
func New(config Config, cdnService supervisor.CDNService, opts ...grpc.ServerOption) (*Server, error) {
	svr := &Server{
		config:  config,
		service: cdnService,
	}
	svr.Server = cdnserver.New(svr, opts...)
	return svr, nil
}

func (css *Server) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, psc chan<- *cdnsystem.PieceSeed) (err error) {
	clientAddr := "unknown"
	if pe, ok := peer.FromContext(ctx); ok {
		clientAddr = pe.Addr.String()
	}
	logger.Infof("trigger obtain seed for taskID: %s, url: %s, urlMeta: %s client: %s", req.TaskId, req.Url, req.UrlMeta, clientAddr)
	var span trace.Span
	ctx, span = tracer.Start(ctx, constants.SpanObtainSeeds, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(constants.AttributeObtainSeedsRequest.String(req.String()))
	span.SetAttributes(constants.AttributeTaskID.String(req.TaskId))
	defer func() {
		if r := recover(); r != nil {
			err = dferrors.Newf(base.Code_UnknownError, "obtain task(%s) seeds encounter an panic: %v", req.TaskId, r)
			span.RecordError(err)
			logger.WithTaskID(req.TaskId).Errorf("%v", err)
		}
	}()
	peerID := idgen.CDNPeerID(css.config.AdvertiseIP)
	hostID := idgen.CDNHostID(hostutils.FQDNHostname, int32(css.config.ListenPort))
	// begin piece
	psc <- &cdnsystem.PieceSeed{
		PeerId:   peerID,
		HostUuid: hostID,
		PieceInfo: &base.PieceInfo{
			PieceNum: common.BeginOfPiece,
		},
		Done: false,
	}
	// register seed task
	registeredTask, pieceChan, err := css.service.RegisterSeedTask(ctx, clientAddr, task.NewSeedTask(req.TaskId, req.Url, req.UrlMeta))
	if err != nil {
		if supervisor.IsResourcesLacked(err) {
			err = dferrors.Newf(base.Code_ResourceLacked, "resources lacked for task(%s): %v", req.TaskId, err)
			span.RecordError(err)
			return err
		}
		err = dferrors.Newf(base.Code_CDNTaskRegistryFail, "failed to register seed task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return err
	}
	for piece := range pieceChan {
		pieceSeed := &cdnsystem.PieceSeed{
			PeerId:   peerID,
			HostUuid: hostID,
			PieceInfo: &base.PieceInfo{
				PieceNum:     int32(piece.PieceNum),
				RangeStart:   piece.PieceRange.StartIndex,
				RangeSize:    piece.PieceLen,
				PieceMd5:     piece.PieceMd5,
				PieceOffset:  piece.OriginRange.StartIndex,
				PieceStyle:   piece.PieceStyle,
				DownloadCost: piece.DownloadCost,
			},
			Done:            false,
			ContentLength:   registeredTask.SourceFileLength,
			TotalPieceCount: registeredTask.TotalPieceCount,
			BeginTime:       piece.BeginDownloadTime,
			EndTime:         piece.EndDownloadTime,
		}
		psc <- pieceSeed
		jsonPiece, err := json.Marshal(pieceSeed)
		if err != nil {
			logger.Errorf("failed to json marshal seed piece: %v", err)
		}
		logger.Debugf("send piece seed: %s to client: %s", jsonPiece, clientAddr)
	}
	seedTask, err := css.service.GetSeedTask(req.TaskId)
	if err != nil {
		err = dferrors.Newf(base.Code_CDNError, "failed to get task(%s): %v", req.TaskId, err)
		if task.IsTaskNotFound(err) {
			err = dferrors.Newf(base.Code_CDNTaskNotFound, "failed to get task(%s): %v", req.TaskId, err)
			span.RecordError(err)
			return err
		}
		err = dferrors.Newf(base.Code_CDNError, "failed to get task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return err
	}
	if !seedTask.IsSuccess() {
		err = dferrors.Newf(base.Code_CDNTaskDownloadFail, "task(%s) status error , status: %s", req.TaskId, seedTask.CdnStatus)
		span.RecordError(err)
		return err
	}
	pieceSeed := &cdnsystem.PieceSeed{
		PeerId:          peerID,
		HostUuid:        hostID,
		Done:            true,
		ContentLength:   seedTask.SourceFileLength,
		TotalPieceCount: seedTask.TotalPieceCount,
	}
	psc <- pieceSeed
	jsonPiece, err := json.Marshal(pieceSeed)
	if err != nil {
		logger.Errorf("failed to json marshal seed piece: %v", err)
	}
	logger.Debugf("send piece seed: %s to client: %s", jsonPiece, clientAddr)
	return nil
}

func (css *Server) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest) (piecePacket *base.PiecePacket, err error) {
	var span trace.Span
	_, span = tracer.Start(ctx, constants.SpanGetPieceTasks, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(constants.AttributeGetPieceTasksRequest.String(req.String()))
	span.SetAttributes(constants.AttributeTaskID.String(req.TaskId))
	logger.Infof("get piece tasks: %s", req)
	defer func() {
		if r := recover(); r != nil {
			err = dferrors.Newf(base.Code_UnknownError, "get task(%s) piece tasks encounter an panic: %v", req.TaskId, r)
			span.RecordError(err)
		}
		if err != nil {
			logger.WithTaskID(req.TaskId).Errorf("get piece tasks failed: %v", err)
		} else {
			logger.WithTaskID(req.TaskId).Infof("get piece tasks success, availablePieceCount(%d), totalPieceCount(%d), pieceMd5Sign(%s), "+
				"contentLength(%d)", len(piecePacket.PieceInfos), piecePacket.TotalPiece, piecePacket.PieceMd5Sign, piecePacket.ContentLength)
		}

	}()
	seedTask, err := css.service.GetSeedTask(req.TaskId)
	if err != nil {
		if task.IsTaskNotFound(err) {
			err = dferrors.Newf(base.Code_CDNTaskNotFound, "failed to get task(%s): %v", req.TaskId, err)
			span.RecordError(err)
			return nil, err
		}
		err = dferrors.Newf(base.Code_CDNError, "failed to get task(%s): %v", req.TaskId, err)
		span.RecordError(err)
		return nil, err
	}
	if seedTask.IsError() {
		err = dferrors.Newf(base.Code_CDNTaskDownloadFail, "task(%s) status is FAIL, cdnStatus: %s", seedTask.ID, seedTask.CdnStatus)
		span.RecordError(err)
		return nil, err
	}
	taskPieces, err := css.service.GetSeedPieces(req.TaskId)
	if err != nil {
		err = dferrors.Newf(base.Code_CDNError, "failed to get pieces of task(%s) from cdn: %v", seedTask.ID, err)
		span.RecordError(err)
		return nil, err
	}
	pieceInfos := make([]*base.PieceInfo, 0, len(taskPieces))
	var count uint32 = 0
	for _, piece := range taskPieces {
		if piece.PieceNum >= req.StartNum && (count < req.Limit || req.Limit <= 0) {
			p := &base.PieceInfo{
				PieceNum:     int32(piece.PieceNum),
				RangeStart:   piece.PieceRange.StartIndex,
				RangeSize:    piece.PieceLen,
				PieceMd5:     piece.PieceMd5,
				PieceOffset:  piece.OriginRange.StartIndex,
				PieceStyle:   piece.PieceStyle,
				DownloadCost: piece.DownloadCost,
			}
			pieceInfos = append(pieceInfos, p)
			count++
		}
	}
	pieceMd5Sign := seedTask.PieceMd5Sign
	// TODO The calculation of sign has been completed after the source has been completed. This is just a fallback
	if len(taskPieces) == int(seedTask.TotalPieceCount) && pieceMd5Sign == "" {
		logger.WithTaskID(req.TaskId).Warn("The code flow should not go to this point, if the output of this log need to check why")
		var pieceMd5s []string
		for i := 0; i < len(taskPieces); i++ {
			pieceMd5s = append(pieceMd5s, taskPieces[i].PieceMd5)
		}
		pieceMd5Sign = digestutils.Sha256(pieceMd5s...)
	}
	pp := &base.PiecePacket{
		TaskId:        req.TaskId,
		DstPid:        req.DstPid,
		DstAddr:       fmt.Sprintf("%s:%d", css.config.AdvertiseIP, css.config.DownloadPort),
		PieceInfos:    pieceInfos,
		TotalPiece:    seedTask.TotalPieceCount,
		ContentLength: seedTask.SourceFileLength,
		PieceMd5Sign:  pieceMd5Sign,
	}
	span.SetAttributes(constants.AttributePiecePacketResult.String(pp.String()))
	return pp, nil
}

func (css *Server) SyncPieceTasks(stream cdnsystem.Seeder_SyncPieceTasksServer) error {
	g, ctx := errgroup.WithContext(stream.Context())
	locker := sync.Mutex{}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		logger.WithTaskID(req.TaskId).Debugf("receive piece task request: %s", req)
		seedTask, err := css.service.GetSeedTask(req.TaskId)
		if err != nil {
			if task.IsTaskNotFound(err) {
				return status.Errorf(codes.Code(base.Code_CDNTaskNotFound), "failed to get task(%s): %v", req.TaskId, err)
			}
			return status.Errorf(codes.Code(base.Code_CDNError), "failed to get task(%s): %v", req.TaskId, err)
		}
		if seedTask.IsError() {
			return status.Errorf(codes.Code(base.Code_CDNError), "task(%s) status is FAIL, cdnStatus: %s", seedTask.ID, seedTask.CdnStatus)
		}
		g.Go(func() error {
			return css.sendTaskPieces(ctx, req, seedTask, stream, &locker)
		})
	}
	return g.Wait()
}

func (css *Server) sendTaskPieces(ctx context.Context, req *base.PieceTaskRequest, seedTask *task.SeedTask, stream cdnsystem.Seeder_SyncPieceTasksServer,
	locker sync.Locker) error {
	ch, err := css.service.WatchTaskProgress(ctx, req.TaskId)
	if err != nil {
		return err
	}
	var alreadySendCount uint32
	var alreadyDownloadCount int32
	for {
		select {
		//case <-ctx.Done():
		//	return nil
		case piece := <-ch:
			alreadyDownloadCount++
			if piece == nil {
				return nil
			}
			if piece.PieceNum >= req.StartNum && (alreadySendCount < req.Limit || req.Limit <= 0) {
				pieceMd5Sign := seedTask.PieceMd5Sign
				if alreadyDownloadCount == seedTask.TotalPieceCount && pieceMd5Sign == "" {
					logger.WithTaskID(req.TaskId).Info("calculate pieceMd5sign")
					pieceMd5Sign, err = css.service.GetPieceMd5Sign(req.TaskId)
					if err != nil {
						return err
					}
				}
				pp := &base.PiecePacket{
					TaskId:  req.TaskId,
					DstPid:  req.DstPid,
					DstAddr: fmt.Sprintf("%s:%d", css.config.AdvertiseIP, css.config.DownloadPort),
					PieceInfos: []*base.PieceInfo{{
						PieceNum:     int32(piece.PieceNum),
						RangeStart:   piece.PieceRange.StartIndex,
						RangeSize:    piece.PieceLen,
						PieceMd5:     piece.PieceMd5,
						PieceOffset:  piece.OriginRange.StartIndex,
						PieceStyle:   piece.PieceStyle,
						DownloadCost: piece.DownloadCost,
					}},
					TotalPiece:    seedTask.TotalPieceCount,
					ContentLength: seedTask.SourceFileLength,
					PieceMd5Sign:  pieceMd5Sign,
				}
				alreadySendCount++
				locker.Lock()
				if err := stream.Send(pp); err != nil {
					locker.Unlock()
					return err
				}
				locker.Unlock()
			}
			if alreadyDownloadCount == seedTask.TotalPieceCount || alreadySendCount == req.Limit {
				return nil
			}
		}
	}
}

func (css *Server) getTaskPieces(req *base.PieceTaskRequest) ([]*base.PieceInfo, error) {
	taskPieces, err := css.service.GetSeedPieces(req.TaskId)
	if err != nil {
		return nil, err
	}
	pieceInfos := make([]*base.PieceInfo, 0, len(taskPieces))
	var count uint32 = 0
	for _, piece := range taskPieces {
		if piece.PieceNum >= req.StartNum && (count < req.Limit || req.Limit <= 0) {
			p := &base.PieceInfo{
				PieceNum:     int32(piece.PieceNum),
				RangeStart:   piece.PieceRange.StartIndex,
				RangeSize:    piece.PieceLen,
				PieceMd5:     piece.PieceMd5,
				PieceOffset:  piece.OriginRange.StartIndex,
				PieceStyle:   piece.PieceStyle,
				DownloadCost: piece.DownloadCost,
			}
			pieceInfos = append(pieceInfos, p)
			count++
		}
	}
	return pieceInfos, nil
}

func (css *Server) ListenAndServe() error {
	// Generate GRPC listener
	lis, _, err := rpc.ListenWithPortRange(css.config.Listen, css.config.ListenPort, css.config.ListenPort)
	if err != nil {
		return err
	}
	//Started GRPC server
	logger.Infof("====starting grpc server at %s://%s====", lis.Addr().Network(), lis.Addr().String())
	return css.Server.Serve(lis)
}

const (
	gracefulStopTimeout = 10 * time.Second
)

func (css *Server) Shutdown() error {
	defer logger.Infof("====stopped rpc server====")
	stopped := make(chan struct{})
	go func() {
		css.Server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-time.After(gracefulStopTimeout):
		css.Server.Stop()
	case <-stopped:
	}
	return nil
}

func (css *Server) GetConfig() Config {
	return css.config
}
