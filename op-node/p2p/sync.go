package p2p

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"golang.org/x/time/rate"

	"github.com/ethereum/go-ethereum/log"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/ethereum-optimism/optimism/op-node/eth"
)

// Limit the maximum range to request at a time.
const maxRangePerWorker = 10

type newStreamFn func(ctx context.Context, peerId peer.ID, protocolId ...protocol.ID) (network.Stream, error)

type receivePayload = func(ctx context.Context, from peer.ID, payload *eth.ExecutionPayload) error

type syncRequest struct {
	start, end uint64
}

type P2PSyncClient struct {
	log log.Logger

	newStreamFn newStreamFn

	sync.Mutex
	// syncing worker per peer
	peers map[peer.ID]context.CancelFunc

	requests chan syncRequest

	resCtx    context.Context
	resCancel context.CancelFunc

	receivePayload receivePayload
	wg             sync.WaitGroup
}

func NewP2PSyncClient(log log.Logger, newStream newStreamFn, rcv receivePayload) *P2PSyncClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &P2PSyncClient{
		log:            log,
		newStreamFn:    newStream,
		peers:          make(map[peer.ID]context.CancelFunc),
		requests:       make(chan syncRequest, 128),
		resCtx:         ctx,
		resCancel:      cancel,
		receivePayload: rcv,
	}
}

// TODO we should be calling AddPeer() and RemovePeer() upon new connect/disconnect events

func (s *P2PSyncClient) AddPeer(id peer.ID) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.peers[id]; ok {
		return nil
	}
	s.wg.Add(1)
	// add new peer routine
	ctx, cancel := context.WithCancel(s.resCtx)
	s.peers[id] = cancel
	go s.eventLoop(ctx, id)
	return nil
}

func (s *P2PSyncClient) RemovePeer(id peer.ID) error {
	s.Lock()
	defer s.Unlock()
	cancel, ok := s.peers[id]
	if !ok {
		return fmt.Errorf("cannot remove peer from sync duties, %s is not registered", id)
	}
	cancel() // once loop exits
	delete(s.peers, id)
	return nil
}

func (s *P2PSyncClient) Close() error {
	s.resCancel()
	s.wg.Wait()
	return nil
}

func (s *P2PSyncClient) RequestL2Range(ctx context.Context, start, end uint64) error {
	// Drain previous requests now that we have new information
	for len(s.requests) > 0 {
		<-s.requests
	}

	for i := start; i < end; i += maxRangePerWorker {
		r := syncRequest{start: i, end: i + maxRangePerWorker}
		if r.end > end {
			r.end = end
		}
		s.log.Info("Scheduling P2P blocks by range request", "start", r.start, "end", r.end, "size", r.end-r.start)
		// schedule new range to be requested
		select {
		case s.requests <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// eventLoop for syncing from a single peer
func (s *P2PSyncClient) eventLoop(ctx context.Context, id peer.ID) {
	defer func() {
		s.Lock()
		delete(s.peers, id) // clean up
		s.wg.Done()
		s.Unlock()
		s.log.Debug("stopped syncing loop of peer", "id", id)
	}()

	log := s.log.New("server", id)
	log.Info("Starting P2P sync client event loop")

	var rl rate.Limiter

	// allow 1 request per 10 ms
	rl.SetLimit(rate.Every(time.Millisecond * 10))
	rl.SetBurst(10) // and burst up to 10 items over that at any time

	t := time.NewTimer(0)
	for {
		now := time.Now()
		// try to reserve 10 events, so we can do a reasonably sized request without having to throttle ourselves
		res := rl.ReserveN(now, 10)
		d := res.DelayFrom(now)
		t.Reset(d)

		select {
		case now = <-t.C:
			// once the peer is available, wait for a sync request
			select {
			case r := <-s.requests:
				log := log.New("start", r.start, "end", r.end)
				if err := s.doRequest(ctx, log, id, &rl, r); err != nil {
					log.Warn("failed p2p sync request", "err", err)
					// If we hit an error, then count it as many requests.
					// We'd like to avoid making more requests for a while, to back off.
					_ = rl.ReserveN(now, 100)
				}
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *P2PSyncClient) doRequest(ctx context.Context, log log.Logger, id peer.ID, rl *rate.Limiter, r syncRequest) error {
	// check if peer is alive still.
	// if not, then reschedule the request, and exit the event loop

	// sync. Utilize our burst allowance if the request is large.

	// open stream to peer
	reqCtx, reqCancel := context.WithTimeout(ctx, time.Second*5)
	str, err := s.newStreamFn(reqCtx, id)
	reqCancel()
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	// set write timeout
	if err := str.SetWriteDeadline(time.Now().Add(time.Second * 5)); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}
	// TODO write range request
	if err := str.CloseWrite(); err != nil {
		return fmt.Errorf("failed to close writer side while making request: %w", err)
	}

	// TODO for each response:
	//  - reset read timeout
	//  - validate response (incl. signature)
	//  - track duration it took to get it
	//  - update scores
	//  - update rate limit after initial 10 responses that we reserved

	// TODO reschedule new sync request if responses finish prematurely,
	// and update the range to skip the already received prefix of blocks.
	return nil
}

func (s *P2PSyncClient) reschedule(r syncRequest) {
	select {
	case s.requests <- r:
	default:
		s.log.Warn("failed to reschedule sync request, sync client is too busy", "start", r.start, "end", r.end)
	}
}

// TODO function to write request to stream

// TODO function to read request from stream

// TODO function to write response chunk to stream

// TODO function to read response chunk from stream

// StreamCtxFn provides a new context to use when handling stream requests
type StreamCtxFn func() context.Context

const (
	serverReadRequestTimeout = time.Second * 10
	serverWriteChunkTimeout  = time.Second * 10
)

// peerStat maintains rate-limiting data of a peer that requests blocks from us.
type peerStat struct {
	// Requests tokenizes each request to sync
	Requests rate.Limiter
	// Chunks tokenizes each requested L2 block
	Chunks rate.Limiter
}

// NewP2PSyncServer creates a LibP2P stream handler function to register the L2 unsafe payloads alt-sync protocol.
//
// TODO: register this as protocol handler on p2p node
// TODO: add interface argument to fetch payloads by range from engine with
// TODO: add interface argument to retrieve cached payload signatures, to serve with the payloads. Without this data, we can only serve unsigned payloads.
func NewP2PSyncServer(log log.Logger, newCtx StreamCtxFn) func(stream network.Stream) {
	// We should never allow over 10K different peers to churn through quickly,
	// so it's fine to prune rate-limit details past this.

	peerRateLimits, _ := simplelru.NewLRU[peer.ID, *peerStat](10000, nil)
	// 3 sync requests per second, with 2 burst
	globalRequestsRL := rate.NewLimiter(3, 2)
	// serve at most 50 blocks per second to external peers
	globalChunksRL := rate.NewLimiter(50, 3)

	var peerStatsLock sync.RWMutex

	// note that the same peer may open parallel streams
	return func(stream network.Stream) {
		peerId := stream.Conn().RemotePeer()
		defer stream.Close()

		// TODO find rate limiting data of peer, or add otherwise
		ctx, cancel := context.WithCancel(newCtx())
		defer cancel()

		log := log.New("peer", peerId)
		// take a token from the global rate-limiter,
		// to make sure there's not too much concurrent server work between different peers.
		if err := globalRequestsRL.Wait(ctx); err != nil {
			log.Warn("timed out waiting for global sync rate limit", "err", err)
			return
		}

		peerStatsLock.Lock()
		ps, _ := peerRateLimits.Get(peerId)
		if ps == nil {
			ps = &peerStat{
				Requests: rate.Limiter{}, // TODO
				Chunks:   rate.Limiter{},
			}
			peerRateLimits.Add(peerId, ps)
		}
		peerStatsLock.Unlock()

		// charge a token as intrinsic cost for bothering us with the request.
		if err := ps.Requests.Wait(ctx); err != nil {
			log.Warn("timed out waiting for global sync rate limit", "err", err)
			return
		}

		if err := stream.SetReadDeadline(time.Now().Add(serverReadRequestTimeout)); err != nil {
			log.Warn("failed to set reading deadline", "err", err)
		}

		// TODO read request
		var req syncRequest

		if err := stream.CloseRead(); err != nil {
			log.Warn("failed to close reading-side of a P2P sync request call", "err", err)
			return
		}

		log = log.New("start", req.start, "end", req.end)

		// TODO validate req bounds
		// lower bound; genesis
		// upper bound; current head

		// TODO determine ideal chunk size to serve responses in
		// TODO: rate limiter must be able to handle this
		maxRangeSpan := uint64(10)

		t := time.NewTimer(123)
		defer t.Stop()
		for i := req.start; i < req.end; i += maxRangeSpan {
			// clip to end
			end := i + maxRangeSpan
			if end > req.end {
				end = req.end
			}
			now := time.Now()
			// Reserve work capacity from rate limiter so we can do the work without sweating.
			// Both from the global as well as peer-specific limiter, to avoid serving too many peers at once,
			// while also protecting against spam from a single peer.
			peerRes := ps.Chunks.ReserveN(now, int(end-i))
			globalRes := globalChunksRL.ReserveN(now, int(end-i))
			peerDelay := peerRes.DelayFrom(now)
			globalDelay := globalRes.DelayFrom(now)
			delay := peerDelay
			if peerDelay < globalDelay {
				delay = globalDelay
			}
			// We wait as long as necessary; we throttle the peer instead of disconnecting.
			// If the requester thinks we're taking too long, then it's their problem and they can disconnect.
			// We'll disconnect ourselves only when failing to read/write,
			// if the work is invalid (range validation), or when individual sub tasks timeout.
			t.Reset(delay)
			select {
			case <-t.C:
				// we are ready to serve now

				// TODO engine API; request payloads-by-range

				// We set a write deadline, to safely write without blocking on a throttling peer connection
				if err := stream.SetWriteDeadline(time.Now().Add(serverWriteChunkTimeout)); err != nil {
					log.Warn("failed to update write deadline")
					return
				}
				// TODO write response chunks

			case <-ctx.Done():
				log.Warn("p2p response handling timed out", "err", ctx.Err())
				return
			}
		}

	}
}
