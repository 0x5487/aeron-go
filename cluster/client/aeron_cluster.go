// Copyright 2022 Steven Stern
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/cluster"
	"github.com/lirm/aeron-go/cluster/codecs"
)

var logger = logging.MustGetLogger("cluster-client")
var marshaller = codecs.NewSbeGoMarshaller()

var TemporaryError = errors.New("temporary error")

// ingressPublication is the part of *aeron.Publication the client uses. It is the seam to the media driver: the tests
// replace the publications the client creates.
type ingressPublication interface {
	IsConnected() bool
	IsClosed() bool
	Close() error
	Channel() string
	Offer(buffer *atomic.Buffer, offset, length int32, reservedValueSupplier term.ReservedValueSupplier) int64
	Offer2(
		bufferOne *atomic.Buffer, offsetOne, lengthOne int32,
		bufferTwo *atomic.Buffer, offsetTwo, lengthTwo int32,
		reservedValueSupplier term.ReservedValueSupplier,
	) int64
}

// egressSubscription is the part of *aeron.Subscription the client uses (the other seam to the media driver).
type egressSubscription interface {
	Poll(handler term.FragmentHandler, fragmentLimit int) int
	TryResolveChannelEndpointPort() string
	Close() error
}

type AeronCluster struct {
	opts                *Options
	aeronClient         *aeron.Aeron
	egressSub           egressSubscription
	ingressChannel      *aeron.ChannelUri
	ingressPub          ingressPublication
	clusterSessionId    int64
	leadershipTermId    int64
	leaderMemberId      int32
	memberByIdMap       map[int32]*memberIngress
	fragmentAssembler   *aeron.FragmentAssembler
	egressHandler       term.FragmentHandler // ac.fragmentAssembler.OnFragment, bound in bindEgressHandler
	egressListener      EgressListener
	sessionMsgHdrBuffer *atomic.Buffer
	keepAliveBuffer     *atomic.Buffer
	state               clientState
	correlationId       int64
	awaitTimeoutTime    int64
	// addPublication creates an ingress publication on channel; the tests replace it.
	addPublication func(channel string) (ingressPublication, error)
	// clock is the time source of the timeouts; the tests replace it.
	clock func() time.Time
}

type memberIngress struct {
	memberId    int32
	endpoint    string
	publication ingressPublication
}

type clientState int8

const (
	clientCreatePublications clientState = iota
	clientAwaitPublicationConnected
	clientAwaitConnectReply
	clientConnected
	// clientAwaitNewLeaderConnection: the session is open and the cluster announced a new leader; the ingress
	// publication to the new leader is not connected yet (Java: AWAIT_NEW_LEADER_CONNECTION).
	clientAwaitNewLeaderConnection
	// clientClosed: the client is finished. It gave up (a timeout, ERROR, AUTHENTICATION_REJECTED; the listener's OnError
	// says why) or the cluster closed the session. It never connects again by itself: the owner calls Close and builds a
	// new client.
	clientClosed
)

// newLeaderConnectTimeout is how long the client waits for the ingress publication of a new leader to connect
// (Java: AeronCluster.Context.messageTimeoutNs, default 5 s).
const newLeaderConnectTimeout = 5 * time.Second

const (
	protocolMajorVersion = 0
	protocolMinorVersion = 2
	protocolPatchVersion = 0
)

var protocolSemanticVersion = util.SemanticVersionCompose(
	protocolMajorVersion, protocolMinorVersion, protocolPatchVersion)

func NewAeronCluster(
	aeronCtx *aeron.Context,
	options *Options,
	egressListener EgressListener,
) (*AeronCluster, error) {
	if egressListener == nil {
		return nil, fmt.Errorf("egressListener is nil")
	}
	ingressChannel, err := aeron.ParseChannelUri(options.IngressChannel)
	if err != nil {
		return nil, err
	}
	if ingressChannel.IsIpc() && options.IngressEndpoints != "" {
		return nil, fmt.Errorf("IngressEndpoints must be empty when using IPC ingress")
	}

	aeronClient, err := aeron.Connect(aeronCtx)
	if err != nil {
		return nil, err
	}

	egressSub, err := aeronClient.AddSubscription(options.EgressChannel, options.EgressStreamId)
	if err != nil {
		return nil, err
	}

	sessionMsgHdrBuf := codecs.MakeClusterMessageBuffer(cluster.SessionMessageHeaderTemplateId, cluster.SessionMessageHdrBlockLength)

	client := &AeronCluster{
		opts:                options,
		aeronClient:         aeronClient,
		egressSub:           egressSub,
		ingressChannel:      &ingressChannel,
		clusterSessionId:    cluster.NullValue,
		leadershipTermId:    cluster.NullValue,
		leaderMemberId:      cluster.NullValue,
		memberByIdMap:       make(map[int32]*memberIngress),
		egressListener:      egressListener,
		state:               clientCreatePublications,
		sessionMsgHdrBuffer: sessionMsgHdrBuf,
		keepAliveBuffer:     codecs.MakeClusterMessageBuffer(cluster.SessionKeepAliveTemplateId, 16),
		clock:               time.Now,
	}
	client.addPublication = client.addDriverPublication
	client.bindEgressHandler()
	client.updateMemberEndpoints(options.IngressEndpoints)

	return client, nil
}

func (ac *AeronCluster) ClusterSessionId() int64 {
	return ac.clusterSessionId
}

func (ac *AeronCluster) LeadershipTermId() int64 {
	return ac.leadershipTermId
}

func (ac *AeronCluster) LeaderMemberId() int32 {
	return ac.leaderMemberId
}

func (ac *AeronCluster) IsConnected() bool {
	return ac.state == clientConnected
}

func (ac *AeronCluster) IsClosed() bool {
	return ac.state == clientClosed
}

func (ac *AeronCluster) Poll() int {
	switch ac.state {
	case clientCreatePublications:
		ret, err := ac.createPublications()
		if err != nil {
			logger.Warningf("error from createPublications %v", err)
		}
		return ret
	case clientAwaitPublicationConnected:
		ret, err := ac.awaitPublicationConnected()
		if err != nil {
			logger.Warningf("error from awaitPublicationConnected %v", err)
		}
		return ret
	case clientAwaitConnectReply:
		if ac.ingressPub.IsConnected() && ac.nowMillis() < ac.awaitTimeoutTime {
			return ac.pollEgress(1)
		}
		ac.giveUp("timed out waiting for session connect reply")
		return 1
	case clientConnected:
		if ac.ingressPub.IsConnected() {
			return ac.pollEgress(10)
			// TODO: check if state == closed
		} else {
			ac.egressListener.OnDisconnect(ac, "ingress publication disconnected")
			ac.state = clientCreatePublications
		}
	case clientAwaitNewLeaderConnection:
		work := ac.pollEgress(10) // another new leader, or the session closing, may be in it
		if ac.state != clientAwaitNewLeaderConnection {
			return work
		}
		if ac.ingressPub != nil && ac.ingressPub.IsConnected() {
			ac.state = clientConnected
			return work + 1
		}
		if ac.nowMillis() >= ac.awaitTimeoutTime {
			ac.giveUp(fmt.Sprintf("timed out waiting for the ingress publication of the new leader (leaderMemberId=%d) to connect",
				ac.leaderMemberId))
			return work + 1
		}
		return work
	}
	return 0
}

func (ac *AeronCluster) nowMillis() int64 {
	return ac.clock().UnixMilli()
}

// giveUp ends the client: it tells the listener why through OnError and becomes closed. The client does not schedule a
// retry; reconnecting is the owner's decision (Java closes the client the same way when it times out).
func (ac *AeronCluster) giveUp(reason string) {
	logger.Warningf("cluster client gives up: %s (state=%d)", reason, ac.state)
	ac.state = clientClosed
	ac.egressListener.OnError(ac, reason)
}

func (ac *AeronCluster) Offer(buffer *atomic.Buffer, offset, length int32) int64 {
	if ac.state != clientConnected {
		return aeron.NotConnected
	} else {
		hdrBuf := ac.sessionMsgHdrBuffer
		return ac.ingressPub.Offer2(hdrBuf, 0, hdrBuf.Capacity(), buffer, offset, length, nil)
	}
}

func (ac *AeronCluster) SendKeepAlive() bool {
	if !ac.IsConnected() {
		return false
	}
	buf := ac.keepAliveBuffer
	for i := 0; i < 3; i++ {
		if result := ac.ingressPub.Offer(buf, 0, buf.Capacity(), nil); result >= 0 {
			return true
		}
		ac.opts.IdleStrategy.Idle(0)
	}
	return false
}

func (ac *AeronCluster) Close() {
	if ac.IsConnected() && ac.ingressPub.IsConnected() {
		ac.sendCloseSession()
	}
	if ac.ingressPub != nil {
		if err := ac.ingressPub.Close(); err != nil {
			logger.Debugf("error closing ingress publication: %v", err)
		}
		ac.ingressPub = nil
	}
	if ac.egressSub != nil {
		if err := ac.egressSub.Close(); err != nil {
			logger.Debugf("error closing egress subscription: %v", err)
		}
		ac.egressSub = nil
	}
	if err := ac.aeronClient.Close(); err != nil {
		logger.Debugf("error closing aeron client: %v", err)
	}
	ac.state = clientClosed
}

func (ac *AeronCluster) updateMemberEndpoints(endpoints string) {
	if endpoints == "" {
		return
	}
	logger.Debugf("updateMemberEndpoints: %s", endpoints)
	for idx, endpoint := range strings.Split(endpoints, ",") {
		if delim := strings.IndexByte(endpoint, '='); delim > 0 {
			memberId, err := strconv.ParseInt(endpoint[:delim], 10, 32)
			if err != nil {
				logger.Warningf("invalid endpoint at idx=%d: %s", idx, endpoint)
				continue
			}
			address := endpoint[delim+1:]
			member := ac.memberByIdMap[int32(memberId)]
			if member == nil {
				member = &memberIngress{
					memberId: int32(memberId),
					endpoint: address,
				}
				ac.memberByIdMap[member.memberId] = member
			} else if address != member.endpoint {
				member.endpoint = address
				member.close()
			}
			if member.memberId == ac.leaderMemberId {
				if !member.hasOpenPublication() {
					pub, err := ac.createIngressPublication(address)
					if err == nil {
						member.publication = pub
					} else {
						logger.Warning(err)
					}
				}
				ac.ingressPub = member.publication
				ac.fragmentAssembler.Clear()
			}
		} else {
			logger.Warningf("endpoint at idx=%d missing '=' separator: %s", idx, endpoint)
		}
	}
}

func (ac *AeronCluster) createPublications() (int, error) {
	if len(ac.memberByIdMap) > 0 {
		for _, member := range ac.memberByIdMap {
			if !member.hasOpenPublication() {
				pub, err := ac.createIngressPublication(member.endpoint)
				if err != nil {
					return 0, err
				}
				member.publication = pub
			}
		}
	} else if ac.ingressPub == nil {
		pub, err := ac.createIngressPublication(ac.opts.IngressChannel)
		if err != nil {
			return 0, err
		}
		ac.ingressPub = pub
	}
	ac.state = clientAwaitPublicationConnected
	ac.awaitTimeoutTime = ac.nowMillis() + (5 * time.Second).Milliseconds()
	return 1, nil
}

func (ac *AeronCluster) createIngressPublication(endpoint string) (ingressPublication, error) {
	if ac.ingressChannel.IsUdp() {
		ac.ingressChannel.Set("endpoint", endpoint)
	}
	channel := ac.ingressChannel.String()
	logger.Debugf("createIngressPublication - endpoint=%s isUdp=%v isExclusive=%v",
		endpoint, ac.ingressChannel.IsUdp(), ac.opts.IsIngressExclusive)
	return ac.addPublication(channel)
}

// addDriverPublication adds the ingress publication to the media driver. A failure returns a nil interface, not an
// interface holding a nil *aeron.Publication.
func (ac *AeronCluster) addDriverPublication(channel string) (ingressPublication, error) {
	var pub *aeron.Publication
	var err error
	if ac.opts.IsIngressExclusive {
		pub, err = ac.aeronClient.AddExclusivePublication(channel, ac.opts.IngressStreamId)
	} else {
		pub, err = ac.aeronClient.AddPublication(channel, ac.opts.IngressStreamId)
	}
	if err != nil {
		return nil, err
	}
	return pub, nil
}

func (ac *AeronCluster) awaitPublicationConnected() (int, error) {
	responseChannel := ac.egressSub.TryResolveChannelEndpointPort()
	if responseChannel == "" {
		// TODO: Is this an error or success condition?
		return 0, nil
	}
	now := ac.nowMillis()
	if now > ac.awaitTimeoutTime {
		ac.giveUp("timed out waiting for connected publication")
		return 0, nil
	}
	if len(ac.memberByIdMap) > 0 {
		for _, member := range ac.memberByIdMap {
			if member.publication != nil && member.publication.IsConnected() {
				ac.ingressPub = member.publication
				ac.fragmentAssembler.Clear()
				err := ac.sendConnectRequest(responseChannel)
				if err == nil {
					logger.Debugf("sent connect request to memberId=%d correlationId=%d channel=%s",
						member.memberId, ac.correlationId, member.publication.Channel())
					ac.state = clientAwaitConnectReply
					ac.awaitTimeoutTime = now + (3 * time.Second).Milliseconds()
					break
				}
				if !errors.Is(err, TemporaryError) {
					return 0, err
				}
			}
		}
	} else if ac.ingressPub.IsConnected() && ac.sendConnectRequest(responseChannel) == nil {
		ac.state = clientAwaitConnectReply
		ac.awaitTimeoutTime = now + (3 * time.Second).Milliseconds()
	}
	return 0, nil
}

// Returns nil on success, TemporaryError, or any other error is a permanent error.
func (ac *AeronCluster) sendConnectRequest(responseChannel string) error {
	ac.correlationId = ac.aeronClient.NextCorrelationID()
	req := codecs.SessionConnectRequest{
		CorrelationId:    ac.correlationId,
		ResponseStreamId: ac.opts.EgressStreamId,
		Version:          int32(protocolSemanticVersion),
		ResponseChannel:  []byte(responseChannel),
	}
	header := codecs.MessageHeader{
		BlockLength: req.SbeBlockLength(),
		TemplateId:  req.SbeTemplateId(),
		SchemaId:    req.SbeSchemaId(),
		Version:     req.SbeSchemaVersion(),
	}
	writer := new(bytes.Buffer)
	if err := header.Encode(marshaller, writer); err != nil {
		return err
	}
	if err := req.Encode(marshaller, writer, true); err != nil {
		return err
	}
	buffer := atomic.MakeBuffer(writer.Bytes())
	result := ac.ingressPub.Offer(buffer, 0, buffer.Capacity(), nil)
	if result >= 0 {
		return nil
	} else {
		return fmt.Errorf("%w, failed to send connect request, channel=%s result=%d",
			TemporaryError, ac.ingressPub.Channel(), result)
	}
}

// bindEgressHandler creates the egress fragment assembler and stores its handler, bound once. Passing
// ac.fragmentAssembler.OnFragment to Poll on every round allocates a closure per round.
func (ac *AeronCluster) bindEgressHandler() {
	ac.fragmentAssembler = aeron.NewFragmentAssembler(ac.onFragment, 0)
	ac.egressHandler = ac.fragmentAssembler.OnFragment
}

func (ac *AeronCluster) pollEgress(fragmentLimit int) int {
	return ac.egressSub.Poll(ac.egressHandler, fragmentLimit)
}

func (ac *AeronCluster) onFragment(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
	if length < cluster.SBEHeaderLength {
		return
	}
	blockLength := buffer.GetUInt16(offset)
	templateId := buffer.GetUInt16(offset + 2)
	schemaId := buffer.GetUInt16(offset + 4)
	version := buffer.GetUInt16(offset + 6)
	if schemaId != cluster.ClusterSchemaId {
		logger.Errorf("unexpected schemaId=%d templateId=%d blockLen=%d version=%d",
			schemaId, templateId, blockLength, version)
		return
	}
	offset += cluster.SBEHeaderLength
	length -= cluster.SBEHeaderLength

	switch templateId {
	case cluster.SessionMessageHeaderTemplateId:
		ac.onSessionMessage(buffer, offset, length, header)
	case cluster.SessionEventTemplateId:
		ac.onSessionEvent(buffer, offset, length, version, blockLength)
	case cluster.NewLeaderEventTemlateId:
		ac.onNewLeaderEvent(buffer, offset, length, version, blockLength)
	case cluster.ChallengeTemplateId:
		e := codecs.Challenge{}
		buf := bytes.Buffer{}
		buffer.WriteBytes(&buf, offset, length)
		if err := e.Decode(marshaller, &buf, version, blockLength, true); err != nil {
			logger.Errorf("new leader event decode error: %v", err)
		} else {
			logger.Warningf("received challenge, corrId=%d clusterSessionId=%d", e.CorrelationId, e.ClusterSessionId)
		}
	}
}

func (ac *AeronCluster) onNewLeaderEvent(buffer *atomic.Buffer, offset, length int32, version, blockLength uint16) {
	e := codecs.NewLeaderEvent{}
	buf := bytes.Buffer{}
	buffer.WriteBytes(&buf, offset, length)
	if err := e.Decode(marshaller, &buf, version, blockLength, true); err != nil {
		logger.Errorf("new leader event decode error: %v", err)
	} else if ac.hasSession() && e.ClusterSessionId == ac.clusterSessionId {
		ac.leadershipTermId = e.LeadershipTermId
		ac.leaderMemberId = e.LeaderMemberId
		ac.sessionMsgHdrBuffer.PutInt64(cluster.SBEHeaderLength, e.LeadershipTermId)
		ac.keepAliveBuffer.PutInt64(cluster.SBEHeaderLength, e.LeadershipTermId)
		if ac.opts.IngressEndpoints != "" {
			ac.closeAllPublications()
			ac.updateMemberEndpoints(string(e.IngressEndpoints))
		}
		ac.state = clientAwaitNewLeaderConnection
		ac.awaitTimeoutTime = ac.nowMillis() + newLeaderConnectTimeout.Milliseconds()
		ac.fragmentAssembler.Clear()
		ac.egressListener.OnNewLeader(ac, e.LeadershipTermId, e.LeaderMemberId)
	} else {
		logger.Debugf("ignored new leader event - state=%v thisSessionId=%d targetSessionId=%d leaderMemberId=%d leaderTermId=%d",
			ac.state, ac.clusterSessionId, e.ClusterSessionId, e.LeaderMemberId, e.LeadershipTermId)
	}
}

func (ac *AeronCluster) onSessionEvent(buffer *atomic.Buffer, offset, length int32, version, blockLength uint16) {
	e := codecs.SessionEvent{}
	buf := bytes.Buffer{}
	buffer.WriteBytes(&buf, offset, length)
	if err := e.Decode(marshaller, &buf, version, blockLength, true); err != nil {
		logger.Errorf("session event decode error: %v", err)
	} else if ac.state == clientAwaitConnectReply && e.CorrelationId == ac.correlationId {
		switch e.Code {
		case codecs.EventCode.OK:
			ac.leadershipTermId = e.LeadershipTermId
			ac.leaderMemberId = e.LeaderMemberId
			ac.clusterSessionId = e.ClusterSessionId
			ac.sessionMsgHdrBuffer.PutInt64(cluster.SBEHeaderLength, e.LeadershipTermId)
			ac.sessionMsgHdrBuffer.PutInt64(cluster.SBEHeaderLength+8, e.ClusterSessionId)
			ac.keepAliveBuffer.PutInt64(cluster.SBEHeaderLength, e.LeadershipTermId)
			ac.keepAliveBuffer.PutInt64(cluster.SBEHeaderLength+8, e.ClusterSessionId)
			ac.state = clientConnected
			ac.closeNonLeaderPublications()
			ac.egressListener.OnConnect(ac)
		case codecs.EventCode.REDIRECT:
			logger.Infof("got redirect - leaderTermId=%d leaderMemberId=%d", e.LeadershipTermId, e.LeaderMemberId)
			ac.leaderMemberId = e.LeaderMemberId
			ac.updateMemberEndpoints(string(e.Detail))
			ac.closeNonLeaderPublications()
			ac.state = clientAwaitPublicationConnected
		case codecs.EventCode.ERROR:
			ac.giveUp(string(e.Detail))
		case codecs.EventCode.AUTHENTICATION_REJECTED:
			ac.giveUp(fmt.Sprintf("authentication rejected (%s)", string(e.Detail)))
		}
	} else if ac.hasSession() && e.ClusterSessionId == ac.clusterSessionId {
		if e.Code == codecs.EventCode.CLOSED {
			ac.egressListener.OnDisconnect(ac, string(e.Detail))
			ac.state = clientClosed
		} else if e.Code == codecs.EventCode.ERROR {
			ac.egressListener.OnError(ac, string(e.Detail))
		} else {
			logger.Infof("onSessionEvent - code=%v (%s)", e.Code, string(e.Detail))
		}
	} else {
		logger.Debugf("ignored session event - state=%v thisSessionId=%d targetSessionId=%d code=%d (%s)",
			ac.state, ac.clusterSessionId, e.ClusterSessionId, e.Code, string(e.Detail))
	}
}

// hasSession says the session is open: connected, or waiting for the ingress publication of a new leader.
func (ac *AeronCluster) hasSession() bool {
	return ac.state == clientConnected || ac.state == clientAwaitNewLeaderConnection
}

func (ac *AeronCluster) closeNonLeaderPublications() {
	for _, member := range ac.memberByIdMap {
		if member.memberId != ac.leaderMemberId {
			member.close()
		}
	}
}

func (ac *AeronCluster) onSessionMessage(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
	if length < cluster.SessionMessageHeaderLength {
		logger.Errorf("received invalid session message - length: %d", length)
		return
	}
	leadershipTermId := buffer.GetInt64(offset)
	clusterSessionId := buffer.GetInt64(offset + 8)
	timestamp := buffer.GetInt64(offset + 16)
	if !ac.hasSession() {
		logger.Debugf("received unexpected session message - leadershipTermId=%d clusterSessionId=%d state=%v",
			leadershipTermId, clusterSessionId, ac.state)
	} else if clusterSessionId == ac.clusterSessionId {
		ac.egressListener.OnMessage(ac, timestamp, buffer, offset+cluster.SessionMessageHeaderLength,
			length-cluster.SessionMessageHeaderLength, header)
	} else {
		logger.Debugf("received unexpected session msg - leaderTermId=%d targetSessionId=%d thisSessionId=%d",
			leadershipTermId, clusterSessionId, ac.clusterSessionId)
	}
}

func (ac *AeronCluster) sendCloseSession() {
	buf := codecs.MakeClusterMessageBuffer(cluster.SessionCloseRequestTemplateId, 16)
	buf.PutInt64(cluster.SBEHeaderLength, ac.leadershipTermId)
	buf.PutInt64(cluster.SBEHeaderLength+8, ac.clusterSessionId)
	for i := 0; i < 3; i++ {
		if result := ac.ingressPub.Offer(buf, 0, buf.Capacity(), nil); result >= 0 {
			return
		}
		ac.opts.IdleStrategy.Idle(0)
	}
}

// closeAllPublications closes the ingress publication of every member and forgets them, so that a member that leads
// again later gets a new one (a closed publication never connects). Java closes the whole member table the same way
// (AeronCluster.updateMemberEndpoints).
func (ac *AeronCluster) closeAllPublications() {
	for _, member := range ac.memberByIdMap {
		member.close()
	}
	if ac.ingressPub != nil {
		// not a member's publication: the client was built without endpoints
		if err := ac.ingressPub.Close(); err != nil {
			logger.Warningf("error closing ingress publication: %v", err)
		}
	}
	ac.ingressPub = nil
}

// hasOpenPublication says the member has a publication that is not closed.
func (member *memberIngress) hasOpenPublication() bool {
	return member.publication != nil && !member.publication.IsClosed()
}

func (member *memberIngress) close() {
	if member.publication != nil {
		if err := member.publication.Close(); err != nil {
			logger.Warningf("error closing member publication, memberId=%d endpoint=%s: %v",
				member.memberId, member.endpoint, err)
		}
		member.publication = nil
	}
}
