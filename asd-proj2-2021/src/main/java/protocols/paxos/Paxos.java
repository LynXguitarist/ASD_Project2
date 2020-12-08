package protocols.paxos;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

/*
 * Proposer sends proposal to acceptor
 * proposal is selected when majority of acceptors accept it (f < N/2)
 * Sequence Number(psn) = instanceNumber
 * A proposed value that was accepted by a majority of acceptors is said to be locked in
 */
public class Paxos extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(Paxos.class);

	// Protocol information, to register in Babel
	public final static short PROTOCOL_ID = 103;
	public final static String PROTOCOL_NAME = "Paxos";

	private Host myself;
	private int joinedInstance;
	private List<Host> membership;

	private List<Integer> decisions;
	private Map<Integer, Integer> proposals; // <proposal_sn, value>

	private int state; // = initial_state
	private int instanceNumber;

	// State: np (highest prepare), na , va (highest accept)
	/* This state is maintained in stable storage */

	// Leaner -> State: decision, na , va , aset

	public Paxos(Properties props) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		joinedInstance = -1; // -1 means we have not yet joined the system
		membership = null;

		/*--------------------- Register Timer Handlers ----------------------------- */

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
		registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
		registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

		/*--------------------- Register Notification Handlers ----------------------------- */
		subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
		subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
	}

	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		// Nothing to do here, we just wait for events from the application or agreement
	}

	public int getState() {
		return state;
	}

	// ---------------------------------------Paxos_Proposer----------------------------//

	private void propose(int v) {
//		while(true) do
//			choose unique sn , higher than any n seen so far
//			send PREPARE(sn) to all acceptors
//			if PREPARE_OK(sna , va) from majority then
//			va = va with highest sna (or choose v otherwise)
//			send ACCEPT (sn , va) to all acceptors
//			if ACCEPT_OK(n) from majority then
//			send DECIDED(va ) to client
//			break
//			else //timeout on waiting ACCEPT_OK
//			continue in while
//			else //timeout on waiting PREPARE_OK
//			continue in while

	}

	// ---------------------------------------Paxos_Acceptor----------------------------//
	private void prepare(int n) {
//		if n > np then
//		np = n // will not accept anything <n
//		reply <
//		PREPARE_OK,na,va
	}

	private void accept(int n, int v) {
//		if n >= np then
//				na = n
//				va = v
//				reply with <ACCEPT_OK,n>
//				send <ACCEPT_OK,na,va > to all learners
	}

	// ---------------------------------------Paxos_Leaners----------------------------//

	// receive message ACCEPT_OK from acceptor a
	private void accepted(int n, int v) {
//		if n > na
//		na = n
//		va = v
//		aset.reset
//		else if n < na
//		return aset.add(a)
//		if aset is a (majority) quorum
//		decision = va
	}

	// -----------------------------IncorrectProtocolLogic--------------------------------//

	// Upon receiving the channelId from the membership, register our own callbacks
	// and serializers
	// --!--!--!--!-- DÁ PARA APROVEITAR MENOS O BroadcastMessage --!--!--!--!-
	private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
		int cId = notification.getChannelId();
		myself = notification.getMyself();
		logger.info("Channel {} created, I am {}", cId, myself);
		// Allows this protocol to receive events from this channel.
		registerSharedChannel(cId);
		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);
		/*---------------------- Register Message Handlers -------------------------- */
		try {
			registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
		} catch (HandlerRegistrationException e) {
			throw new AssertionError("Error registering message handler.", e);
		}

	}

	private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
		if (joinedInstance >= 0) {
			// Obviously your agreement protocols will not decide things as soon as you
			// receive the first message
			triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
		} else {
			// We have not yet received a JoinedNotification, but we are already receiving
			// messages from the other
			// agreement instances, maybe we should do something with them...?
		}
	}

	private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
		// We joined the system and can now start doing things
		joinedInstance = notification.getJoinInstance();
		membership = new LinkedList<>(notification.getMembership());
		logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
	}

	private void uponProposeRequest(ProposeRequest request, short sourceProto) {
		logger.debug("Received " + request);
		BroadcastMessage msg = new BroadcastMessage(request.getInstance(), request.getOpId(), request.getOperation());
		logger.debug("Sending to: " + membership);
		membership.forEach(h -> sendMessage(msg, h));
	}

	private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
		logger.debug("Received " + request);
		// The AddReplicaRequest contains an "instance" field, which we ignore in this
		// incorrect protocol.
		// You should probably take it into account while doing whatever you do here.
		membership.add(request.getReplica());
	}

	private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
		logger.debug("Received " + request);
		// The RemoveReplicaRequest contains an "instance" field, which we ignore in
		// this incorrect protocol.
		// You should probably take it into account while doing whatever you do here.
		membership.remove(request.getReplica());
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		// If a message fails to be sent, for whatever reason, log the message and the
		// reason
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

}
