package protocols.statemachine;

import protocols.paxos.notifications.DecidedNotification;
import protocols.paxos.notifications.JoinedNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.Operation;
import utils.StateMachineOperation;
import utils.Utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.app.HashApp;
import protocols.app.messages.ResponseMessage;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.paxos.Paxos;
import protocols.paxos.requests.AddReplicaRequest;
import protocols.paxos.requests.ProposeRequest;
import protocols.paxos.requests.RemoveReplicaRequest;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.JoinedReply;
import protocols.statemachine.requests.JoinedRequest;
import protocols.statemachine.requests.OrderRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;

public class StateMachine extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(StateMachine.class);

	// Replica id
	public static int REPLICA_ID;

	private enum State {
		JOINING, ACTIVE
	}

	// Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "StateMachine";
	public static final short PROTOCOL_ID = 200;

	private final Host self; // My own address/port
	private final int channelId; // Id of the created channel

	private State state;
	private Map<Integer, Integer> operationSequence; // sequence of operations <Instance, Sqn>
	private Map<Host, Short> numRetries;
	private List<Host> membership;
	private Queue<OrderRequest> pendingRequests; // maybe order by operationId with SortedMap

	private int nextInstance;

	public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		nextInstance = 0;

		String address = props.getProperty("address");
		String port = props.getProperty("p2p_port");

		logger.info("Listening on {}:{}", address, port);
		this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));
		this.pendingRequests = new LinkedList<>();
		this.operationSequence = new HashMap<>();
		this.numRetries = new HashMap<>();

		REPLICA_ID = self.getPort(); // port number will be the replica ID

		Properties channelProps = new Properties();
		channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
		channelProps.setProperty(TCPChannel.PORT_KEY, port); // The port to bind to
		channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
		channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
		channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
		channelId = createChannel(TCPChannel.NAME, channelProps);

		/*-------------------- Register Channel Events ------------------------------- */
		registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
		registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
		registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
		registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
		registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);
		registerRequestHandler(JoinedRequest.REQUEST_ID, this::uponJoinedRequest);

		/*--------------------- Register Reply Handlers ----------------------------- */
		registerReplyHandler(CurrentStateReply.REQUEST_ID, this::uponCurrentStateReply);
		registerReplyHandler(JoinedReply.REQUEST_ID, this::uponJoinedReply);

		/*-------------------- Register Message Handlers -------------------------- */
		registerMessageHandler(channelId, ResponseMessage.MSG_ID, null, this::uponMsgFail);

		/*--------------------- Register Notification Handlers ----------------------------- */
		subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);
	}

	@Override
	public void init(Properties props) {
		// Inform the state machine protocol about the channel we created in the
		// constructor
		triggerNotification(new ChannelReadyNotification(channelId, self));

		String host = props.getProperty("initial_membership");
		String[] hosts = host.split(",");
		List<Host> initialMembership = new LinkedList<>();
		for (String s : hosts) {
			String[] hostElements = s.split(":");
			Host h;
			try {
				h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
			} catch (UnknownHostException e) {
				throw new AssertionError("Error parsing initial_membership", e);
			}
			initialMembership.add(h);
		}

		if (initialMembership.contains(self)) {
			state = State.ACTIVE;
			logger.debug("Starting in ACTIVE as I am part of initial membership");
			// I'm part of the initial membership, so I'm assuming the system is
			// Bootstrapping
			membership = new LinkedList<>(initialMembership);
			membership.forEach(this::openConnection);
			triggerNotification(new JoinedNotification(membership, 0));
		} else {

			state = State.JOINING;
			logger.debug("Starting in JOINING as I am not part of initial membership");

			Collections.shuffle(initialMembership);
			Host connectedHost = initialMembership.get(0);
			openConnection(connectedHost);

			sendRequest(new JoinedRequest(self), StateMachine.PROTOCOL_ID);
		}

	}

	/*--------------------------------- Requests ---------------------------------------- */
	private void uponOrderRequest(OrderRequest request, short sourceProto) {
		logger.debug("Received request: " + request);
		if (state == State.JOINING) {
			pendingRequests.add(request);
		} else if (state == State.ACTIVE) {

			byte[] operation = null;
			operation = Utils.joinByteArray(request.getOperation(), 'a');

			pendingRequests.add(new OrderRequest(request.getOpId(), operation));

			// only this request in queue
			if (pendingRequests.size() == 1) {
				OrderRequest orderRequest = pendingRequests.poll();
				sendRequest(new ProposeRequest(nextInstance, orderRequest.getOpId(), operation), Paxos.PROTOCOL_ID);
			}
		}
	}

	private void uponJoinedRequest(JoinedRequest request, short sourceProto) {
		// A replica requested to join the system
		// Propose a request as a StateMachine operation
		logger.debug("Request to join by: " + request.getReplica());
		try {

			byte[] tmp = Utils.convertToBytes(new StateMachineOperation(request.getReplica(), self));
			byte[] operation = Utils.joinByteArray(tmp, 's');

			pendingRequests.add(new OrderRequest(UUID.randomUUID(), operation));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void uponJoinedReply(JoinedReply reply, short sourceProto) {
		// This replica joined the system
		// The replica that replied sent the instance, state and membership
		logger.debug("Joined in instance: " + reply.getInstance());

		membership = new LinkedList<>(reply.getMembership());
		membership.forEach(this::openConnection);
		nextInstance = reply.getInstance();

		sendRequest(new InstallStateRequest(reply.getState()), HashApp.PROTO_ID);
		// Notifies Agreement Protocol that this replica joined the system
		triggerNotification(new JoinedNotification(membership, reply.getInstance()));
	}

	private void uponCurrentStateReply(CurrentStateReply reply, short sourceProto) {
		// Receives the reply from the Application
		// Sends reply to the replica that requested to Join the system(JoinedReply)
		logger.debug("Got the state of the system in instance: " + reply.getInstance());

		sendReply(new JoinedReply(nextInstance, membership, reply.getState()), StateMachine.PROTOCOL_ID);
	}

	/*--------------------------------- Notifications ---------------------------------------- */
	private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
		logger.debug("Received notification: " + notification);

		Operation op = Utils.splitByteArray(notification.getOperation());
		char c = op.getC();
		byte[] operation = op.getOperation();

		if (state != State.ACTIVE) { // if it is not ACTIVE, ignores
			return;
		} else if (c != 's') { // it's an application operation
			logger.debug("Application operation");
			triggerNotification(new ExecuteNotification(notification.getOpId(), operation));
			nextInstance++;
		} else if (nextInstance < notification.getInstance()) { // state machine operation
			// only executes the operations if it's instance > nextInstance
			// get currentState first
			logger.debug("StateMachine operation");
			try {
				StateMachineOperation st = (StateMachineOperation) Utils.convertFromBytes(operation);

				Host host = st.getToAdd();
				if (st.getToAdd() == self || st.getToAdd().equals(self)) {
					membership.forEach(h -> sendRequest(new AddReplicaRequest(nextInstance, host), Paxos.PROTOCOL_ID));

					membership.add(host);
					sendRequest(new CurrentStateRequest(nextInstance), HashApp.PROTO_ID);
				}
				nextInstance++;
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		OrderRequest orderRequest = pendingRequests.poll();
		if (orderRequest != null)
			sendRequest(new ProposeRequest(nextInstance, orderRequest.getOpId(), operation), Paxos.PROTOCOL_ID);
	}

	/*--------------------------------- Multi-Paxos ---------------------------------------- */
	private void uponChangeLeader(DecidedNotification notification, short sourceProto) {

	}

	/*--------------------------------- Messages ---------------------------------------- */
	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		// If a message fails to be sent, for whatever reason, log the message and the
		// reason
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*
	 * --------------------------------- TCPChannel Events
	 * ----------------------------
	 */
	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
		logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());

		short retries = 0;
		if (numRetries.containsKey(event.getNode()))
			retries = numRetries.get(event.getNode());

		if (retries < 3 && membership.contains(event.getNode())) {
			numRetries.put(event.getNode(), retries++);
			openConnection(event.getNode());
			logger.info("Connection to {} restored!", event.getNode());

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			boolean wasRemoved = membership.remove(event.getNode());
			if (wasRemoved) // only sends request if is still in membership
				sendRequest(new RemoveReplicaRequest(nextInstance, event.getNode()), Paxos.PROTOCOL_ID);
		}
	}

	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		logger.debug("Connection to {} is up", event.getNode());
	}

	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
	}

	private void uponInConnectionUp(InConnectionUp event, int channelId) {
		logger.trace("Connection from {} is up", event.getNode());
	}

	private void uponInConnectionDown(InConnectionDown event, int channelId) {
		logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
	}

}
