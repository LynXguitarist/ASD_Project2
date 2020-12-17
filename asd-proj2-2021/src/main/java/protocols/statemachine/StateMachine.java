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
import utils.Utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.glass.ui.Application;

import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.app.HashApp;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

/**
 * This is NOT fully functional StateMachine implementation. This is simply an
 * example of things you can do, and can be used as a starting point.
 *
 * You are free to change/delete anything in this class, including its fields.
 * The only thing that you cannot change are the notifications/requests between
 * the StateMachine and the APPLICATION You can change the requests/notification
 * between the StateMachine and AGREEMENT protocol, however make sure it is
 * coherent with the specification shown in the project description.
 *
 * Do not assume that any logic implemented here is correct, think for yourself!
 */
public class StateMachine extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(StateMachine.class);

	// Replica id
	public static int REPLICA_ID;

	private enum State {
		JOINING, ACTIVE // INACTIVE
	}

	// Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "StateMachine";
	public static final short PROTOCOL_ID = 200;

	private final Host self; // My own address/port
	private final int channelId; // Id of the created channel

	private State state;
	private Map<Integer, Integer> operationSequence; // sequence of operations <Instance, Sqn>
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
			logger.info("Starting in ACTIVE as I am part of initial membership");
			// I'm part of the initial membership, so I'm assuming the system is
			// Bootstrapping
			membership = new LinkedList<>(initialMembership);
			membership.forEach(this::openConnection);
			triggerNotification(new JoinedNotification(membership, 0));
		} else {
			// You have to do something to join the system and know which instance you
			// joined
			// (and copy the state of that instance)

			state = State.JOINING;
			logger.info("Starting in JOINING as I am not part of initial membership");

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
			// Do something smart (like buffering the requests)
			pendingRequests.add(request); // right???
		} else if (state == State.ACTIVE) {
			// Also do something smart, we don't want an infinite number of instances
			// active

			// Maybe you should modify what is it that you are proposing so that you
			// remember that this operation was issued by the application
			// (and not an internal operation, check the uponDecidedNotification)

			// Add a char to the operation, to know if is an application or state
			// operation
			pendingRequests.add(request);
			OrderRequest orderRequest = pendingRequests.poll();
			byte[] operation = null;

			if (orderRequest == null)
				operation = Utils.joinByteArray(request.getOperation(), 't');
			else
				operation = orderRequest.getOperation();

			sendRequest(new ProposeRequest(nextInstance++, orderRequest.getOpId(), operation), Paxos.PROTOCOL_ID);
		}
	}

	private void uponJoinedRequest(JoinedRequest request, short sourceProto) {
		// A replica requested to join the system
		// Propose a request as a StateMachine operation

		byte[] operation = Utils.joinByteArray(request.getReplica(), 'f'); // operation, construct the operation???
		pendingRequests.add(new OrderRequest(UUID.randomUUID(), operation));

		// sendRequest(new ProposeRequest(nextInstance, UUID.randomUUID(), operation),
		// Paxos.PROTOCOL_ID);
	}

	private void uponJoinedReply(JoinedReply reply, short sourceProto) {
		// This replica joined the system
		// The replica that replied sent the instance, state and membership
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
		// State = bytes or Object with the State???
		sendReply(new JoinedReply(nextInstance, membership, reply.getState()), StateMachine.PROTOCOL_ID);
	}

	/*--------------------------------- Notifications ---------------------------------------- */
	private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
		logger.debug("Received notification: " + notification);
		// Maybe we should make sure operations are executed in order?

		// You should be careful and check if this operation is an application operation
		// (and send it up)
		// or if this is an operations that was executed by the state machine itself (in
		// which case you should execute)

		Operation op = Utils.splitByteArray(notification.getOperation());
		char c = op.getC();
		byte[] operation = op.getOperation();

		if (state != State.ACTIVE) { // if it is not ACTIVE, ignores
			return;
		} else if (c == 't') { // it's an application operation
			/*
			 * OrderRequest orderRequest = pendingRequests.poll(); if (orderRequest != null)
			 * operation = orderRequest.getOperation(); else operation = op.getOperation();
			 */
			sendRequest(new ProposeRequest(nextInstance++, notification.getOpId(), operation), Paxos.PROTOCOL_ID);
		} else if (nextInstance < notification.getInstance()) { // state machine operation

			// only executes the operations if it's instance > nextInstance
			// get currentState first

			// How to know what replica to add? Is in the operation?
			// sendRequest(new AddReplicaRequest(nextInstance, ), destination);

			// como ir buscar o host a ser criado
			membership.forEach(host -> sendRequest(new AddReplicaRequest(nextInstance, host), Paxos.PROTOCOL_ID));

			sendRequest(new CurrentStateRequest(nextInstance), HashApp.PROTO_ID);
			triggerNotification(new ExecuteNotification(notification.getOpId(), operation));
		}
	}

	/*--------------------------------- Multi-Paxos ---------------------------------------- */
	private void uponChangeLeader(DecidedNotification notification, short sourceProto) {
		// For Multi-Paxos
		// Change type of notification
		// a partir dos 15 min Multi Paxos Lab 7
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
		while (retries < 5) {
			if (membership.contains(event.getNode())) {
				logger.info("Connection to {} restored!", event.getNode());
				openConnection(event.getNode());
				return;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		sendRequest(new RemoveReplicaRequest(event.getId(), event.getNode()), Paxos.PROTOCOL_ID); // ebent.getId() ???
	}

	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		logger.info("Connection to {} is up", event.getNode());
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
