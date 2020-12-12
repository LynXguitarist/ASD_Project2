package protocols.paxos;

import java.io.IOException;
import java.util.*;

import javafx.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.notifications.DecidedNotification;


import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.paxos.messages.*;
import protocols.paxos.requests.DecideRequest;
import protocols.paxos.requests.ProposeRequest;
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
    private int MEMBERSHIP_SIZE;

    private Map<Integer, UUID> proposals; // <proposal_sn, value>

    private int nrPrepareOk = 0;
    private int nrAcceptOk = 0;

    private UUID newValue = null;

    private int hal;
    private int highestPrepare; // highest prepare
    private int na; // self prepare
    private UUID va; // value
    private UUID decision; // self decision
    private Set<Pair<Integer, UUID>> aset; // map that learners have of accepted values

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
    public void init(Properties props) {
        //Nothing to do here, we just wait for events from the application or agreement
    }

    Comparator<Integer> keyComparator = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return Integer.compare(o1, o2);
        }
    };

    // ---------------------------------------Paxos_Acceptor----------------------------//
    private void prepare(int seq, UUID value) {
        if (seq > highestPrepare) {
            highestPrepare = seq;
            newValue = value;
        } else {
            int maxKey = Collections.max(proposals.keySet());
            newValue = proposals.get(maxKey);
        }
        // reply <PREPARE_OK,na,va>
    }

    private void accept(int seq, UUID value) {
        if (seq >= highestPrepare) {
            na = seq;
            va = value;
            highestPrepare = seq;
            newValue = value;
            // reply with <ACCEPT_OK,n>
            // send <ACCEPT_OK,na,va > to all learners
        }
    }

    // ---------------------------------------Paxos_Leaners----------------------------//

    // receive message ACCEPT_OK from acceptor a
    private void accepted(int n, UUID v) {
        if (n > na) {
            na = n;
            va = v;
            // asset.reset
        } else if (n < na) {
            return;
        }
        // if asset is a (majority) quorum
        decision = va;
    }

    // -----------------------------IncorrectProtocolLogic--------------------------------//

    // Upon receiving the channelId from the membership, register our own callbacks
    // and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareMessage_OK.MSG_ID, PrepareMessage_OK.serializer);
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptMessage_OK.MSG_ID, AcceptMessage_OK.serializer);
        registerMessageSerializer(cId, AcceptMessage_LOK.MSG_ID, AcceptMessage_LOK.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareMessage_OK.MSG_ID, this::uponPrepareMessage_OK, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage_OK.MSG_ID, this::uponAcceptMessage_OK, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage_LOK.MSG_ID, this::uponAcceptMessage_LOK, this::uponMsgFail);

        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }
/*
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
*/
    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        // We joined the system and can now start doing things
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        MEMBERSHIP_SIZE = membership.size();
        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
    }

    // ---------------------------------------Paxos_Proposer----------------------------//

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Received " + request);
        logger.debug("Sending to: " + membership);
        PaxosState ps = PaxosInstances.getInstance().getPaxosInstance(request.getInstance());
        installState(ps);

        UUID proposeValue = null;
        while (true) {
            //Aqui mudar pois mapa estará na instance
           // int maxKey = Collections.max(proposals.keySet());

            int numberSeq = ps.getSequenceNumber() + MEMBERSHIP_SIZE;

            proposals.put(numberSeq, request.getOpId());
            PrepareMessage msgPrepare = new PrepareMessage(request.getInstance(), request.getOpId(),
                    request.getOperation(), numberSeq, proposeValue );
            membership.forEach(h -> sendMessage(msgPrepare, h));

            long startTimePrepare = System.currentTimeMillis(); // fetch starting time
            while (nrPrepareOk < (membership.size() / 2) || (System.currentTimeMillis() - startTimePrepare) < 10000) {
            }
            if (nrPrepareOk >= (membership.size() / 2)) {
                if (newValue != null) {
                    proposeValue = newValue;
                } else {
                    proposeValue = request.getOpId();
                }
                AcceptMessage msgAccept = new AcceptMessage(request.getInstance(), request.getOpId(),
                        request.getOperation(), numberSeq, proposeValue);
                membership.forEach(h -> sendMessage(msgAccept, h));
                long startTimeAccept = System.currentTimeMillis(); // fetch starting time
                while (nrAcceptOk < (membership.size() / 2) || (System.currentTimeMillis() - startTimeAccept) < 10000) {
                }
                if (nrAcceptOk >= (membership.size() / 2)) {
                    DecideRequest decide = new DecideRequest(request.getInstance(), request.getOpId(),
                            request.getOperation(), numberSeq);
                    //enviar para o cliente
                    break;
                } else {
                    nrAcceptOk = 0;
                }
            } else {
                nrPrepareOk = 0;
            }
        }
    }

    private void installState(PaxosState ps) {

        nrPrepareOk = ps.getPrepareOk();
         nrAcceptOk =  ps.getNrAcceptOk();

        UUID newValue = ps.get

        hal;
        highestPrepare;
         na;
        UUID va;
        UUID decision;
        Set<Pair<Integer, UUID>> //fazer igual
    }

    private void uponPrepareMessage(PrepareMessage prepareMessage, Host host, short i, int i1) {
        //vale a pena estar separado?
        prepare(prepareMessage.getSeqNumber(), prepareMessage.getProposeValue());
        PrepareMessage_OK msgPrepare_OK = new PrepareMessage_OK(prepareMessage.getInstance(), prepareMessage.getOpId(), prepareMessage.getOp(), highestPrepare, newValue);
        //host é quem me enviou?
        sendMessage(msgPrepare_OK, host);
    }

    private void uponPrepareMessage_OK(PrepareMessage_OK prepareMessage_OK, Host host, short i, int i1) {
        nrPrepareOk++;
    }

    /**
     * Learner receive this message
     */
    private void uponAcceptMessage_LOK(AcceptMessage_OK msg, Host host, short sourceProto, int channelId) {
        UUID v = msg.getProposeValue();
        int n = msg.getSeqNumber();

        if (n > hal) {
            hal = n;
            va = v;
            aset.clear();
        } else if (n == hal) {
            aset.add(new Pair(n, v));
        }
        if (aset.size() > (membership.size() / 2)) {
            decision = va;
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
        }
    }

    /**
     * Proposer receive this message
     */
    private void uponAcceptMessage_OK(AcceptMessage_OK msg, Host host, short sourceProto, int channelId) {
        UUID v = msg.getProposeValue();
        int n = msg.getSeqNumber();

        if (n > hal) {
            hal = n;
            va = v;
            aset.clear();
        } else if (n == hal) {
            aset.add(new Pair(n, v));
        }
        if (aset.size() > (membership.size() / 2)) {
            decision = va;
            //trigger decide

        }
    }

    private void uponAcceptMessage(AcceptMessage acceptMessage, Host host, short i, int i1) {

    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        // The AddReplicaRequest contains an "instance" field, which we ignore in this
        // incorrect protocol.
        // You should probably take it into account while doing whatever you do here.

        membership.add(request.getReplica());
        //PaxosState ps = PaxosInstances.getInstance().getPaxosInstance(request.getInstance());
        //installState(ps);
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        // The RemoveReplicaRequest contains an "instance" field, which we ignore in
        // this incorrect protocol.
        // You should probably take it into account while doing whatever you do here.
        //PaxosInstances.getInstance().removeInstance(request.getInstance());

        membership.remove(request.getReplica());

        MEMBERSHIP_SIZE = membership.size();
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        // If a message fails to be sent, for whatever reason, log the message and the
        // reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
