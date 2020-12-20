package protocols.paxos;


import java.io.IOException;
import java.util.*;

import javafx.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import protocols.paxos.messages.*;
import protocols.paxos.notifications.DecidedNotification;
import protocols.paxos.notifications.JoinedNotification;
import protocols.paxos.requests.AddReplicaRequest;
import protocols.paxos.requests.ProposeRequest;
import protocols.paxos.requests.RemoveReplicaRequest;
import protocols.paxos.timers.TimerLeaderAlive;
import protocols.paxos.timers.TimerNoOp;
import protocols.statemachine.StateMachine;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

import protocols.paxos.timers.Timer;

/*
 * Proposer sends proposal to acceptor
 * proposal is selected when majority of acceptors accept it (f < N/2)
 * Sequence Number(psn) = instanceNumber
 * A proposed value that was accepted by a majority of acceptors is said to be locked in
 */
public class MultiPaxos extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Paxos.class);

    // Protocol information, to register in Babel
    public final static short PROTOCOL_ID = 104;
    public final static String PROTOCOL_NAME = "MultiPaxos";

    private Host myself;
    private int joinedInstance;
    private List<Host> membership;
    private int MEMBERSHIP_SIZE;

    private Map<Integer, UUID> proposals; // <proposal_sn, value>


    private Map<Integer, PaxosState> paxosInstances;

    public MultiPaxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; // -1 means we have not yet joined the system
        membership = null;

        /*--------------------- Register Timer Handlers ----------------------------- */
        try {
            registerTimerHandler(Timer.TIMER_ID, this::uponTimer);
        } catch (HandlerRegistrationException e) {
            e.printStackTrace();
        }

        try {
            registerTimerHandler(TimerNoOp.TIMER_ID, this::uponTimerNoOp);
        } catch (HandlerRegistrationException e) {
            e.printStackTrace();
        }

        try {
            registerTimerHandler(TimerLeaderAlive.TIMER_ID, this::uponTimerLeaderAlive);
        } catch (HandlerRegistrationException e) {
            e.printStackTrace();
        }

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    private void uponTimerLeaderAlive(TimerLeaderAlive timer, long timerId) {
        //prepare om nr seq mais alto
        //TODO
        PaxosState paxosState = paxosInstances.get(timer.getInstance());
        int numberSeq = paxosState.getSequenceNumber() + MEMBERSHIP_SIZE;
        paxosState.updateSeqNumber(numberSeq);
        Pair<UUID, byte[]> pendingOp = paxosState.getOnePendingOp();

        //ACHO QUE NAO É NECESSARIO
        //setupTimer(new Timer(numberSeq, request, sourceProto), 10000);

        UUID proposeValue = pendingOp.getKey();
        paxosState.updateProposeValue(proposeValue);
        PrepareMessage msgPrepare = new PrepareMessage(paxosState.instance, pendingOp.getKey(), pendingOp.getValue(), numberSeq, proposeValue);
        membership.forEach(h -> sendMessage(msgPrepare, h));

    }

    //Envia NO OP
    private void uponTimerNoOp(TimerNoOp timer, long timerId) {
        AcceptMessage msgAccept = new AcceptMessage(timer.getInstance(), null,
                null, timer.getTimerId(), null);
        membership.forEach(h -> sendMessage(msgAccept, h));
    }

    private void uponTimer(Timer timer, long timerId) {
        uponProposeRequest(timer.getRequest(), timer.getSourceProto());
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
        registerMessageSerializer(cId, PendingOpMessage.MSG_ID, PendingOpMessage.serializer);


        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareMessage_OK.MSG_ID, this::uponPrepareMessage_OK, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage_OK.MSG_ID, this::uponAcceptMessage_OK, this::uponMsgFail);
            registerMessageHandler(cId, PendingOpMessage.MSG_ID, this::uponPendingOpMessage, this::uponMsgFail);


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

    private void uponPendingOpMessage(PendingOpMessage msg, Host host, short i, int i1) {
        PaxosState paxosState = paxosInstances.get(msg.getInstance());
        paxosState.addPendingOp(msg.getOpId(), msg.getOp());
    }

    // ---------------------------------------Paxos_Proposer----------------------------//

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        //ver se já existe no meu paxos State um lider
        //caso nao haja entao eu faço prepare dizendo que sou o novo lider
        //caso haja entao eu nao posso fazer prepare, tenho de enviar as op cliente que recebo para o lider (será neste metodo, ou só adiciona a uma lista??)
        logger.debug("Received " + request);
        logger.debug("Sending to: " + membership);
        int numberSeq;
        PaxosState paxosState = paxosInstances.get(request.getInstance());

        //No caso de já existir um lider
        Host leader = paxosState.getLeader();
        if (leader != null && !leader.equals(myself)) {
            paxosState.addPendingOp(request.getOpId(), request.getOperation());
            PendingOpMessage msgPendingOp = new PendingOpMessage(request.getInstance(), request.getOpId(), request.getOperation());
            sendMessage(msgPendingOp, paxosState.getLeader());

        } else if (leader.equals(myself)) {
            int seq = paxosState.getSequenceNumber();
            //reset do timer
            cancelTimer(seq);
            setupTimer(new TimerNoOp(seq, request.getInstance()), 10000);

            paxosState.addPendingOp(request.getOpId(), request.getOperation());
            Pair<UUID, byte[]> operarion = paxosState.getOnePendingOp();

            AcceptMessage msgAccept = new AcceptMessage(request.getInstance(), operarion.getKey(),
                    operarion.getValue(), seq, operarion.getKey());
            membership.forEach(h -> sendMessage(msgAccept, h));

        } else {
            paxosState.setIsProposer();

            if (paxosState != null) {
                numberSeq = paxosState.getSequenceNumber() + MEMBERSHIP_SIZE;
                paxosState.updateSeqNumber(numberSeq);

            } else {
                paxosInstances.put(request.getInstance(), new PaxosState(request.getInstance()));
                paxosState = paxosInstances.get(request.getInstance());
                numberSeq = StateMachine.REPLICA_ID;
            }

            //ACHO QUE NAO É NECESSARIO???
            setupTimer(new Timer(numberSeq, request, sourceProto), 10000);

            UUID proposeValue = request.getOpId();
            paxosState.updateProposeValue(proposeValue);
            PrepareMessage msgPrepare = new PrepareMessage(request.getInstance(), request.getOpId(), request.getOperation(), numberSeq, proposeValue);
            membership.forEach(h -> sendMessage(msgPrepare, h));
        }
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short i, int i1) {
        //se replica enviar prepare ok entao significa que esta a concordar com a eleiçao do nobo lider
        //vou ter que ter um timeout algures para lidar como o facto de uma mensagem demorar demasiado tempo e trocar de lider
        int seq = msg.getSeqNumber();
        UUID value = msg.getProposeValue();
        PaxosState paxosState = paxosInstances.get(msg.getInstance());
        int highestPrepare = paxosState.getHighestPrepare();
        if (seq > highestPrepare) {

            //sera que preciso de guardar o id?
            paxosState.setLeader(host);
            //iniciar timmer para ver se o lider falha
            if (!myself.equals(paxosState.getLeader())) {
                setupTimer(new TimerLeaderAlive(StateMachine.REPLICA_ID, msg.getInstance()), 10000);
            }

            highestPrepare = seq;
            paxosState.setHighestPrepare(highestPrepare);
            UUID prepareValue = paxosState.getPrepareValue();
            if (prepareValue != null) {
                value = prepareValue;
            }
            PrepareMessage_OK msgPrepare_OK = new PrepareMessage_OK(msg.getInstance(), msg.getOpId(), msg.getOp(), seq, value);
            sendMessage(msgPrepare_OK, host);
        }
    }

    //TENHO QUE VER SE POSSO INCREMENTAR O NRPREPAREOK  TODO Comparar com o que tenho no meu paxos state
    private void uponPrepareMessage_OK(PrepareMessage_OK msg, Host host, short i, int i1) {

        PaxosState paxosState = paxosInstances.get(msg.getInstance());
        int nrPrepareOK = paxosState.getNrPrepareOK();
        nrPrepareOK++;
        paxosState.updateNrPrepareOK(nrPrepareOK);

        UUID proposeValue = msg.getProposeValue();
        paxosState.updateProposeValue(proposeValue);
        if (nrPrepareOK > (MEMBERSHIP_SIZE / 2)) {
            //Replica passa a ser lider
            paxosState.setLeaderId(StateMachine.REPLICA_ID);
            paxosState.setLeader(myself);

            //Definir timer para enviar no ops
            setupTimer(new TimerNoOp(paxosState.getSequenceNumber(), msg.getInstance()), 10000);

            AcceptMessage msgAccept = new AcceptMessage(msg.getInstance(), msg.getOpId(),
                    msg.getOp(), paxosState.getSequenceNumber(), proposeValue);
            membership.forEach(h -> sendMessage(msgAccept, h));
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short i, int i1) {

        if (msg.getOp() == null) {
            cancelTimer(StateMachine.REPLICA_ID);
            setupTimer(new TimerLeaderAlive(StateMachine.REPLICA_ID, msg.getInstance()), 10000);
        } else {
            UUID value = msg.getProposeValue();
            int seq = msg.getSeqNumber();

            PaxosState paxosState = paxosInstances.get(msg.getInstance());
            int highestPrepare = paxosState.getHighestPrepare();

            cancelTimer(StateMachine.REPLICA_ID);
            setupTimer(new TimerLeaderAlive(StateMachine.REPLICA_ID, msg.getInstance()), 10000);

            if (seq >= highestPrepare) {
                paxosState.setAcceptValue(value);
                paxosState.setAcceptSeq(seq);
                AcceptMessage_OK msgAccept_OK = new AcceptMessage_OK(msg.getInstance(), msg.getOpId(), msg.getOp(), seq, value);
                membership.forEach(h -> sendMessage(msgAccept_OK, h));
            }
        }
    }

    private void uponAcceptMessage_OK(AcceptMessage_OK msg, Host host, short sourceProto, int channelId) {
        UUID value = msg.getProposeValue();
        int seq = msg.getSeqNumber();
        PaxosState paxosState = paxosInstances.get(msg.getInstance());
        if (myself.equals(paxosState.getLeader())) {
            int nrAcceptOK = paxosState.getNrAcceptOK();
            nrAcceptOK++;
            paxosState.updateNrAcceptOK(nrAcceptOK);

            if (nrAcceptOK > (MEMBERSHIP_SIZE / 2)) {
                paxosState.setPrepareValue(value);
                paxosState.setDecidedValue(value);

                //tratar de começar numa nova instancia
                int newInstance = msg.getInstance() + 1;
                paxosInstances.put(newInstance, new PaxosState(newInstance));
                Queue<Pair<UUID, byte[]>> pendingOps = paxosState.getPendingOp();
                PaxosState newPaxosState = paxosInstances.get(newInstance);
                newPaxosState.addAllPendingOp(pendingOps);
                newPaxosState.setLeader(paxosState.getLeader());
                newPaxosState.updateSeqNumber(paxosState.getSequenceNumber());
                Pair<UUID, byte[]> pendingOp = newPaxosState.getOnePendingOp();
                newPaxosState.updateProposeValue(pendingOp.getKey());
                AcceptMessage msgAccept = new AcceptMessage(newInstance, pendingOp.getKey(),
                        pendingOp.getValue(), newPaxosState.getSequenceNumber(),  pendingOp.getKey());
                membership.forEach(h -> sendMessage(msgAccept, h));



                //Copiar o estado de uma instancia para a outa
                triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
            }
        } else {
            int highestAccept = paxosState.getAcceptSeq();
            Set<Pair<Integer, UUID>> aset = paxosState.getAset();
            if (seq > highestAccept) {
                paxosState.setAcceptSeq(seq);
                paxosState.setAcceptValue(value);
                aset.clear();
            } else if (seq < highestAccept) {
                return;
            }
            aset.add(new Pair(seq, value));
            paxosState.setAset(aset);

            if (aset.size() > (MEMBERSHIP_SIZE / 2)) {
                paxosState.setPrepareValue(value);
                paxosState.setDecidedValue(value);
                triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
            }
        }
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
