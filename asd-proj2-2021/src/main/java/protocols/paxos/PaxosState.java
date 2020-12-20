package protocols.paxos;

import javafx.util.Pair;
import protocols.statemachine.StateMachine;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class PaxosState {

    int instance;
    private int sequenceNumber;
    private UUID proposeValue;
    private UUID prepareValue;
    private UUID acceptValue;
    private int acceptSeq;
    private int highestPrepare;
    private int highestAccept;
    private int nrPrepareOk;
    private int nrAcceptOk;
    private UUID decision; // self decision
    private boolean isProposer;
    private Set<Pair<Integer, UUID>> aset; // map that learners have of accepted values
    //For multi-paxos
    private int leaderId;
    private Host leader;
    private Queue<Pair<UUID, byte[]>> pendingOps;


    public PaxosState(int instance) {
        this.instance = instance;
        proposeValue = null;
        prepareValue = null;
        acceptSeq = -1;
        sequenceNumber = StateMachine.REPLICA_ID;
        highestPrepare = -1;
        highestAccept = -1;
        nrPrepareOk = 0;
        nrAcceptOk = 0;
        acceptValue = null;
        decision = null;
        isProposer = false;
        aset = new HashSet<>();
        //For multi-paxos
        leaderId = StateMachine.REPLICA_ID;
        leader = null;
        pendingOps = new LinkedList<>();


    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void updateSeqNumber(int numberSeq) {
        this.sequenceNumber = numberSeq;
    }

    public void updateProposeValue(UUID proposeValue) {
        this.proposeValue = proposeValue;
    }

    public int getHighestPrepare() {
        return highestPrepare;
    }

    public void setHighestPrepare(int highestPrepare) {
        this.highestPrepare = highestPrepare;
    }

    public UUID getPrepareValue() {
        return prepareValue;
    }

    public int getNrPrepareOK() {
        return nrPrepareOk;
    }

    public void updateNrPrepareOK(int nrPrepareOK) {
        this.nrPrepareOk = nrPrepareOK;
    }

    public void setAcceptValue(UUID acceptValue) {
        this.acceptValue = acceptValue;
    }

    public void setAcceptSeq(int acceptSeq) {
        this.acceptSeq = acceptSeq;
    }

    public int getNrAcceptOK() {
        return nrAcceptOk;
    }

    public void updateNrAcceptOK(int nrAcceptOK) {
        this.nrAcceptOk = nrAcceptOK;
    }

    public void setPrepareValue(UUID proposeValue) {
        this.proposeValue = proposeValue;
    }

    public void setDecidedValue(UUID proposeValue) {
        this.decision = proposeValue;
    }

    public int getAcceptSeq() {
        return this.acceptSeq;
    }

    public Set<Pair<Integer, UUID>> getAset() {
        return aset;
    }

    public void setAset(Set<Pair<Integer, UUID>> aset) {
        this.aset = aset;
    }

    public boolean isProposer() {
        return isProposer;
    }

    public void setIsProposer() {
        isProposer = true;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void addPendingOp(UUID requestId, byte[] request) {
        pendingOps.add(new Pair(requestId, request));
    }

    public void setLeader(Host leader) {
        this.leader = leader;
    }

    public Host getLeader() {
        return leader;
    }

    public void setIsProposerFalse() {
        this.isProposer = false;
    }

    public Pair<UUID, byte[]> getOnePendingOp() {
        return pendingOps.poll();
    }

    public Queue<Pair<UUID, byte[]>> getPendingOp() {
        return pendingOps;
    }

    public void addAllPendingOp(Queue<Pair<UUID, byte[]>> pendingOp) {
        pendingOps = pendingOp;
    }
}
