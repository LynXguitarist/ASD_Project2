package protocols.paxos;

import javafx.util.Pair;
import protocols.statemachine.StateMachine;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PaxosState {
    int instance;
    private int nrPrepareOk;
    private int nrAcceptOk;

    private UUID newValue;

    private int hal;
    private int highestPrepare; // highest prepare
    private int na; // self prepare
    private UUID va; // value
    private UUID decision; // self decision
    private Set<Pair<Integer, UUID>> aset; // map that learners have of accepted values
    private int sequenceNumber;

    public PaxosState(int instance) {
        this.instance=instance;
        aset = new HashSet<>();
        highestPrepare = -1;
        na = -1;
        va = null;
        decision = null;
        hal = -1;
        newValue = null;
        nrPrepareOk = 0;
        nrAcceptOk = 0;
        sequenceNumber = StateMachine.REPLICA_ID;
    }

    public int getSequenceNumber(){ return sequenceNumber; }

    public void updateSeqNumber(int numberSeq) {
        this.sequenceNumber = numberSeq;
    }
}
