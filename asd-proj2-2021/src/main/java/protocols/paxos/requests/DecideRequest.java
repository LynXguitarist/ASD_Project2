package protocols.paxos.requests;

import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class DecideRequest extends ProtoRequest {

    public static final short REQUEST_ID = 102;

    private final int instance;
    private final UUID opId;
    private final byte[] operation;
    private final int seqNumber;


    public DecideRequest(int instance, UUID opId, byte[] operation, int seqNumber) {
        super(REQUEST_ID);
        this.instance = instance;
        this.opId = opId;
        this.operation = operation;
        this.seqNumber = seqNumber;
    }

    public int getInstance() {
        return instance;
    }

    public byte[] getOperation() {
        return operation;
    }

    public UUID getOpId() {
        return opId;
    }

    public int getSeqNumber(){ return seqNumber; }

    @Override
    public String toString() {
        return "DecideRequest{" +
                "instance=" + instance +
                ", opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                ", seqNumber=" + seqNumber +
                '}';
    }
}
