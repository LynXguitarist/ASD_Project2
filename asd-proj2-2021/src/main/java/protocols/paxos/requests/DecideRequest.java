package protocols.paxos.requests;

import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class DecideRequest extends ProtoRequest {

    public static final short REQUEST_ID = 102;

    private final int instance;
    private final UUID opId;
    private final byte[] operation;
    private final UUID proposeValue;


    public DecideRequest(int instance, UUID opId, byte[] operation, UUID proposeValue) {
        super(REQUEST_ID);
        this.instance = instance;
        this.opId = opId;
        this.operation = operation;
        this.proposeValue = proposeValue;
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

    public UUID getProposeValue(){ return proposeValue; }

    @Override
    public String toString() {
        return "DecideRequest{" +
                "instance=" + instance +
                ", opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                ", proposeValue=" + proposeValue +
                '}';
    }
}
