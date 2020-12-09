package protocols.paxos.requests;

import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class DecideRequest extends ProtoRequest {

    public static final short REQUEST_ID = 102;

    private final int instance;
    private final UUID opId;
    private final byte[] operation;
    private final int va;

    public DecideRequest(int instance, UUID opId, byte[] operation, int va) {
        super(REQUEST_ID);
        this.instance = instance;
        this.opId = opId;
        this.operation = operation;
        this.va = va;
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

    public int getVa(){ return va; }

    @Override
    public String toString() {
        return "DecideRequest{" +
                "instance=" + instance +
                ", opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                ", va=" + va +
                '}';
    }
}
