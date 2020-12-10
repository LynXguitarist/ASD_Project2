package protocols.paxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class AcceptMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final int seqNumber;
    private final int proposeValue;

    public AcceptMessage(int instance, UUID opId, byte[] op, int seqNumber, int proposeValue) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.seqNumber = seqNumber;
        this.proposeValue = proposeValue;

    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }

    public int getSeqNumber(){ return seqNumber; }

    public int getProposeValue() { return proposeValue; }

    @Override
    public String toString() {
        return "AcceptMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                ", seqNumber=" + seqNumber +
                ", proposeValue=" + proposeValue +
                '}';
    }

    public static ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) {
            out.writeInt(msg.seqNumber);
            out.writeInt(msg.proposeValue);
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) {
            int seqNumber = in.readInt();
            int proposeValue = in.readInt();
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new AcceptMessage(instance, opId, op, seqNumber, proposeValue);
        }
    };

}
