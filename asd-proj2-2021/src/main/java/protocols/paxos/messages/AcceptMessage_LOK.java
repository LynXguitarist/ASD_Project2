package protocols.paxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class AcceptMessage_LOK extends ProtoMessage {

    public final static short MSG_ID = 105;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final int seqNumber;
    private final UUID proposeValue;

    public AcceptMessage_LOK(int instance, UUID opId, byte[] op, int seqNumber, UUID proposeValue) {
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

    public UUID getProposeValue() { return proposeValue; }


    @Override
    public String toString() {
        return "AcceptMessage_LOK{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                ", seqNumber=" + seqNumber +
                ", proposeValue=" + proposeValue +
                '}';
    }

    public static ISerializer<AcceptMessage_LOK> serializer = new ISerializer<AcceptMessage_LOK>() {
        @Override
        public void serialize(AcceptMessage_LOK msg, ByteBuf out) {
            out.writeInt(msg.seqNumber);
            out.writeLong(msg.proposeValue.getMostSignificantBits());
            out.writeLong(msg.proposeValue.getLeastSignificantBits());
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public AcceptMessage_LOK deserialize(ByteBuf in) {
            int seqNumber = in.readInt();
            long highProposeValue = in.readLong();
            long lowProposeValue = in.readLong();
            UUID proposalValue = new UUID(highProposeValue, lowProposeValue);
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new AcceptMessage_LOK(instance, opId, op, seqNumber, proposalValue);
        }
    };

}
