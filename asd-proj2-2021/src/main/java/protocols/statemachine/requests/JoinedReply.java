package protocols.statemachine.requests;

import java.util.List;

import org.apache.commons.codec.binary.Hex;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinedReply extends ProtoReply {

	public static final short REQUEST_ID = 202;

	private int instance;
	private List<Host> membership;
	private byte[] state;

	public JoinedReply(int instance, List<Host> membership, byte[] state) {
		super(REQUEST_ID);
		this.instance = instance;
		this.membership = membership;
		this.state = state;
	}

	public int getInstance() {
		return instance;
	}

	public List<Host> getMembership() {
		return membership;
	}

	public byte[] getState() {
		return state;
	}

	@Override
	public String toString() {
		return "JoinedReply{" + "instance=" + instance + ", membership=" + membership + ", state="
				+ Hex.encodeHexString(state) + '}';
	}

}
