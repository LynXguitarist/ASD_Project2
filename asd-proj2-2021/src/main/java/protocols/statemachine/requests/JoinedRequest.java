package protocols.statemachine.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinedRequest extends ProtoRequest {

	public static final short REQUEST_ID = 203;

	private Host replica;

	public JoinedRequest(Host replica) {
		super(REQUEST_ID);
		this.replica = replica;
	}

	public Host getReplica() {
		return replica;
	}

	@Override
	public String toString() {
		return "OrderRequest{" + "replica=" + replica + '}';
	}
}
