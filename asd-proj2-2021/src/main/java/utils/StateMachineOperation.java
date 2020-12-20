package utils;

import java.io.Serializable;

import pt.unl.fct.di.novasys.network.data.Host;

public class StateMachineOperation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Host toAdd, whoAdds;

	public StateMachineOperation(Host toAdd, Host whoAdds) {
		this.toAdd = toAdd;
		this.whoAdds = whoAdds;
	}

	public Host getToAdd() {
		return toAdd;
	}

	public Host getWhoAdds() {
		return whoAdds;
	}
}
