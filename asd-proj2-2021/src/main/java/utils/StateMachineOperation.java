package utils;

import pt.unl.fct.di.novasys.network.data.Host;

public class StateMachineOperation {

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
