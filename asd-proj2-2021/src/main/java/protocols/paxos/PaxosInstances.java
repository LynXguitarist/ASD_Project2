package protocols.paxos;

import java.util.HashMap;
import java.util.Map;

public class PaxosInstances {

	/** Map with key number of instance and value state of replica */
	private Map<Integer,PaxosState> paxosInstances;
	private static PaxosInstances instance;

	public PaxosInstances() {
		paxosInstances = new HashMap<>();
	}

	public static synchronized PaxosInstances getInstance() {
		if (instance != null)
			return instance;

		instance = new PaxosInstances();
		return instance;
	}

	public void addInstance(int instanceNumber, PaxosState state) { paxosInstances.put(instanceNumber, state);
	}

	public void removeInstance(int instanceNumber) {
		paxosInstances.remove(instanceNumber);
	}

	public PaxosState getPaxosInstance(int instanceNumber) {
		return paxosInstances.get(instanceNumber);
	}

}
