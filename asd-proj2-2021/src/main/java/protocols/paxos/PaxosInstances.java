package protocols.paxos;

import java.util.HashMap;
import java.util.Map;

public class PaxosInstances {

	private Map<Integer, Paxos> paxosInstances;
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

	public void addInstance(int instanceNumber, Paxos paxos) {
		paxosInstances.put(instanceNumber++, paxos);
	}

	public void removeInstance(int instanceNumber) {
		paxosInstances.remove(instanceNumber);
	}

	public Paxos getPaxosInstance(int instanceNumber) {
		return paxosInstances.get(instanceNumber);
	}

}
