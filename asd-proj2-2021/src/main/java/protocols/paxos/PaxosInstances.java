package protocols.paxos;

import java.util.HashMap;
import java.util.Map;

public class PaxosInstances {

	private Map<Integer, Paxos> paxosInstances;
	private int instanceNumber;
	

	private static PaxosInstances instance;

	public PaxosInstances() {
		paxosInstances = new HashMap<>();

		instanceNumber = 0;
	}

	public static synchronized PaxosInstances getInstance() {
		if (instance != null)
			return instance;

		instance = new PaxosInstances();
		return instance;
	}

	public void addInstance(Paxos paxos) {
		paxosInstances.put(instanceNumber++, paxos);
	}

	public Paxos getPaxosInstance(int instanceNumber) {
		return paxosInstances.get(instanceNumber);
	}

}
