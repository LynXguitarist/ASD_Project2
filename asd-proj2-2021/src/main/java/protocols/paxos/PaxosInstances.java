package protocols.paxos;

import java.util.HashMap;
import java.util.Map;

public class PaxosInstances {

	private Map<Integer, Paxos> paxosInstances;

	public PaxosInstances() {
		paxosInstances = new HashMap<>();
	}

	// Paxos - substituir por inputs e gerar o objecto???
	public void addInstance(Paxos paxos) {
		paxosInstances.put(paxosInstances.size(), paxos);
	}

	public Paxos getInstancesState(int instanceNumber) {
		return paxosInstances.get(instanceNumber);
	}
}
