package protocols.paxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class TimerLeaderAlive extends ProtoTimer {
    public static final short TIMER_ID = 302;
    private final int timerID;
    private final int instance;


    public TimerLeaderAlive(int timerID, int instance) {
        super(TIMER_ID);
        this.timerID = timerID;
        this.instance = instance;
    }


    @Override
    public ProtoTimer clone() {
        return this;
    }

    public int getTimerId() {
        return this.timerID;
    }

    public int getInstance() {
        return this.instance;
    }

}

