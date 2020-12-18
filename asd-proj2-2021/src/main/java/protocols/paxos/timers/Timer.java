package protocols.paxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class Timer extends ProtoTimer {
    public static final short TIMER_ID = 300;
    private final int timerID;



    public Timer(int timerID) {
        super(TIMER_ID);
        this.timerID = timerID;

    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

    public int getTimerId(){
        return this.timerID;
    }

}
