package protocols.paxos.timers;

import protocols.paxos.requests.ProposeRequest;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class Timer extends ProtoTimer {
    public static final short TIMER_ID = 300;
    private final int timerID;
    private final ProposeRequest request;
    private final short sourceProto;




    public Timer(int timerID, ProposeRequest request, short sourceProto){
        super(TIMER_ID);
        this.timerID = timerID;
        this.request = request;
        this.sourceProto = sourceProto;

    }


    @Override
    public ProtoTimer clone() {
        return this;
    }

    public int getTimerId(){
        return this.timerID;
    }

    public short getSourceProto(){
        return sourceProto;
    }

    public ProposeRequest getRequest() {
        return request;
    }
}
