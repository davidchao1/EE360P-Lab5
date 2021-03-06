package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    public boolean accepted;
    public boolean majority = false;
    public int proposal;
    public Object value = new Object();
    boolean prepare;
    // your data here
    public int seq;
    public Response(boolean a,int p,Object v,boolean messageType){
        this.accepted = a;
        this.proposal = p;
        this.value = v;
        this.prepare = messageType;
    }
    public Response(){}


    // Your constructor and methods here
}
