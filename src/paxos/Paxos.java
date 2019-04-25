package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex = new ReentrantLock();
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]
    AtomicInteger n = new AtomicInteger(1);
    Registry registry;
    PaxosRMI stub;
    int num_paxos;
    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing
    AtomicInteger current = new AtomicInteger(1);
    int sequence;
    Object value;
    Map<Integer, instance> instances = new ConcurrentHashMap<Integer, instance>();
    Map<Integer, Object> vals = new ConcurrentHashMap<Integer, Object>();
    // Your data here
    int[]finished;

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */

    private class instance{
        Object value_accept;
        State state;
        int highPrepare;
        int high_accept;


        int num_prepare;
        int lowProposal;




        public instance(){
            highPrepare = Integer.MIN_VALUE;
            lowProposal = Integer.MAX_VALUE;
            state = State.Pending;
            value_accept = null;
            high_accept = 0;
            num_prepare = 0;
        }
    }
    private instance getInstance(int seq) {
        mutex.lock();
        if(!instances.containsKey(seq)) {
            instance inst = new instance();
            instances.put(seq, inst);
        }
        mutex.unlock();
        return instances.get(seq);

    }
    public Paxos(int me, String[] peers, int[] ports){
//    	try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);
        this.num_paxos= peers.length;
        this.finished = new int[peers.length];
        for(int i = 0 ;i<peers.length;i++){
            finished[i] = -1;
        }
        // Your initialization code here

        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        // Your code here
    	mutex.lock();
        this.sequence = seq;
        this.value = value;
        mutex.unlock();
        vals.put(seq,value);
        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run(){
        //Your code here
        mutex.lock();
        int sequence = this.sequence;
        mutex.unlock();
        if(this.Min()>sequence){
            return;
        }
        Object val =this.vals.get(sequence);
        while(this.getInstance(sequence).state != State.Decided){
            Response proposalResponse = sendPrepare(sequence,val);
            if(proposalResponse.majority){
                Request acceptRequest = new Request(sequence, proposalResponse.proposal, proposalResponse.value);
                if(sendAccept(acceptRequest)){
                    sendDecide(acceptRequest);
                }
            }
        }
    }
    public Response sendPrepare(int seq,Object value){
        instance inst = this.getInstance(seq);
        Object val = value;
        int accepted = 0;
        int numAccepted = inst.highPrepare;

        Request prepareRequest = createprepareRequest(seq,val,inst);
        //int ProposalSum = prepareRequest.proposal;

        for(int i =0;i<this.peers.length;i++) {
            Response prepareResponse;
            if (this.me == i) {
                prepareResponse = this.Prepare(prepareRequest);
                // Proposal to peers
            } else {
                prepareResponse = this.Call("Prepare", prepareRequest, i);
            }
            if(prepareResponse!=null&&prepareResponse.accepted){
                accepted++;
                if(prepareResponse.proposal>numAccepted){
                    numAccepted = prepareResponse.proposal;
                    val = prepareResponse.value;
                }
            }
        }
        Response proposalResponse = new Response();
        if(accepted>=this.num_paxos/2+1){
            proposalResponse.majority = true;
            proposalResponse.proposal = numAccepted;			// used to send accept(n, v')
            proposalResponse.value = val;
        }
        return proposalResponse;

    }

    public Request createprepareRequest(int sequence, Object val, instance inst){
        int proposal = 0;
        if(inst.highPrepare == Integer.MIN_VALUE){
            proposal = this.me+1;
        }else{
            proposal = inst.highPrepare / this.num_paxos+1 * this.num_paxos + this.me+1;
        }
        Request r = new Request(sequence,proposal,val);
        return r;
    }
    public boolean sendAccept(Request accept){
        Object val = accept.value;
        int proposal = accept.proposal;
        int numAccepted = 0;
        for(int i = 0;i<peers.length;i++){
            Response accResp;
            if(i==this.me){
                accResp = this.Accept(accept);
            }else{
                accResp = this.Call("Accept",accept,i);
            }
            if(accResp!=null&&accResp.accepted){
                numAccepted++;
            }
        }
        if(numAccepted>=this.num_paxos/2+1){
            return true;
        }
        return false;
    }
    public void sendDecide(Request decide){
        instance inst = this.instances.get(decide.sequence);		// key should exist
        inst.high_accept = decide.proposal;
        inst.highPrepare = decide.proposal;
        inst.value_accept = decide.value;
        inst.state = State.Decided;
        for(int i =0;i<this.num_paxos;i++){
            Response decideResp;
            if(i!=this.me){
                Request request = new Request();
                request.sequence = decide.sequence;
                request.proposal = decide.proposal;
                request.value = decide.value;
                request.me = this.me;
                request.finished  = this.finished[this.me];
                //decideResp =
                    this.Call("Decide",request,i);

            }

        }
    }
    // RMI handler
    public Response Prepare(Request req){
        // your code here
        instance t = this.getInstance(req.sequence);
        if(req.proposal>t.highPrepare){
            t.highPrepare = req.proposal;
            if(t.value_accept==null){
                t.value_accept = req.value;
            }
            Response prepareResponse = new Response(true,t.highPrepare,t.value_accept,true);
            return prepareResponse;
        }else{
            Response prepareResponse = new Response(false,0,null,true);
            return prepareResponse;
        }
    }

    public Response Accept(Request req){
        instance t = this.getInstance(req.sequence);
        if(req.proposal>=t.highPrepare){
            t.highPrepare = req.proposal;
            t.num_prepare = req.proposal;
            t.value_accept = req.value;
            Response prepareResponse = new Response(true,t.highPrepare,t.value_accept,false);
            return prepareResponse;
        }else{
            Response prepareResponse = new Response(false,0,null,false);
            return prepareResponse;
        }

    }

    public Response Decide(Request req){
        // your code here

        instance inst = this.getInstance(req.sequence);
        inst.highPrepare = req.proposal;
        inst.high_accept = req.proposal;
        inst.value_accept = req.value;
        inst.state = State.Decided;
    //    Done(req.sequence);

        this.finished[req.me] = req.finished;
        return new Response();

    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        if(seq > this.finished[this.me]) {
            this.finished[this.me] = seq;
        }

    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        int max = Integer.MIN_VALUE;
        for(int key : instances.keySet()) {
            if (key > max) {
                max = key;
            }
        }
        return max;
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        int min = this.finished[this.me];
        for(int i = 0;i<finished.length;i++){
            if(finished[i]<min){
                min = finished[i];
            }
        }
        for(int key : instances.keySet()){
            if(key<min&&instances.get(key).state == State.Decided){
                instances.remove(key);
            }

        }
        return min + 1;
    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        if(this.Min()>seq){
            return new retStatus(State.Forgotten,null);
        }
        if(this.instances.containsKey(seq)){
            instance inst = instances.get(seq);
            return new retStatus(inst.state, inst.value_accept);
        }else {
            return new retStatus(State.Pending, null);
        }
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}
