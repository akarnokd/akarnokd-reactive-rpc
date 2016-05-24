package hu.akarnokd.reactiverpc;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.reactivestreams.Publisher;

import rsc.flow.Cancellation;
import rsc.publisher.Px;

public class BasicPingPongTest {

    interface PingPongClientAPI {
        @RsRpc
        Publisher<Integer> pong(Publisher<Integer> ping);
    }
    
    static class PingPongServerAPI {
        
        @RsRpc
        public Publisher<Integer> pong(RpcStreamContext<Void> ctx, Publisher<Integer> ping) {
            return Px.wrap(ping).map(v -> v + 1);
        }
    }
    
    @Test
    public void pingPong() throws Exception {
        
        RpcServer<Void> server = RpcServer.createLocal(new PingPongServerAPI());
        RpcClient<PingPongClientAPI> client = RpcClient.createRemote(PingPongClientAPI.class);
        
        AtomicReference<Cancellation> cancel = new AtomicReference<>();
        
        try (AutoCloseable c = server.start(12345)) {
            
            PingPongClientAPI api = client.connect(InetAddress.getLocalHost(), 12345, cancel::set);
            
            System.out.println(Px.wrap(api.pong(Px.just(1))).blockingFirst());
            
            cancel.get().dispose();
        }
        
    }
}
