package hu.akarnokd.reactiverpc;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.Test;
import org.reactivestreams.Publisher;

import rsc.flow.Cancellation;
import rsc.publisher.Px;

public class BasicPingPongTest {

    interface PingPongClientAPI {
        @RsRpc
        Publisher<Integer> pong(Publisher<Integer> ping);
        
        @RsRpc
        void send(Publisher<Integer> values);
        
        @RsRpc
        Publisher<Integer> receive();
        
        @RsRpc
        void umap(Function<Publisher<Integer>, Publisher<Integer>> mapper);
    }
    
    static class PingPongServerAPI {
        
        @RsRpc
        public Publisher<Integer> pong(RpcStreamContext<Void> ctx, Publisher<Integer> ping) {
            return Px.wrap(ping).map(v -> v + 1);
        }
        
        @RsRpc
        public void send(RpcStreamContext<Void> ctx, Publisher<Integer> values) {
            Px.wrap(values).subscribe(v -> {
                System.out.println("Server: " + v);
            }, Throwable::printStackTrace);
        }
        
        @RsRpc
        public Publisher<Integer> receive(RpcStreamContext<Void> ctx) {
            return Px.just(100);
        }
        
        @RsRpc
        public Publisher<Integer> umap(RpcStreamContext<Void> ctx, Publisher<Integer> values) {
            Px.wrap(values).subscribe(v -> {
                System.out.println("Server: " + v);
            }, Throwable::printStackTrace);
            
            return Px.just(50);
        }
    }
    
    static void print(Publisher<?> p) {
        System.out.println(Px.wrap(p).blockingLast());
    }
    
    @Test
    public void pingPong() throws Exception {
        
        RpcServer<Void> server = RpcServer.createLocal(new PingPongServerAPI());
        RpcClient<PingPongClientAPI> client = RpcClient.createRemote(PingPongClientAPI.class);
        
        AtomicReference<Cancellation> cancel = new AtomicReference<>();
        
        try (AutoCloseable c = server.start(12345)) {
            
            PingPongClientAPI api = client.connect(InetAddress.getLocalHost(), 12345, cancel::set);

            System.out.println("Map:");
            print(api.pong(Px.just(1)));
            
            System.out.println("Send:");
            api.send(Px.just(20));
            
            Thread.sleep(200);
            
            System.out.println("Receive:");
            print(api.receive());
            
            System.out.println("Umap:");
            api.umap(o -> Px.wrap(o).map(v -> -v));
            
            Thread.sleep(1000);
            
            cancel.get().dispose();
        }
        
    }
}
