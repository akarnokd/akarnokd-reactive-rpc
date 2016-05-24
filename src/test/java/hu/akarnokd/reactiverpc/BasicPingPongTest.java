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
        Integer pong2(Integer ping);
        
        @RsRpc
        Publisher<Integer> pong(Publisher<Integer> ping);
        
        @RsRpc
        void send(Publisher<Integer> values);

        @RsRpc
        void send2(Integer values);

        @RsRpc
        Publisher<Integer> receive();
        
        @RsRpc
        Integer receive2();
        
        @RsRpc
        void umap(Function<Publisher<Integer>, Publisher<Integer>> mapper);
        
        @RsRpc
        void send3(Integer v1, Integer v2);
        
        @RsRpc
        Integer receive3();
        
        @RsRpc
        Integer map3(Integer v1, Integer v2);
    }
    
    static class PingPongServerAPI {
        
        @RsRpc
        public void send3(RpcStreamContext<Void> ctx, Integer v1, Integer v2) {
            System.out.println("Server send3: " + v1 + ", " + v2);
        }
        
        @RsRpc
        public Integer receive3(RpcStreamContext<Void> ctx) {
            return 33;
        }
        
        @RsRpc
        public Integer map3(RpcStreamContext<Void> ctx, Integer v1, Integer v2) {
            return v1 + v2;
        }

        @RsRpc
        public Publisher<Integer> pong2(RpcStreamContext<Void> ctx, Publisher<Integer> ping) {
            return Px.wrap(ping).map(v -> v + 1);
        }

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
        public void send2(RpcStreamContext<Void> ctx, Publisher<Integer> values) {
            send(ctx, values);
        }
        @RsRpc
        public Publisher<Integer> receive(RpcStreamContext<Void> ctx) {
            return Px.range(1, 1000);
        }

        @RsRpc
        public Publisher<Integer> receive2(RpcStreamContext<Void> ctx) {
            return receive(ctx);
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

            System.out.println("Sync map:");
            System.out.println(api.pong2(2));
            
            System.out.println("Send:");
            api.send(Px.just(20));
            
            Thread.sleep(200);

            System.out.println("Send:");
            api.send2(25);
            
            Thread.sleep(200);

            System.out.println("Receive:");
            long t = System.currentTimeMillis();
            print(Px.wrap(api.receive()));
            
            System.out.println("t = " + (System.currentTimeMillis() - t));

            System.out.println("Receive sync:");
            System.out.println(api.receive2());
            
            System.out.println("Umap:");
            api.umap(o -> Px.wrap(o).map(v -> -v));
            
            Thread.sleep(5000);
            
            System.out.println("-----------");
            
            api.send3(1, 2);
            
            Thread.sleep(1000);
            
            System.out.println(api.receive3());
            
            System.out.println(api.map3(1, 2));
            
            cancel.get().dispose();
        }
        
    }
}
