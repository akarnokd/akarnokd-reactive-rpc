package hu.akarnokd.reactiverpc;

import java.io.IOException;
import java.net.*;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import rsc.flow.Cancellation;
import rsc.scheduler.*;

public final class RpcClient<T> {

    final Class<T> remoteAPI;
    
    final Object localAPI;
    
    static Scheduler scheduler = new ExecutorServiceScheduler(Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "akarnokd-reactive-rpc-clientpool");
        t.setDaemon(true);
        return t;
    }));
    
    private RpcClient(Class<T> remoteAPI, Object localAPI) {
        this.remoteAPI = remoteAPI;
        this.localAPI = localAPI;
    }
    
    public static RpcClient<Void> createLocal(Object localAPI) {
        Objects.requireNonNull(localAPI, "localAPI");
        return new RpcClient<>(null, localAPI);
    }
    
    public static <T> RpcClient<T> createRemote(Class<T> remoteAPI) {
        Objects.requireNonNull(remoteAPI, "remoteAPI");
        return new RpcClient<>(remoteAPI, null);
    }
    
    public static <T> RpcClient<T> createBidirectional(Class<T> remoteAPI, Object localAPI) {
        Objects.requireNonNull(remoteAPI, "remoteAPI");
        Objects.requireNonNull(localAPI, "localAPI");
        return new RpcClient<>(remoteAPI, localAPI);
    }
    
    public T connect(InetAddress endpoint, int port, Consumer<Cancellation> close) {
        Socket socket;
        
        try {
            socket = new Socket(endpoint, port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return RpcSocketManager.connect(socket, endpoint, port, remoteAPI, localAPI, close, scheduler, false);
    }
}
