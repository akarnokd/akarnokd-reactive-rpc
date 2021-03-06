package hu.akarnokd.reactive.rpc;

import java.io.IOException;
import java.net.*;
import java.util.Objects;
import java.util.function.Consumer;

import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

public final class RpcClient<T> {

    final Class<T> remoteAPI;
    
    final Object localAPI;
    
    static Scheduler scheduler = Schedulers.io();
    
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
    
    public T connect(InetAddress endpoint, int port, Consumer<Disposable> close) {
        Socket socket;
        
        try {
            socket = new Socket(endpoint, port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return RpcSocketManager.connect(socket, endpoint, port, remoteAPI, localAPI, close, scheduler, false);
    }
}
