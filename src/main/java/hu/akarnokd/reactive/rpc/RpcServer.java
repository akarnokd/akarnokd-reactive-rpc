package hu.akarnokd.reactive.rpc;

import java.io.IOException;
import java.net.*;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.schedulers.SingleScheduler;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;

public final class RpcServer<T> {
    
    final Object localAPI;
    final Class<T> remoteAPI;

    static Scheduler scheduler = Schedulers.io();

    
    private RpcServer(Object localAPI, Class<T> remoteAPI) {
        this.localAPI = localAPI;
        this.remoteAPI = remoteAPI;
        
    }
    
    public static RpcServer<Void> createLocal(Object localAPI) {
        Objects.requireNonNull(localAPI, "localAPI");
        RpcServiceMapper.serverServiceMap(localAPI);
        return new RpcServer<>(localAPI, null);
    }
    
    public static <T> RpcServer<T> createRemote(Class<T> remoteAPI) {
        Objects.requireNonNull(remoteAPI, "remoteAPI");
        RpcServiceMapper.clientServiceMap(remoteAPI);
        return new RpcServer<>(null, remoteAPI);
    }
    
    public static <T> RpcServer<T> createBidirectional(Object localAPI, Class<T> remoteAPI) {
        Objects.requireNonNull(localAPI, "localAPI");
        RpcServiceMapper.serverServiceMap(localAPI);
        Objects.requireNonNull(remoteAPI, "remoteAPI");
        RpcServiceMapper.clientServiceMap(remoteAPI);
        return new RpcServer<>(localAPI, remoteAPI);
    }
    
    public AutoCloseable start(int port) {
        ServerSocket ssocket;
        try {
            ssocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return setup(ssocket);
    }
    
    AutoCloseable setup(ServerSocket ssocket) {
        Scheduler acceptor = new SingleScheduler();

        AtomicBoolean done = new AtomicBoolean();
        Disposable c = acceptor.scheduleDirect(() -> {
            socketAccept(ssocket, done);
        });
        
        return () -> {
            if (done.compareAndSet(false, true)) {
                c.dispose();
                ssocket.close();
            }
        };
    }
    
    void socketAccept(ServerSocket ssocket, AtomicBoolean done) {
        while (!Thread.currentThread().isInterrupted()) {
            Socket socket;
            
            try {
                socket = ssocket.accept();
                
            } catch (IOException e) {
                if (!done.get()) {
                    RxJavaPlugins.onError(e);
                }
                return;
            }
            
            try {
                RpcSocketManager.connect(socket, socket.getInetAddress(), socket.getPort(), 
                        remoteAPI, localAPI, c -> { }, scheduler, true);
            } catch (Throwable ex) {
                RxJavaPlugins.onError(ex);
                try {
                    socket.close();
                } catch (IOException e) {
                    RxJavaPlugins.onError(e);
                }
            }
        }
    }
    
    public AutoCloseable start(InetAddress localAddress, int port) {
        ServerSocket ssocket;
        try {
            ssocket = new ServerSocket(port, 50, localAddress);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return setup(ssocket);
    }
}
