package hu.akarnokd.reactiverpc;

import java.io.IOException;
import java.net.*;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import rsc.flow.Cancellation;
import rsc.scheduler.*;
import rsc.util.UnsignalledExceptions;

public final class RpcServer<T> {
    
    final Object localAPI;
    final Class<T> remoteAPI;

    static Scheduler scheduler = new ExecutorServiceScheduler(Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "akarnokd-reactive-rpc-clientpool");
        t.setDaemon(true);
        return t;
    }));

    
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
        Scheduler acceptor = new ParallelScheduler(1, "akarnokd-reactive-rpc-connection", true);

        AtomicBoolean done = new AtomicBoolean();
        Cancellation c = acceptor.schedule(() -> {
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
                    UnsignalledExceptions.onErrorDropped(e);
                }
                return;
            }
            
            try {
                RpcSocketManager.connect(socket, socket.getInetAddress(), socket.getPort(), 
                        remoteAPI, localAPI, c -> { }, scheduler, true);
            } catch (Throwable ex) {
                UnsignalledExceptions.onErrorDropped(ex);
                try {
                    socket.close();
                } catch (IOException e) {
                    UnsignalledExceptions.onErrorDropped(e);
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
