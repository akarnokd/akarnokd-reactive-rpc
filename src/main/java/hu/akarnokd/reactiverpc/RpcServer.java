package hu.akarnokd.reactiverpc;

import java.io.IOException;
import java.net.*;
import java.util.Objects;

import rsc.flow.Cancellation;
import rsc.scheduler.*;
import rsc.util.UnsignalledExceptions;

public final class RpcServer<T> {
    
    final Object localAPI;
    final Class<T> remoteAPI;

    private RpcServer(Object localAPI, Class<T> remoteAPI) {
        this.localAPI = localAPI;
        this.remoteAPI = remoteAPI;
        
    }
    
    public static RpcServer<Void> createLocal(Object localAPI) {
        Objects.requireNonNull(localAPI, "localAPI");
        return new RpcServer<>(localAPI, null);
    }
    
    public static <T> RpcServer<T> createRemote(Class<T> remoteAPI) {
        Objects.requireNonNull(remoteAPI, "remoteAPI");
        return new RpcServer<>(null, remoteAPI);
    }
    
    public static <T> RpcServer<T> createBidirectional(Object localAPI, Class<T> remoteAPI) {
        Objects.requireNonNull(localAPI, "localAPI");
        Objects.requireNonNull(remoteAPI, "remoteAPI");
        return new RpcServer<>(localAPI, remoteAPI);
    }
    
    public AutoCloseable start(int port) {
        ServerSocket ssocket;
        try {
            ssocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        Scheduler acceptor = new ParallelScheduler(1, "akarnokd-reactive-rpc-connection", true);
        
        Cancellation c = acceptor.schedule(() -> {
            socketAccept(ssocket);
        });
        
        return () -> {
            c.dispose();
            ssocket.close();
        };
    }
    
    void socketAccept(ServerSocket ssocket) {
        while (!Thread.currentThread().isInterrupted()) {
            Socket socket;
            
            try {
                socket = ssocket.accept();
                
            } catch (IOException e) {
                UnsignalledExceptions.onErrorDropped(e);
                return;
            }
            
            RpcSocketManager.connect(socket, socket.getInetAddress(), socket.getPort(), 
                    remoteAPI, localAPI, c -> { });
        }
    }
    
    public AutoCloseable start(InetAddress localAddress, int port) {
        return null;
    }
}
