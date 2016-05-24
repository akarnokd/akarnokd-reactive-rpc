package hu.akarnokd.reactiverpc;

import java.io.*;
import java.lang.reflect.Proxy;
import java.net.*;
import java.util.*;
import java.util.function.Consumer;

import com.sun.media.jfxmediaimpl.MediaDisposer.Disposable;

import rsc.scheduler.*;
import rsc.util.UnsignalledExceptions;

public final class RpcClient<T> {

    final Class<T> remoteAPI;
    
    final Object localAPI;
    
    final Scheduler dispatcher;
    
    private RpcClient(Class<T> remoteAPI, Object localAPI) {
        this.remoteAPI = remoteAPI;
        this.localAPI = localAPI;
        this.dispatcher = new ParallelScheduler(2, "akarnokd-reactive-rpc-io", true);
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
        InputStream in;
        OutputStream out;
        
        try {
            socket = new Socket(endpoint, port);
            
            in = socket.getInputStream();
            out = socket.getOutputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        Scheduler.Worker reader = dispatcher.createWorker();
        Scheduler.Worker writer = dispatcher.createWorker();
        
        close.accept(() -> {
            reader.shutdown();
            writer.shutdown();
            
            try {
                socket.close();
            } catch (IOException ex) {
                UnsignalledExceptions.onErrorDropped(ex);
            }
        });
        
        
        
        Map<String, Object> clientMap;
        Map<String, Object> serverMap;

        RpcIOManager[] io = { null };
        T api;
        RpcStreamContextImpl<T> ctx;
        
        if (remoteAPI != null) {
            clientMap = RpcServiceMapper.clientServiceMap(remoteAPI);

            api = remoteAPI.cast(Proxy.newProxyInstance(remoteAPI.getClassLoader(), new Class[] { remoteAPI }, 
                (o, m, args) -> {
                    String name = m.getName();
                    RsRpc a = m.getAnnotation(RsRpc.class);
                    if (a == null) {
                        throw new IllegalArgumentException("The method " + m.getName() + " is not a proper RsRpc method");
                    }
                    String aname = a.name();
                    if (!aname.isEmpty()) {
                        name = aname;
                    }
                    
                    Object action = clientMap.get(name);
                    if (action == null) {
                        throw new IllegalArgumentException("The method " + m.getName() + " is not a proper RsRpc method");
                    }
                    return RpcServiceMapper.dispatchClient(name, action, args, io[0]);
                }
            ));
        } else {
            api = null;
        }

        ctx = new RpcStreamContextImpl<>(endpoint, port, api);

        if (localAPI != null) {
            serverMap = RpcServiceMapper.serverServiceMap(localAPI);
            
            io[0] = new RpcIOManager(reader, in, writer, out, (streamId, function, iom) -> {
                Object action = serverMap.get(function);
                return RpcServiceMapper.dispatchServer(streamId, action, iom, ctx);
            }, false);
            
            RpcServiceMapper.invokeInit(localAPI, ctx);
        } else {
            io[0] = new RpcIOManager(reader, in, writer, out, (streamId, function, iom) -> false, false);
        }
        
        
        return api;
    }
}
