package hu.akarnokd.reactiverpc;

import java.io.*;
import java.lang.reflect.Proxy;
import java.net.*;
import java.util.Map;
import java.util.function.Consumer;

import rsc.flow.Cancellation;
import rsc.scheduler.*;
import rsc.util.UnsignalledExceptions;

public enum RpcSocketManager {
    ;
    
    public static <T> T connect(Socket socket, InetAddress endpoint, int port, 
            Class<T> remoteAPI, Object localAPI, Consumer<Cancellation> close) {
        ;
        InputStream in;
        OutputStream out;
        
        try {
            in = socket.getInputStream();
            out = socket.getOutputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        Scheduler dispatcher = new ParallelScheduler(2, "akarnokd-reactive-rpc-io", true);
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
        
        io[0].start();
        
        return api;
    }
}
