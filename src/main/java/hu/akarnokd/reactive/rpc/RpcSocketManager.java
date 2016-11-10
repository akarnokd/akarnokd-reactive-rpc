package hu.akarnokd.reactive.rpc;

import java.io.*;
import java.lang.reflect.Proxy;
import java.net.*;
import java.util.Map;
import java.util.function.Consumer;

import hu.akarnokd.reactive.pc.RsAPIManager;
import io.reactivex.Scheduler;
import io.reactivex.disposables.*;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;

enum RpcSocketManager {
    ;
    
    public static <T> T connect(Socket socket, InetAddress endpoint, int port, 
            Class<T> remoteAPI, Object localAPI, 
            Consumer<Disposable> close, Scheduler scheduler,
            boolean server) {
        ;
        InputStream in;
        OutputStream out;
        
        try {
            in = socket.getInputStream();
            out = socket.getOutputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        Scheduler.Worker reader = Schedulers.io().createWorker();
        Scheduler.Worker writer = Schedulers.io().createWorker();
        
        Map<String, Object> clientMap;
        Map<String, Object> serverMap;


        RsAPIManager[] am = { null };
        T api;
        RpcStreamContextImpl<T> ctx;
        
        if (remoteAPI != null) {
            clientMap = RpcServiceMapper.clientServiceMap(remoteAPI);

            api = remoteAPI.cast(Proxy.newProxyInstance(remoteAPI.getClassLoader(), new Class[] { remoteAPI }, 
                (o, m, args) -> {
                    String name = m.getName();
                    RsRpc a = m.getAnnotation(RsRpc.class);
                    if (a == null) {
                        throw new IllegalArgumentException("The method '" + m.getName() + "' is not annotated with RsRpc");
                    }
                    String aname = a.name();
                    if (!aname.isEmpty()) {
                        name = aname;
                    }
                    
                    Object action = clientMap.get(name);
                    if (action == null) {
                        throw new IllegalArgumentException("The method '" + m.getName() + "' is not a proper RsRpc method");
                    }
                    return RpcServiceMapper.dispatchClient(name, action, args, am[0]);
                }
            ));
        } else {
            api = null;
        }

        RpcIOManager rpcIo;
        
        ctx = new RpcStreamContextImpl<>(endpoint, port, api, Schedulers.io());

        if (localAPI != null) {
            serverMap = RpcServiceMapper.serverServiceMap(localAPI);
            
            RsAPIManager apiMgr = new RsAPIManager(server, (streamId, function, iom) -> {
                Object action = serverMap.get(function);
                if (action == null) {
                    RxJavaPlugins.onError(new IllegalStateException("Function " + function + " not found"));
                    return false;
                }
                return RpcServiceMapper.dispatchServer(streamId, action, iom, ctx);
            }, 
            () -> {
                RpcServiceMapper.invokeDone(localAPI, ctx);
            });
            
            rpcIo = new RpcIOManager(reader, in, writer, out, apiMgr, server);
            apiMgr.setSend(rpcIo);
            
            am[0] = apiMgr;
            reader.schedule(() -> { RpcServiceMapper.invokeInit(localAPI, ctx); });
        } else {
            RsAPIManager apiMgr = new RsAPIManager(server, (streamId, function, iom) -> false, 
            () -> { });
            
            rpcIo = new RpcIOManager(reader, in, writer, out, apiMgr, server);
            apiMgr.setSend(rpcIo);
            
            am[0] = apiMgr;
        }
        
        rpcIo.start();
        
        close.accept(Disposables.fromRunnable(() -> {
            rpcIo.close();
            
            try {
                socket.close();
            } catch (IOException ex) {
                RxJavaPlugins.onError(ex);
            }
        }));
        
        return api;
    }
}
