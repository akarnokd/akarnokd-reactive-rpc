package hu.akarnokd.reactive.rpc;

import java.io.IOException;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;

import org.reactivestreams.*;

import hu.akarnokd.reactive.pc.RsAPIManager;
import io.reactivex.Flowable;
import io.reactivex.internal.subscribers.BlockingLastSubscriber;
import io.reactivex.internal.subscriptions.*;
import io.reactivex.plugins.RxJavaPlugins;

enum RpcServiceMapper {
    ;
    
    public static void invokeInit(Object api, RpcStreamContext<?> ctx) {
        for (Method m : api.getClass().getMethods()) {
            if (m.isAnnotationPresent(RsRpcInit.class)) {
                if (m.getReturnType() == Void.TYPE) {
                    if (m.getParameterCount() == 1 &&
                            RpcStreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
                        
                        try {
                            m.invoke(api, ctx);
                        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                            RxJavaPlugins.onError(e);
                            throw new IllegalStateException(e);
                        }
                        
                        return;
                    }
                }
                throw new IllegalStateException("RsRpcInit method has to be void and accepting only a single RpcStreamContext parameter");
            }
        }
    }
    
    public static void invokeDone(Object api, RpcStreamContext<?> ctx) {
        for (Method m : api.getClass().getMethods()) {
            if (m.isAnnotationPresent(RsRpcDone.class)) {
                if (m.getReturnType() == Void.TYPE) {
                    if (m.getParameterCount() == 1 &&
                            RpcStreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
                        
                        try {
                            m.invoke(api, ctx);
                        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                            RxJavaPlugins.onError(e);
                            throw new IllegalStateException(e);
                        }
                        
                        return;
                    }
                }
                throw new IllegalStateException("RsRpcInit method has to be void and accepting only a single RpcStreamContext parameter");
            }
        }
    }
    
    public static Map<String, Object> serverServiceMap(Object api) {
        Map<String, Object> result = new HashMap<>();
        
        for (Method m : api.getClass().getMethods()) {
            if (m.isAnnotationPresent(RsRpc.class)) {
                RsRpc a = m.getAnnotation(RsRpc.class);
                
                String name = m.getName();
                
                String aname = a.name();
                if (!aname.isEmpty()) {
                    name = aname;
                }
                
                Class<?> rt = m.getReturnType();
                
                if (rt == Void.TYPE) {
                    int pc = m.getParameterCount();
                    if (pc == 2) {
                        if (RpcStreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
                            if (Publisher.class.isAssignableFrom(m.getParameterTypes()[1])) {
                                result.put(name, new RpcServerReceive(m, api));
                            } else {
                                throw new IllegalStateException("RsRpc annotated methods require a second Publisher as a parameter: " + m);
                            }
                        } else {
                            throw new IllegalStateException("RsRpc annotated methods require a first RpcStreamContext as a parameter: " + m);
                        }
                    } else 
                    if (pc > 2) {
                        if (RpcStreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
                            result.put(name, new RpcServerSyncReceive(m, api));
                        } else {
                            throw new IllegalStateException("RsRpc annotated methods require a first RpcStreamContext as a parameter: " + m);
                        }
                    } else {
                        throw new IllegalStateException("RsRpc annotated methods require one RpcStreamContext and one Publisher as a parameter: " + m);
                    }
                } else
                if (Publisher.class.isAssignableFrom(rt)) {
                    int pc = m.getParameterCount();
                    if (pc == 1) {
                        if (RpcStreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
                            result.put(name, new RpcServerSend(m, api));
                        } else {
                            throw new IllegalStateException("RsRpc annotated methods require at one RpcStreamContext as a parameter: " + m);
                        }
                    } else
                    if (pc == 2) {
                        if (RpcStreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
                            if (Publisher.class.isAssignableFrom(m.getParameterTypes()[1])) {
                                result.put(name, new RpcServerMap(m, api));
                            } else {
                                throw new IllegalStateException("RsRpc annotated methods require the second parameter to be Publisher.");
                            }
                        } else {
                            throw new IllegalStateException("RsRpc annotated methods require the first parameter to be RpcStreamContext.");
                        }
                    } else {
                        throw new IllegalStateException("RsRpc annotated methods require one RpcStreamContext and one Publisher as a parameter: " + m);
                    }
                } else {
                    int pc = m.getParameterCount();
                    if (pc > 0 && RpcStreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
                        if (pc == 1) {
                            result.put(name, new RpcServerSyncSend(m, api));
                        } else {
                            result.put(name, new RpcServerSyncMap(m, api));
                        }
                    } else {
                        throw new IllegalStateException("RsRpc annotated methods require at one RpcStreamContext as a parameter: " + m);
                    }
                }
            }
        }
        
        return result;
    }
    
    public static Map<String, Object> clientServiceMap(Class<?> api) {
        Map<String, Object> result = new HashMap<>();
        
        for (Method m : api.getMethods()) {
            if (m.isAnnotationPresent(RsRpc.class)) {
                RsRpc a = m.getAnnotation(RsRpc.class);
                
                String name = m.getName();
                
                String aname = a.name();
                if (!aname.isEmpty()) {
                    name = aname;
                }
                
                if (result.containsKey(name)) {
                    throw new IllegalStateException("Overloads with the same target name are not supported");
                }
                
                Class<?> rt = m.getReturnType();
                
                if (rt == Void.TYPE) {
                    int pc = m.getParameterCount();
                    if (pc == 0) {
                        throw new IllegalStateException("RsRpc annotated void methods require at least one parameter");
                    } else
                    if (pc == 1) {
                        if (Function.class.isAssignableFrom(m.getParameterTypes()[0])) {
                            result.put(name, new RpcClientUmap());
                            continue;
                        } else
                        if (Publisher.class.isAssignableFrom(m.getParameterTypes()[0])) {
                            result.put(name, new RpcClientSend());
                            continue;
                        }
                    }
                    result.put(name, new RpcClientSyncSend());
                } else
                if (Publisher.class.isAssignableFrom(rt)) {
                    int pc = m.getParameterCount();
                    if (pc > 1) {
                        throw new IllegalStateException("RsRpc annotated methods returning a Publisher require 0 or 1 parameter: " + m);
                    }
                    if (pc == 0) {
                        result.put(name, new RpcClientReceive());
                    } else {
                        if (Publisher.class.isAssignableFrom(m.getParameterTypes()[0])) {
                            result.put(name, new RpcClientMap());
                        } else {
                            throw new IllegalStateException("RsRpc annotated methods returning a Publisher allows only Publisher as parameter: " + m);
                        }
                    }
                } else {
                    if (m.getParameterCount() == 0) {
                        result.put(name, new RpcClientSyncReceive());
                    } else {
                        result.put(name, new RpcClientSyncMap());
                    }
                }
            }
        }
        
        return result;
    }
    
    public static boolean dispatchServer(long streamId, Object action, RsAPIManager io, RpcStreamContext<?> ctx) {
        if (action instanceof RpcServerSend) {
            RpcServerSend rpcServerSend = (RpcServerSend) action;
            return rpcServerSend.send(streamId, ctx, io);
        } else
        if (action instanceof RpcServerReceive) {
            RpcServerReceive rpcServerReceive = (RpcServerReceive) action;
            return rpcServerReceive.receive(streamId, ctx, io);
        } else
        if (action instanceof RpcServerMap) {
            RpcServerMap rpcServerMap = (RpcServerMap) action;
            return rpcServerMap.map(streamId, ctx, io);
        } else
        if (action instanceof RpcServerSyncSend) {
            RpcServerSyncSend rpcServerSyncSend = (RpcServerSyncSend) action;
            return rpcServerSyncSend.send(streamId, ctx, io);
        } else
        if (action instanceof RpcServerSyncReceive) {
            RpcServerSyncReceive rpcServerSyncReceive = (RpcServerSyncReceive) action;
            return rpcServerSyncReceive.receive(streamId, ctx, io);
        } else
        if (action instanceof RpcServerSyncMap) {
            RpcServerSyncMap rpcServerSyncMap = (RpcServerSyncMap) action;
            return rpcServerSyncMap.map(streamId, ctx, io);
        }
        RxJavaPlugins.onError(new IllegalStateException("Unsupported action: " + action.getClass()));
        return false;
    }
    
    public static Object dispatchClient(String name, Object action, Object[] args, RsAPIManager io) {
        if (action instanceof RpcClientSend) {
            if (args[0] == null) {
                throw new NullPointerException("The source Publisher is null");
            }
            RpcClientSend rpcSend = (RpcClientSend) action;
            rpcSend.send(name, (Publisher<?>)args[0], io);
            return null;
        } else
        if (action instanceof RpcClientReceive) {
            RpcClientReceive rpcReceive = (RpcClientReceive) action;
            return rpcReceive.receive(name, io);
        } else
        if (action instanceof RpcClientMap) {
            if (args[0] == null) {
                throw new NullPointerException("The source Publisher is null");
            }
            RpcClientMap rpcMap = (RpcClientMap) action;
            return rpcMap.map(name, (Publisher<?>)args[0], io);
        } else
        if (action instanceof RpcClientUmap) {
            if (args[0] == null) {
                throw new NullPointerException("The umapper function is null");
            }
            RpcClientUmap rpcUmap = (RpcClientUmap) action;
            @SuppressWarnings("unchecked")
            Function<Publisher<?>, Publisher<?>> f = (Function<Publisher<?>, Publisher<?>>)args[0];
            rpcUmap.umap(name, f, io);
            return null;
        } else
        if (action instanceof RpcClientSyncSend) {
            RpcClientSyncSend rpcClientSyncSend = (RpcClientSyncSend) action;
            rpcClientSyncSend.send(name, args, io);
            return null;
        } else
        if (action instanceof RpcClientSyncReceive) {
            RpcClientSyncReceive rpcClientSyncReceive = (RpcClientSyncReceive) action;
            return rpcClientSyncReceive.receive(name, io);
        } else
        if (action instanceof RpcClientSyncMap) {
            RpcClientSyncMap rpcClientSyncMap = (RpcClientSyncMap) action;
            return rpcClientSyncMap.map(name, args, io);
        }
        
        throw new IllegalStateException("Unsupported action class: " + action.getClass());
    }
    
    static final class RpcClientSyncSend {
        
        public void send(String function, Object[] args, RsAPIManager io) {
            if (args.length == 1) {
                RpcClientSend.sendStatic(function, Flowable.just(args[0]), io);
            } else {
                RpcClientSend.sendStatic(function, Flowable.just(args), io);
            }
        }
    }
    
    static final class RpcClientSend {

        public static void sendStatic(String function, Publisher<?> values, RsAPIManager io) {
            long streamId = io.newStreamId();
            
            SendSubscriber s = new SendSubscriber(io, streamId);
            io.registerSubscription(streamId, s);
            io.sendNew(streamId, function);
            
            values.subscribe(s);
        }

        public void send(String function, Publisher<?> values, RsAPIManager io) {
            sendStatic(function, values, io);
        }
        
        static final class SendSubscriber 
        extends DeferredSubscription
        implements Subscriber<Object> {
            private static final long serialVersionUID = 5036194896026890449L;

            final RsAPIManager io;
            
            final long streamId;
            
            boolean done;
            
            public SendSubscriber(RsAPIManager io, long streamId) {
                this.io = io;
                this.streamId = streamId;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                super.setSubscription(s);
            }
            
            @Override
            public void onNext(Object t) {
                if (done) {
                    return;
                }
                try {
                    io.sendNext(streamId, t);
                } catch (IOException ex) {
                    cancel();
                    onError(ex);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                if (done) {
                    RxJavaPlugins.onError(t);
                    return;
                }
                done = true;
                io.deregister(streamId);
                io.sendError(streamId, t);
            }
            
            @Override
            public void onComplete() {
                if (done) {
                    return;
                }
                done = true;
                io.deregister(streamId);
                io.sendComplete(streamId);
            }
        }
    }
    
    static final class RpcClientSyncReceive {
        public Object receive(String function, RsAPIManager io) {
            Publisher<?> p = RpcClientReceive.receiveStatic(function, io);
            
            BlockingLastSubscriber<Object> subscriber = new BlockingLastSubscriber<>();
            p.subscribe(subscriber);
            return subscriber.blockingGet();
        }
    }
    
    static final class RpcClientReceive {
        
        static final class RpcReceiveSubscription implements Subscription {
            final long streamId;
            
            final RsAPIManager io;
            
            public RpcReceiveSubscription(long streamId, RsAPIManager io) {
                this.streamId = streamId;
                this.io = io;
            }
            
            @Override
            public void request(long n) {
                if (SubscriptionHelper.validate(n)) {
                    io.sendRequested(streamId, n);
                }
            }

            @Override
            public void cancel() {
                io.deregister(streamId);
                io.sendCancel(streamId, "");
            }
        }

        public static Publisher<?> receiveStatic(String function, RsAPIManager io) {
            return s -> {
                long streamId = io.newStreamId();
                io.registerSubscriber(streamId, s);
                io.sendNew(streamId, function);
                
                s.onSubscribe(new RpcReceiveSubscription(streamId, io));
            };
        }
        
        public Publisher<?> receive(String function, RsAPIManager io) {
            return receiveStatic(function, io);
        }
        
    }
    
    static final class RpcClientSyncMap {
        public Object map(String function, Object[] args, RsAPIManager io) {
            Publisher<?> p; 
            
            if (args.length == 1) {
                p = RpcClientMap.mapStatic(function, Flowable.just(args[0]), io);
            } else {
                p = RpcClientMap.mapStatic(function, Flowable.just(args), io);
            }
            
            BlockingLastSubscriber<Object> subscriber = new BlockingLastSubscriber<>();
            p.subscribe(subscriber);
            return subscriber.blockingGet();
        }
    }
    
    static final class RpcClientMap {
        
        public static Publisher<?> mapStatic(String function, Publisher<?> values, RsAPIManager io) {
            return s -> {
                long streamId = io.newStreamId();
                
                final AtomicInteger open = new AtomicInteger(2);
                
                RpcMapReceiverSubscriber receiver = new RpcMapReceiverSubscriber(s, streamId, open, io);

                RpcMapSubscriber sender = new RpcMapSubscriber(streamId, open, io);
                receiver.sender = sender;
                
                io.registerSubscriber(streamId, receiver);
                io.registerSubscription(streamId, sender);
                io.sendNew(streamId, function);
                
                s.onSubscribe(receiver.s);
                
                values.subscribe(sender);
            };
        }
        
        public Publisher<?> map(String function, Publisher<?> values, RsAPIManager io) {
            return mapStatic(function, values, io);
        }
        
        static final class RpcMapSubscriber extends DeferredSubscription implements Subscriber<Object> {
            private static final long serialVersionUID = 45948919781327335L;

            final long streamId;
            
            final AtomicInteger open;
            
            final RsAPIManager io;
            
            boolean done;
            
            public RpcMapSubscriber(long streamId, AtomicInteger open, RsAPIManager io) {
                this.streamId = streamId;
                this.open = open;
                this.io = io;
            }

            @Override
            public void onSubscribe(Subscription s) {
                super.setSubscription(s);
            }

            @Override
            public void onNext(Object t) {
                if (done) {
                    return;
                }
                try {
                    io.sendNext(streamId, t);
                } catch (IOException ex) {
                    cancel();
                    onError(ex);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (done) {
                    RxJavaPlugins.onError(t);
                    return;
                }
                done = true;
                if (open.decrementAndGet() == 0) {
                    io.deregister(streamId);
                }
                io.sendError(streamId, t);
            }

            @Override
            public void onComplete() {
                if (done) {
                    return;
                }
                done = true;
                if (open.decrementAndGet() == 0) {
                    io.deregister(streamId);
                }
                io.sendComplete(streamId);
            }
        }
        
        static final class RpcMapReceiverSubscriber implements Subscriber<Object>, Subscription {
            final Subscriber<Object> actual;
            
            final long streamId;
            
            final AtomicInteger open;
            
            final RsAPIManager io;

            Subscription s;

            RpcMapSubscriber sender;
            
            public RpcMapReceiverSubscriber(Subscriber<Object> actual, long streamId, AtomicInteger open, RsAPIManager io) {
                this.actual = actual;
                this.streamId = streamId;
                this.open = open;
                this.io = io;
                this.s = new Subscription() {
                    @Override
                    public void request(long n) {
                        innerRequest(n);
                    }
                    
                    @Override
                    public void cancel() {
                        innerCancel();
                    }
                };
            }

            void innerRequest(long n) {
                if (SubscriptionHelper.validate(n)) {
                    io.sendRequested(streamId, n);
                }
            }
            
            void innerCancel() {
                if (open.decrementAndGet() != 0) {
                    io.deregister(streamId);
                }
                io.sendCancel(streamId, "");
            }
            

            @Override
            public void onSubscribe(Subscription s) {
                // IO manager won't call this
            }

            @Override
            public void onNext(Object t) {
                actual.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                if (open.decrementAndGet() != 0) {
                    io.deregister(streamId);
                }
                actual.onError(t);
            }

            @Override
            public void onComplete() {
                if (open.decrementAndGet() != 0) {
                    io.deregister(streamId);
                }
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                sender.request(n);
            }

            @Override
            public void cancel() {
                sender.cancel();
            }
        }
    }
    
    static final class RpcClientUmap {
        public void umap(String function, Function<Publisher<?>, Publisher<?>> mapper, RsAPIManager io) {

            long streamId = io.newStreamId();
            
            AtomicBoolean onceInner = new AtomicBoolean();
            
            RpcUmapReceiver receiver = new RpcUmapReceiver(streamId, io, onceInner);
            
            receiver.provider = new RpcUmapProvider(streamId, io, onceInner);
            
            io.registerSubscriber(streamId, receiver);
            io.registerSubscription(streamId, receiver);
            
            io.sendNew(streamId, function);

            AtomicBoolean once = new AtomicBoolean();
            Publisher<Object> p = s -> {
                if (once.compareAndSet(false, true)) {
                    receiver.actual = s;
                    s.onSubscribe(receiver.s);
                } else {
                    EmptySubscription.error(new IllegalStateException("Only one subscriber allowed"), s);
                }
            };
            
            Publisher<?> u;
            
            try {
                u = mapper.apply(p);
            } catch (Throwable ex) {
                u = w -> {
                    EmptySubscription.error(ex, w);
                };
            }
            
            if (u == null) {
                u = w -> {
                    EmptySubscription.error(new NullPointerException("The umapper returned a null Publisher"), w);
                };
            }
            
            u.subscribe(receiver.provider);
        }
        
        static final class RpcUmapReceiver implements Subscriber<Object>, Subscription {
            final long streamId;
            
            final RsAPIManager io;
            
            final AtomicBoolean once;
            
            Subscriber<Object> actual;
            
            RpcUmapProvider provider;
            
            Subscription s;
            
            public RpcUmapReceiver(long streamId, RsAPIManager io, AtomicBoolean once) {
                this.streamId = streamId;
                this.io = io;
                this.once = once;
                this.s = new Subscription() {
                    @Override
                    public void request(long n) {
                        if (SubscriptionHelper.validate(n)) {
                            io.sendRequested(streamId, n);
                        }
                    }
                    
                    @Override
                    public void cancel() {
                        if (once.compareAndSet(false, true)) {
                            io.sendCancel(streamId, "");
                        }
                    }
                };
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                // not called
            }
            
            @Override
            public void onNext(Object t) {
                actual.onNext(t);
            }
            
            @Override
            public void onError(Throwable t) {
                once.set(true);
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                once.set(true);
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                provider.request(n);
            }
            
            @Override
            public void cancel() {
                provider.cancel();
            }
        }
        
        static final class RpcUmapProvider extends DeferredSubscription implements Subscriber<Object> {
            private static final long serialVersionUID = 2958791596026498820L;

            final long streamId;
            
            final RsAPIManager io;
            
            final AtomicBoolean once;
            
            boolean done;
            
            public RpcUmapProvider(long streamId, RsAPIManager io, AtomicBoolean once) {
                this.streamId = streamId;
                this.io = io;
                this.once = once;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                setSubscription(s);
            }
            
            @Override
            public void onNext(Object t) {
                if (done) {
                    return;
                }
                try {
                    io.sendNext(streamId, t);
                } catch (IOException ex) {
                    onError(ex);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                if (done) {
                    RxJavaPlugins.onError(t);
                    return;
                }
                done = true;
                cancel();
                io.deregister(streamId);
                if (once.compareAndSet(false, true)) {
                    io.sendCancel(streamId, "");
                }
                io.sendError(streamId, t);
            }
            
            @Override
            public void onComplete() {
                if (done) {
                    return;
                }
                done = true;
                cancel();
                io.deregister(streamId);
                if (once.compareAndSet(false, true)) {
                    io.sendCancel(streamId, "");
                }
                io.sendComplete(streamId);
            }
        }
    }
    
    static final class RpcServerSyncSend {
        final Method m;
        
        final Object instance;
        
        public RpcServerSyncSend(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean send(long streamId, RpcStreamContext<?> ctx, RsAPIManager io) {
            RpcServerSend.ServerSendSubscriber parent = new RpcServerSend.ServerSendSubscriber(streamId, io);
            io.registerSubscription(streamId, parent);
            
            Flowable.fromCallable(() -> {
                return m.invoke(instance, ctx);
            })
            .subscribe(parent);
            
            return true;
        }
    }
    
    static final class RpcServerSend {
        final Method m;
        
        final Object instance;
        
        public RpcServerSend(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean send(long streamId, RpcStreamContext<?> ctx, RsAPIManager io) {
            Publisher<?> output;
            try {
                output = (Publisher<?>)m.invoke(instance, ctx);
            } catch (Throwable ex) {
                RxJavaPlugins.onError(ex);
                
                io.sendError(streamId, ex);
                return true;
            }
            
            if (output == null) {
                io.sendError(streamId, new IllegalStateException("The service implementation returned a null Publisher"));
                return true;
            }

            ServerSendSubscriber parent = new ServerSendSubscriber(streamId, io);
            io.registerSubscription(streamId, parent);
            
            output.subscribe(parent);
            
            return true;
        }
        
        static final class ServerSendSubscriber 
        extends DeferredSubscription implements Subscriber<Object> {
            private static final long serialVersionUID = 4628307619135978981L;

            final long streamId;
            
            final RsAPIManager io;
            
            boolean done;
            
            public ServerSendSubscriber(long streamId, RsAPIManager io) {
                this.streamId = streamId;
                this.io = io;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                setSubscription(s);
            }
            
            @Override
            public void onNext(Object t) {
                if (done) {
                    return;
                }
                try {
                    io.sendNext(streamId, t);
                } catch (IOException ex) {
                    cancel();
                    onError(ex);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                if (done) {
                    RxJavaPlugins.onError(t);
                    return;
                }
                done = true;
                io.deregister(streamId);
                io.sendError(streamId, t);
            }
            
            @Override
            public void onComplete() {
                if (done) {
                    return;
                }
                done = true;
                io.deregister(streamId);
                io.sendComplete(streamId);
            }
        }
    }
    
    static final class RpcServerSyncReceive {
        final Method m;
        
        final Object instance;
        
        public RpcServerSyncReceive(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean receive(long streamId, RpcStreamContext<?> ctx, RsAPIManager io) {
            RpcServerReceive.ServerReceiveSubscriber parent = new RpcServerReceive.ServerReceiveSubscriber(streamId, io);
            
            Publisher<?> p = s -> {
                    parent.actual = s;
                    io.registerSubscriber(streamId, parent);
                    s.onSubscribe(parent);
            };
            Flowable.fromPublisher(p).observeOn(ctx.scheduler()).map(o -> {
                syncInvoke(m, instance, ctx, o);
                return 0;
            }).subscribe();
            return true;
        }
    }
    
    static Object syncInvoke(Method m, Object instance, RpcStreamContext<?> ctx, Object o) {
        int c = m.getParameterCount();

        if (!(o instanceof Object[])) {
            if (c == 2 && m.getParameterTypes()[1].isAssignableFrom(o.getClass())) {
                try {
                    return m.invoke(instance, ctx, o);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            throw new IllegalArgumentException(m.getName() + ": input is not an array of objects but " + o.getClass());
        }
        Object[] a = (Object[])o;
        if (a.length != c - 1) {
            throw new IllegalArgumentException(m.getName() + ": Input array size mismatch: " + a.length + " instead of " + (c - 1));
        }
        
        Object[] b = new Object[c];
        b[0] = ctx;
        System.arraycopy(a, 0, b, 1, c - 1);
        
        try {
            return m.invoke(instance, b);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    static final class RpcServerReceive {
        final Method m;
        
        final Object instance;
        
        public RpcServerReceive(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean receive(long streamId, RpcStreamContext<?> ctx, RsAPIManager io) {
            
            ServerReceiveSubscriber parent = new ServerReceiveSubscriber(streamId, io);
            
            AtomicBoolean once = new AtomicBoolean();
            Publisher<?> p = s -> {
                if (once.compareAndSet(false, true)) {
                    parent.actual = s;
                    io.registerSubscriber(streamId, parent);
                    s.onSubscribe(parent);
                } else {
                    EmptySubscription.error(new IllegalStateException("This Publisher allows only a single subscriber"), s);
                }
            };
            
            try {
                m.invoke(instance, ctx, p);
            } catch (Throwable ex) {
                RxJavaPlugins.onError(ex);
                
                io.sendCancel(streamId, ex.toString());
            }
            return true;
        }
        
        static final class ServerReceiveSubscriber implements Subscriber<Object>, Subscription {
            final long streamId;
            
            final RsAPIManager io;
            
            Subscriber<Object> actual;

            public ServerReceiveSubscriber(long streamId, RsAPIManager io) {
                this.streamId = streamId;
                this.io = io;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                // not called by the IO manager
            }
            
            @Override
            public void onNext(Object t) {
                actual.onNext(t);
            }
            
            @Override
            public void onError(Throwable t) {
                io.deregister(streamId);
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                io.deregister(streamId);
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                if (SubscriptionHelper.validate(n)) {
                    io.sendRequested(streamId, n);
                }
            }
            
            @Override
            public void cancel() {
                io.deregister(streamId);
                io.sendCancel(streamId, "");
            }
        }
    }

    static final class RpcServerSyncMap {
        final Method m;
        
        final Object instance;
        
        public RpcServerSyncMap(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean map(long streamId, RpcStreamContext<?> ctx, RsAPIManager io) {
            AtomicInteger innerOnce = new AtomicInteger(2);

            RpcServerMap.ServerMapSubscriber parent = new RpcServerMap.ServerMapSubscriber(streamId, io, innerOnce);
            RpcServerMap.ServerSendSubscriber sender = new RpcServerMap.ServerSendSubscriber(streamId, io, innerOnce);
            parent.sender = sender;

            io.registerSubscriber(streamId, parent);
            io.registerSubscription(streamId, sender);

            AtomicBoolean once = new AtomicBoolean();
            
            Publisher<?> p = s -> {
                if (once.compareAndSet(false, true)) {
                    parent.actual = s;
                    s.onSubscribe(parent.s);
                } else {
                    EmptySubscription.error(new IllegalStateException("This Publisher allows only a single subscriber"), s);
                }
            };
            
            Publisher<?> u = Flowable.fromPublisher(p).observeOn(ctx.scheduler())
                    .map(o -> syncInvoke(m, instance, ctx, o));
            
            u.subscribe(sender);
            
            return true;
        }
    }
    
    static final class RpcServerMap {
        final Method m;
        
        final Object instance;
        
        public RpcServerMap(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean map(long streamId, RpcStreamContext<?> ctx, RsAPIManager io) {
            AtomicInteger innerOnce = new AtomicInteger(2);

            ServerMapSubscriber parent = new ServerMapSubscriber(streamId, io, innerOnce);
            ServerSendSubscriber sender = new ServerSendSubscriber(streamId, io, innerOnce);
            parent.sender = sender;

            io.registerSubscriber(streamId, parent);
            io.registerSubscription(streamId, sender);

            AtomicBoolean once = new AtomicBoolean();
            
            Publisher<?> p = s -> {
                if (once.compareAndSet(false, true)) {
                    parent.actual = s;
                    s.onSubscribe(parent.s);
                } else {
                    EmptySubscription.error(new IllegalStateException("This Publisher allows only a single subscriber"), s);
                }
            };
            
            Publisher<?> u;
            try {
                u = (Publisher<?>)m.invoke(instance, ctx, p);
            } catch (Throwable ex) {
                RxJavaPlugins.onError(ex);
                
                u = s -> EmptySubscription.error(ex, s);
            }
            
            if (u == null) {
                u = s -> EmptySubscription.error(new NullPointerException("The service implementation returned a null Publisher"), s);
            }
            
            u.subscribe(sender);
            
            return true;
        }
        
        static final class ServerMapSubscriber implements Subscriber<Object>, Subscription {
            final long streamId;
            
            final RsAPIManager io;
            
            final AtomicInteger once;
            
            final Subscription s;
            
            Subscriber<Object> actual;
            
            ServerSendSubscriber sender;

            public ServerMapSubscriber(long streamId, RsAPIManager io, AtomicInteger once) {
                this.streamId = streamId;
                this.io = io;
                this.once = once;
                this.s = new Subscription() {

                    @Override
                    public void request(long n) {
                        innerRequest(n);
                    }

                    @Override
                    public void cancel() {
                        innerCancel();
                    }
                    
                };
            }

            @Override
            public void onSubscribe(Subscription s) {
                // IO won't call this
            }
            
            @Override
            public void onNext(Object t) {
                actual.onNext(t);
            }
            
            @Override
            public void onError(Throwable t) {
                if (once.decrementAndGet() == 0) {
                    io.deregister(streamId);
                }
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                if (once.decrementAndGet() == 0) {
                    io.deregister(streamId);
                }
                actual.onComplete();
            }

            public void innerRequest(long n) {
                if (SubscriptionHelper.validate(n)) {
                    io.sendRequested(streamId, n);
                }
            }
            
            public void innerCancel() {
                if (once.decrementAndGet() == 0) {
                    io.deregister(streamId);
                }
                io.sendCancel(streamId, "");
            }

            @Override
            public void request(long n) {
                sender.request(n);
            }
            
            @Override
            public void cancel() {
                sender.cancel();
            }
            
        }
        static final class ServerSendSubscriber 
        extends DeferredSubscription implements Subscriber<Object> {
            private static final long serialVersionUID = -7430636028018326515L;

            final long streamId;
            
            final RsAPIManager io;
            
            final AtomicInteger once;
            
            boolean done;
            
            public ServerSendSubscriber(long streamId, RsAPIManager io, AtomicInteger once) {
                this.streamId = streamId;
                this.io = io;
                this.once = once;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                setSubscription(s);
            }
            
            @Override
            public void onNext(Object t) {
                if (done) {
                    return;
                }
                try {
                    io.sendNext(streamId, t);
                } catch (IOException ex) {
                    cancel();
                    onError(ex);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                if (done) {
                    RxJavaPlugins.onError(t);
                    return;
                }
                done = true;
                if (once.decrementAndGet() == 0) {
                    io.deregister(streamId);
                }
                io.sendError(streamId, t);
            }
            
            @Override
            public void onComplete() {
                if (done) {
                    return;
                }
                done = true;
                if (once.decrementAndGet() == 0) {
                    io.deregister(streamId);
                }
                io.sendComplete(streamId);
            }
        }
    }
}
