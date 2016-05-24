package hu.akarnokd.reactiverpc;

import java.io.IOException;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;

import org.reactivestreams.*;

import rsc.util.*;

public enum RpcServiceMapper {
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
                            UnsignalledExceptions.onErrorDropped(e);
                            throw new IllegalStateException(e);
                        }
                        
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
                    throw new IllegalStateException("Unsupported RsRpc annotated return type: " + m);
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
                
                Class<?> rt = m.getReturnType();
                
                if (rt == Void.TYPE) {
                    int pc = m.getParameterCount();
                    if (pc != 1) {
                        throw new IllegalStateException("RsRpc annotated methods require exactly one parameter: " + m);
                    }
                    if (Function.class.isAssignableFrom(m.getParameterTypes()[0])) {
                        result.put(name, new RpcClientUmap());
                    } else
                    if (Publisher.class.isAssignableFrom(m.getParameterTypes()[0])) {
                        result.put(name, new RpcClientSend());
                    } else {
                        throw new IllegalStateException("RsRpc annotated methods require a Function or Publisher as a parameter: " + m);
                    }
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
                    throw new IllegalStateException("Unsupported RsRpc annotated return type: " + m);
                }
            }
        }
        
        return result;
    }
    
    public static boolean dispatchServer(long streamId, Object action, RpcIOManager io, RpcStreamContext<?> ctx) {
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
        }
        return false;
    }
    
    public static Object dispatchClient(String name, Object action, Object[] args, RpcIOManager io) {
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
        }
        throw new IllegalStateException("Unsupported action class: " + action.getClass());
    }
    
    static final class RpcClientSend {
        
        public void send(String function, Publisher<?> values, RpcIOManager io) {
            long streamId = io.newStreamId();
            
            SendSubscriber s = new SendSubscriber(io, streamId);
            io.registerSubscription(streamId, s);
            io.sendNew(streamId, function);
            
            values.subscribe(s);
        }
        
        static final class SendSubscriber 
        extends DeferredSubscription
        implements Subscriber<Object>, Subscription {
            
            final RpcIOManager io;
            
            final long streamId;
            
            boolean done;
            
            volatile Subscription s;
            static final AtomicReferenceFieldUpdater<SendSubscriber, Subscription> S =
                    AtomicReferenceFieldUpdater.newUpdater(SendSubscriber.class, Subscription.class, "s");
            
            public SendSubscriber(RpcIOManager io, long streamId) {
                this.io = io;
                this.streamId = streamId;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                super.set(s);
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
                    UnsignalledExceptions.onErrorDropped(t);
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
    
    static final class RpcClientReceive {
        
        static final class RpcReceiveSubscription implements Subscription {
            final long streamId;
            
            final RpcIOManager io;
            
            public RpcReceiveSubscription(long streamId, RpcIOManager io) {
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

        public Publisher<?> receive(String function, RpcIOManager io) {
            return s -> {
                long streamId = io.newStreamId();
                io.registerSubscriber(streamId, s);
                io.sendNew(streamId, function);
                
                s.onSubscribe(new RpcReceiveSubscription(streamId, io));
            };
        }
        
    }
    
    static final class RpcClientMap {
        
        public Publisher<?> map(String function, Publisher<?> values, RpcIOManager io) {
            return s -> {
                long streamId = io.newStreamId();
                
                final AtomicInteger open = new AtomicInteger(2);
                
                Subscriber<Object> parent = new RpcMapSubscriber(streamId, open, io);
                
                io.registerSubscriber(streamId, parent);
                io.sendNew(streamId, function);
                
                s.onSubscribe(new Subscription() {

                    @Override
                    public void request(long n) {
                        if (SubscriptionHelper.validate(n)) {
                            io.sendRequested(streamId, n);
                        }
                    }

                    @Override
                    public void cancel() {
                        if (open.decrementAndGet() != 0) {
                            io.deregister(streamId);
                        }
                        io.sendCancel(streamId, "");
                    }
                    
                });
                
                values.subscribe(parent);
            };
        }
        
        static final class RpcMapSubscriber extends DeferredSubscription implements Subscriber<Object> {
            
            final long streamId;
            
            final AtomicInteger open;
            
            final RpcIOManager io;
            
            boolean done;
            
            public RpcMapSubscriber(long streamId, AtomicInteger open, RpcIOManager io) {
                this.streamId = streamId;
                this.open = open;
                this.io = io;
            }

            @Override
            public void onSubscribe(Subscription s) {
                super.set(s);
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
                    UnsignalledExceptions.onErrorDropped(t);
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
    }
    
    static final class RpcClientUmap {
        public void umap(String function, Function<Publisher<?>, Publisher<?>> mapper, RpcIOManager io) {

            long streamId = io.newStreamId();
            
            AtomicBoolean onceInner = new AtomicBoolean();
            
            RpcUmapReceiver receiver = new RpcUmapReceiver(streamId, io, onceInner);
            
            receiver.provider = new RpcUmapProvider(streamId, io, onceInner);
            
            io.registerSubscriber(streamId, receiver);
            
            io.sendNew(streamId, function);

            AtomicBoolean once = new AtomicBoolean();
            Publisher<Object> p = s -> {
                if (once.compareAndSet(false, true)) {
                    receiver.actual = s;
                    s.onSubscribe(receiver.s);
                } else {
                    EmptySubscription.error(s, new IllegalStateException("Only one subscriber allowed"));
                }
            };
            
            Publisher<?> u;
            
            try {
                u = mapper.apply(p);
            } catch (Throwable ex) {
                u = w -> {
                    EmptySubscription.error(w, ex);
                };
            }
            
            if (u == null) {
                u = w -> {
                    EmptySubscription.error(w, new NullPointerException("The umapper returned a null Publisher"));
                };
            }
            
            u.subscribe(receiver.provider);
        }
        
        static final class RpcUmapReceiver implements Subscriber<Object>, Subscription {
            final long streamId;
            
            final RpcIOManager io;
            
            final AtomicBoolean once;
            
            Subscriber<Object> actual;
            
            RpcUmapProvider provider;
            
            Subscription s;
            
            public RpcUmapReceiver(long streamId, RpcIOManager io, AtomicBoolean once) {
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
            final long streamId;
            
            final RpcIOManager io;
            
            final AtomicBoolean once;
            
            boolean done;
            
            public RpcUmapProvider(long streamId, RpcIOManager io, AtomicBoolean once) {
                this.streamId = streamId;
                this.io = io;
                this.once = once;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                set(s);
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
                    UnsignalledExceptions.onErrorDropped(t);
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
    
    static final class RpcServerSend {
        final Method m;
        
        final Object instance;
        
        public RpcServerSend(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean send(long streamId, RpcStreamContext<?> ctx, RpcIOManager io) {
            Publisher<?> output;
            try {
                output = (Publisher<?>)m.invoke(instance, ctx);
            } catch (Throwable ex) {
                UnsignalledExceptions.onErrorDropped(ex);
                
                io.sendError(streamId, ex);
                return true;
            }
            
            if (output == null) {
                io.sendError(streamId, new IllegalStateException("The service implementation returned a null Publisher"));
                return true;
            }

            ServerSendSubscriber parent = new ServerSendSubscriber(streamId, io);
            io.registerSubscriber(streamId, parent);
            
            output.subscribe(parent);
            
            return true;
        }
        
        static final class ServerSendSubscriber 
        extends DeferredSubscription implements Subscriber<Object> {
            final long streamId;
            
            final RpcIOManager io;
            
            boolean done;
            
            public ServerSendSubscriber(long streamId, RpcIOManager io) {
                this.streamId = streamId;
                this.io = io;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                set(s);
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
                    UnsignalledExceptions.onErrorDropped(t);
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
    
    static final class RpcServerReceive {
        final Method m;
        
        final Object instance;
        
        public RpcServerReceive(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean receive(long streamId, RpcStreamContext<?> ctx, RpcIOManager io) {
            
            ServerReceiveSubscriber parent = new ServerReceiveSubscriber(streamId, io);
            
            AtomicBoolean once = new AtomicBoolean();
            Publisher<?> p = s -> {
                if (once.compareAndSet(false, true)) {
                    parent.actual = s;
                    io.registerSubscriber(streamId, parent);
                    s.onSubscribe(parent);
                } else {
                    EmptySubscription.error(s, new IllegalStateException("This Publisher allows only a single subscriber"));
                }
            };
            
            try {
                m.invoke(instance, ctx, p);
            } catch (Throwable ex) {
                UnsignalledExceptions.onErrorDropped(ex);
                
                io.sendCancel(streamId, ex.toString());
            }
            return true;
        }
        
        static final class ServerReceiveSubscriber implements Subscriber<Object>, Subscription {
            final long streamId;
            
            final RpcIOManager io;
            
            Subscriber<Object> actual;

            public ServerReceiveSubscriber(long streamId, RpcIOManager io) {
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
    
    static final class RpcServerMap {
        final Method m;
        
        final Object instance;
        
        public RpcServerMap(Method m, Object instance) {
            this.m = m;
            this.instance = instance;
        }
        
        public boolean map(long streamId, RpcStreamContext<?> ctx, RpcIOManager io) {
            AtomicInteger innerOnce = new AtomicInteger(2);

            ServerMapSubscriber parent = new ServerMapSubscriber(streamId, io, innerOnce);
            ServerSendSubscriber sender = new ServerSendSubscriber(streamId, io, innerOnce);
            parent.sender = sender;

            io.registerSubscriber(streamId, parent);

            AtomicBoolean once = new AtomicBoolean();
            
            Publisher<?> p = s -> {
                if (once.compareAndSet(false, true)) {
                    parent.actual = s;
                    s.onSubscribe(parent.s);
                } else {
                    EmptySubscription.error(s, new IllegalStateException("This Publisher allows only a single subscriber"));
                }
            };
            
            Publisher<?> u;
            try {
                u = (Publisher<?>)m.invoke(instance, ctx, p);
            } catch (Throwable ex) {
                UnsignalledExceptions.onErrorDropped(ex);
                
                u = s -> EmptySubscription.error(s, ex);
            }
            
            if (u == null) {
                u = s -> EmptySubscription.error(s, new NullPointerException("The service implementation returned a null Publisher"));
            }
            
            u.subscribe(sender);
            
            return true;
        }
        
        static final class ServerMapSubscriber implements Subscriber<Object>, Subscription {
            final long streamId;
            
            final RpcIOManager io;
            
            final AtomicInteger once;
            
            final Subscription s;
            
            Subscriber<Object> actual;
            
            ServerSendSubscriber sender;

            public ServerMapSubscriber(long streamId, RpcIOManager io, AtomicInteger once) {
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
            final long streamId;
            
            final RpcIOManager io;
            
            final AtomicInteger once;
            
            boolean done;
            
            public ServerSendSubscriber(long streamId, RpcIOManager io, AtomicInteger once) {
                this.streamId = streamId;
                this.io = io;
                this.once = once;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                set(s);
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
                    UnsignalledExceptions.onErrorDropped(t);
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
