package hu.akarnokd.reactive.pc;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import io.reactivex.plugins.RxJavaPlugins;

/**
 * Allows registering Subscribers and Subscriptions for incoming messages
 * and allows sending messages.
 */
public final class RsAPIManager implements RsPcReceive, RsPcSend {

    static volatile boolean logMessages = false;
    
    final ConcurrentMap<Long, Subscriber<?>> subscribers;

    final ConcurrentMap<Long, Subscription> subscriptions;

    RsPcSend send;
    
    final AtomicLong streamIds;
    
    final boolean server;
    
    final RsPcNewStream onNew;
    
    final Runnable onTerminate;
    
    final AtomicBoolean terminateOnce;
    
    public RsAPIManager(boolean server, RsPcNewStream onNew, Runnable onTerminate) {
        this.server = server;
        this.onNew = onNew;
        this.onTerminate = onTerminate;
        this.terminateOnce = new AtomicBoolean();
        this.subscribers = new ConcurrentHashMap<>();
        this.subscriptions = new ConcurrentHashMap<>();
        this.streamIds = new AtomicLong((server ? Long.MIN_VALUE : 0) + 1);
    }
    
    public long newStreamId() {
        return streamIds.getAndIncrement();
    }

    public void registerSubscription(long streamId, Subscription s) {
        if (subscriptions.putIfAbsent(streamId, s) != null) {
            throw new IllegalStateException("StreamID " + streamId + " already registered");
        }
    }
    
    public void registerSubscriber(long streamId, Subscriber<?> s) {
        if (subscribers.putIfAbsent(streamId, s) != null) {
            throw new IllegalStateException("StreamID " + streamId + " already registered");
        }
    }
    
    public void deregister(long streamId) {
        subscribers.remove(streamId);
        subscriptions.remove(streamId);
    }
    
    @Override
    public void onNew(long streamId, String function) {
        if (logMessages) {
            System.out.printf("%s/onNew/%d/%s%n", server ? "server" : "client", streamId, function);
        }
        if (!onNew.onNew(streamId, function, this)) {
            if (logMessages) {
                System.out.printf("%s/onNew/%d/%s%n", server ? "server" : "client", streamId, "New stream(" + function + ") rejected");
            }
            send.sendCancel(streamId, "New stream(" + function + ") rejected");
        }
    }

    @Override
    public void onNext(long streamId, Object o) {

        if (logMessages) {
            System.out.printf("%s/onNext/%d/value=%s%n", server ? "server" : "client", streamId, o);
        }
        @SuppressWarnings("unchecked")
        Subscriber<Object> local = (Subscriber<Object>)subscribers.get(streamId);
        if (local != null) {
            try {
                local.onNext(o);
            } catch (Throwable ex) {
                send.sendCancel(streamId, ex.toString());
                local.onError(ex);
            }
        }
    }

    @Override
    public void onError(long streamId, String reason) {
        onError(streamId, new Exception(reason));
    }

    @Override
    public void onError(long streamId, Throwable e) {
        if (logMessages) {
            System.out.printf("%s/onError/%d/%s%n", server ? "server" : "client", streamId, e);
        }
        if (streamId > 0) {
            Subscriber<?> local = subscribers.get(streamId);
            if (local != null) {
                local.onError(e);
                return;
            }
        } else
        if (streamId < 0) {
            if (terminateOnce.compareAndSet(false, true)) {
                onTerminate.run();
            }
            if (send.isClosed()) {
                return;
            }
        }
        RxJavaPlugins.onError(e);
    }

    @Override
    public void onComplete(long streamId) {
        Subscriber<?> local = subscribers.get(streamId);
        if (local != null) {
            local.onComplete();
            return;
        }
    }

    @Override
    public void onCancel(long streamId, String reason) {
        if (logMessages) {
            System.out.printf("%s/onCancel/%d/%s%n", server ? "server" : "client", streamId, reason);
        }
        Subscription remove = subscriptions.get(streamId);
        if (remove != null) {
            remove.cancel();
        }
    }

    @Override
    public void onRequested(long streamId, long n) {
        if (logMessages) {
            System.out.printf("%s/onRequested/%d/%d%n", server ? "server" : "client", streamId, n);
        }
        Subscription remote = subscriptions.get(streamId);
        if (remote != null) {
            
            remote.request(n);
            return;
        }
    }

    @Override
    public void sendNew(long streamId, String function) {
        send.sendNew(streamId, function);
    }

    @Override
    public void sendCancel(long streamId, String reason) {
        send.sendCancel(streamId, reason);
    }

    @Override
    public void sendNext(long streamId, Object o) throws IOException {
        send.sendNext(streamId, o);
    }

    @Override
    public void sendError(long streamId, Throwable e) {
        send.sendError(streamId, e);
    }

    @Override
    public void sendComplete(long streamId) {
        send.sendComplete(streamId);
    }

    @Override
    public void sendRequested(long streamId, long n) {
        send.sendRequested(streamId, n);
    }

    @Override
    public boolean isClosed() {
        return send.isClosed();
    }
    
    public void setSend(RsPcSend send) {
        this.send = send;
    }
}
