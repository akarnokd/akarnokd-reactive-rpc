package hu.akarnokd.reactiverpc;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.*;

import rsc.scheduler.Scheduler.Worker;
import rsc.util.UnsignalledExceptions;

public class RpcIOManager implements RsRpcProtocol.RsRpcReceive {
    
    @FunctionalInterface
    public interface OnNewStream {
        boolean onNew(long streamId, String function, RpcIOManager manager);
    }
    
    final Worker reader;
    
    final Worker writer;
    
    final ConcurrentMap<Long, Object> streams;

    final InputStream in;
    
    final OutputStream out;
    
    final OnNewStream onNew;
    
    final AtomicLong streamIds;
    
    public RpcIOManager(Worker reader, InputStream in, 
            Worker writer, OutputStream out,
            OnNewStream onNew,
            boolean server) {
        this.reader = reader;
        this.writer = writer;
        this.in = in;
        this.out = out;
        this.onNew = onNew;
        this.streams = new ConcurrentHashMap<>();
        this.streamIds = new AtomicLong((server ? Long.MIN_VALUE : 0) + 1);
    }
    
    public void start() {
        reader.schedule(this::handleRead);
    }
    
    void handleRead() {
        while (!Thread.currentThread().isInterrupted()) {
            RsRpcProtocol.receive(in, this);
        }
    }
    
    public long newStreamId() {
        return streamIds.getAndIncrement();
    }
    
    public void registerSubscription(long streamId, Subscription s) {
        if (streams.putIfAbsent(streamId, s) != null) {
            throw new IllegalStateException("StreamID " + streamId + " already registered");
        }
    }
    
    public void registerSubscriber(long streamId, Subscriber<?> s) {
        if (streams.putIfAbsent(streamId, s) != null) {
            throw new IllegalStateException("StreamID " + streamId + " already registered");
        }
    }
    
    @Override
    public void onNew(long streamId, String function) {
        if (!onNew.onNew(streamId, function, this)) {
            writer.schedule(() -> {
                RsRpcProtocol.cancel(out, streamId, "New stream(" + function + ") rejected");
            });
        }
    }

    @Override
    public void onCancel(long streamId, String reason) {
        Object remove = streams.get(streamId);
        if (remove != null) {
            // TODO log reason?
            if (remove instanceof Subscription) {
                Subscription s = (Subscription) remove;
                s.cancel();
            } else {
                UnsignalledExceptions.onErrorDropped(new IllegalStateException("Stream " + streamId + " directed at wrong receiver: " + remove.getClass()));
            }
        }
    }

    @Override
    public void onNext(long streamId, byte[] payload, int read) {
        
        Object local = streams.get(streamId);
        if (local instanceof Subscriber) {
            @SuppressWarnings("unchecked")
            Subscriber<Object> s = (Subscriber<Object>)local;
            
            if (payload.length != read) {
                s.onError(new IOException("Partial value received: expected = " + payload.length + ", actual = " + read));
            } else {
                Object o;
                
                try {
                    ByteArrayInputStream bin = new ByteArrayInputStream(payload);
                    ObjectInputStream oin = new ObjectInputStream(bin);
                    o = oin.readObject();
                } catch (IOException | ClassNotFoundException ex) {
                    sendCancel(streamId, ex.toString());
                    s.onError(ex);
                    return;
                }
                
                try {
                    s.onNext(o);
                } catch (Throwable ex) {
                    sendCancel(streamId, ex.toString());
                    s.onError(ex);
                }
            }
        }
    }

    @Override
    public void onError(long streamId, String reason) {
        Object local = streams.get(streamId);
        if (local instanceof Subscriber) {
            Subscriber<?> s = (Subscriber<?>) local;
            
            s.onError(new Exception(reason));
            return;
        }
        
        UnsignalledExceptions.onErrorDropped(new Exception(reason));
    }

    @Override
    public void onComplete(long streamId) {
        Object local = streams.get(streamId);
        if (local instanceof Subscriber) {
            Subscriber<?> s = (Subscriber<?>) local;
            
            s.onComplete();
            return;
        }
    }

    @Override
    public void onRequested(long streamId, long requested) {
        Object remote = streams.get(streamId);
        if (remote instanceof Subscription) {
            Subscription s = (Subscription) remote;
            
            s.request(requested);
            return;
        }
    }

    @Override
    public void onUnknown(int type, int flags, long streamId, byte[] payload, int read) {
        // TODO Auto-generated method stub
        
    }

    static void flush(OutputStream out) {
        try {
            out.flush();
        } catch (IOException ex) {
            UnsignalledExceptions.onErrorDropped(ex);
        }
    }
    
    public void sendNew(long streamId, String function) {
        writer.schedule(() -> {
            RsRpcProtocol.open(out, streamId, function);
            flush(out);
        });
    }
    
    public void sendNext(long streamId, Object o) throws IOException {
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        
        try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
            oout.writeObject(o);
        }

        byte[] payload = bout.toByteArray();
        
        writer.schedule(() -> {
            RsRpcProtocol.next(out, streamId, payload);
            flush(out);
        });
    }

    public void sendError(long streamId, Throwable e) {
        writer.schedule(() -> {
            RsRpcProtocol.error(out, streamId, e);
            flush(out);
        });
    }
    
    public void sendComplete(long streamId) {
        writer.schedule(() -> {
            RsRpcProtocol.complete(out, streamId);
            flush(out);
        });
    }
    
    public void sendCancel(long streamId, String reason) {
        writer.schedule(() -> {
            RsRpcProtocol.cancel(out, streamId, reason);
            flush(out);
        });
    }

    public void sendRequested(long streamId, long requested) {
        writer.schedule(() -> {
            RsRpcProtocol.request(out, streamId, requested);
            flush(out);
        });
    }

    public void deregister(long streamId) {
        if (streams.remove(streamId) == null) {
            // TODO
        }
    }

}
