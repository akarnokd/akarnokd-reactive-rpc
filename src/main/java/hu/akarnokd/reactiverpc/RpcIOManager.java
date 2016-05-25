package hu.akarnokd.reactiverpc;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import rsc.scheduler.Scheduler.Worker;
import rsc.util.UnsignalledExceptions;

final class RpcIOManager implements RsRpcProtocol.RsRpcReceive {
    
    static volatile boolean logMessages = false;
    
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
    
    volatile boolean closed;
    
    final AtomicBoolean terminateOnce;
    
    final Runnable onTerminate;
    
    final boolean server;
    
    final byte[] readBuffer;
    
    final byte[] writeBuffer;
    
    public RpcIOManager(Worker reader, InputStream in, 
            Worker writer, OutputStream out,
            OnNewStream onNew,
            Runnable onTerminate,
            boolean server) {
        this.reader = reader;
        this.writer = writer;
        this.in = in;
        this.out = out;
        this.onNew = onNew;
        this.terminateOnce = new AtomicBoolean();
        this.onTerminate = onTerminate;
        this.streams = new ConcurrentHashMap<>();
        this.streamIds = new AtomicLong((server ? Long.MIN_VALUE : 0) + 1);
        this.server = server;
        this.readBuffer = new byte[16];
        this.writeBuffer = new byte[32];
    }
    
    public void start() {
        reader.schedule(this::handleRead);
    }
    
    public void close() {
        this.closed = true;
        
        try {
            in.close();
        } catch (IOException e) {
            UnsignalledExceptions.onErrorDropped(e);
        }
        
        try {
            out.close();
        } catch (IOException e) {
            UnsignalledExceptions.onErrorDropped(e);
        }
        
        reader.shutdown();
        
        writer.shutdown();
    }
    
    void handleRead() {
        while (!Thread.currentThread().isInterrupted() && !closed) {
            if (!RsRpcProtocol.receive(in, readBuffer, this)) {
                break;
            }
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
        if (logMessages) {
            System.out.printf("%s/onNew/%d/%s%n", server ? "server" : "client", streamId, function);
        }
        if (!onNew.onNew(streamId, function, this)) {
            writer.schedule(() -> {
                if (logMessages) {
                    System.out.printf("%s/onNew/%d/%s%n", server ? "server" : "client", streamId, "New stream(" + function + ") rejected");
                }
                RsRpcProtocol.cancel(out, streamId, "New stream(" + function + ") rejected", writeBuffer);
            });
        }
    }

    @Override
    public void onCancel(long streamId, String reason) {
        if (logMessages) {
            System.out.printf("%s/onCancel/%d/%s%n", server ? "server" : "client", streamId, reason);
        }
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
    public void onNext(long streamId, byte[] payload, int count, int read) {
        if (logMessages) {
            System.out.printf("%s/onNext/%d/len=%d/%d%n", server ? "server" : "client", streamId, payload.length, read);
        }
        Object local = streams.get(streamId);
        if (local instanceof Subscriber) {
            @SuppressWarnings("unchecked")
            Subscriber<Object> s = (Subscriber<Object>)local;
            
            if (count != read) {
                s.onError(new IOException("Partial value received: expected = " + payload.length + ", actual = " + read));
            } else {
                Object o;
                
                try {
                    o = decode(payload);
                } catch (IOException | ClassNotFoundException ex) {
                    sendCancel(streamId, ex.toString());
                    s.onError(ex);
                    return;
                }
                
                if (logMessages) {
                    System.out.printf("%s/onNext/%d/value=%s%n", server ? "server" : "client", streamId, o);
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
    
    static final byte PAYLOAD_OBJECT = 0;
    static final byte PAYLOAD_INT = 1;
    static final byte PAYLOAD_LONG = 2;
    static final byte PAYLOAD_STRING = 3;
    
    Object decode(byte[] payload) throws IOException, ClassNotFoundException {
        byte type = payload[0];
        if (type == PAYLOAD_INT) {
            int v = (payload[1] & 0xFF)
                    | ((payload[2] & 0xFF) << 8)
                    | ((payload[3] & 0xFF) << 16)
                    | ((payload[4] & 0xFF) << 24)
                    ;
            return v;
        } else
        if (type == PAYLOAD_LONG) {
            long v = (payload[1] & 0xFFL)
                    | ((payload[2] & 0xFFL) << 8)
                    | ((payload[3] & 0xFFL) << 16)
                    | ((payload[4] & 0xFFL) << 24)
                    | ((payload[5] & 0xFFL) << 32)
                    | ((payload[6] & 0xFFL) << 40)
                    | ((payload[7] & 0xFFL) << 48)
                    | ((payload[8] & 0xFFL) << 56)
                    ;
            return v;
        } else
        if (type == PAYLOAD_STRING) {
            return RpcHelper.readUtf8(payload, 1, payload.length - 1);
        }
        
        ByteArrayInputStream bin = new ByteArrayInputStream(payload);
        bin.skip(1);
        ObjectInputStream oin = new ObjectInputStream(bin);
        return oin.readObject();
    }
    
    byte[] encode(Object o) throws IOException {
        
        if (o instanceof Integer) {
            byte[] r = new byte[5];
            r[0] = PAYLOAD_INT;
            int v = (Integer)o;
            r[1] = (byte)(v & 0xFF);
            r[2] = (byte)((v >> 8) & 0xFF);
            r[3] = (byte)((v >> 16) & 0xFF);
            r[4] = (byte)((v >> 24) & 0xFF);
            return r;
        } else
        if (o instanceof Long) {
            byte[] r = new byte[9];
            r[0] = PAYLOAD_LONG;
            long v = (Long)o;
            r[1] = (byte)(v & 0xFF);
            r[2] = (byte)((v >> 8) & 0xFF);
            r[3] = (byte)((v >> 16) & 0xFF);
            r[4] = (byte)((v >> 24) & 0xFF);
            r[5] = (byte)((v >> 32) & 0xFF);
            r[6] = (byte)((v >> 40) & 0xFF);
            r[7] = (byte)((v >> 48) & 0xFF);
            r[8] = (byte)((v >> 56) & 0xFF);
            return r;
        } else
        if (o instanceof String) {
            byte[] text = RsRpcProtocol.utf8((String)o);
            byte[] r = new byte[text.length + 1];
            r[0] = PAYLOAD_STRING;
            System.arraycopy(text, 0, r, 1, text.length);
            return r;
        }
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        bout.write(PAYLOAD_OBJECT);
        
        try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
            oout.writeObject(o);
        }

        return bout.toByteArray();
    }

    @Override
    public void onError(long streamId, String reason) {
        if (logMessages) {
            System.out.printf("%s/onError/%d/%s%n", server ? "server" : "client", streamId, reason);
        }
        if (streamId > 0) {
            Object local = streams.get(streamId);
            if (local instanceof Subscriber) {
                Subscriber<?> s = (Subscriber<?>) local;
                
                s.onError(new Exception(reason));
                return;
            }
        }
        if (streamId < 0) {
            if (terminateOnce.compareAndSet(false, true)) {
                onTerminate.run();
            }
            if (closed) {
                return;
            }
        }
        UnsignalledExceptions.onErrorDropped(new Exception(reason));
    }

    @Override
    public void onComplete(long streamId) {
        if (logMessages) {
            System.out.printf("%s/onComplete/%d%n", server ? "server" : "client", streamId);
        }
        Object local = streams.get(streamId);
        if (local instanceof Subscriber) {
            Subscriber<?> s = (Subscriber<?>) local;
            
            s.onComplete();
            return;
        }
    }

    @Override
    public void onRequested(long streamId, long requested) {
        if (logMessages) {
            System.out.printf("%s/onRequested/%d/%d%n", server ? "server" : "client", streamId, requested);
        }
        Object remote = streams.get(streamId);
        if (remote instanceof Subscription) {
            Subscription s = (Subscription) remote;
            
            s.request(requested);
            return;
        }
    }

    @Override
    public void onUnknown(int type, int flags, long streamId, byte[] payload, int read) {
        if (logMessages) {
            System.out.printf("%s/onUnknown/%d/len=%d/%d%n", server ? "server" : "client", streamId, payload.length, read);
        }
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
            if (logMessages) {
                System.out.printf("%s/sendNew/%d/%s%n", server ? "server" : "client", streamId, function);
            }
            RsRpcProtocol.open(out, streamId, function, writeBuffer);
            flush(out);
        });
    }
    
    public void sendNext(long streamId, Object o) throws IOException {
        
        byte[] payload = encode(o);
        
        writer.schedule(() -> {
            if (logMessages) {
                System.out.printf("%s/sendNext/%d/%s%n", server ? "server" : "client", streamId, o);
            }
            RsRpcProtocol.next(out, streamId, payload, writeBuffer);
            flush(out);
        });
    }

    public void sendError(long streamId, Throwable e) {
        writer.schedule(() -> {
            if (logMessages) {
                e.printStackTrace();
                System.out.printf("%s/sendError/%d/%s%n", server ? "server" : "client", streamId, e);
            }
            RsRpcProtocol.error(out, streamId, e, writeBuffer);
            flush(out);
        });
    }
    
    public void sendComplete(long streamId) {
        writer.schedule(() -> {
            if (logMessages) {
                System.out.printf("%s/sendComplete/%d%n", server ? "server" : "client", streamId);
            }
            RsRpcProtocol.complete(out, streamId, writeBuffer);
            flush(out);
        });
    }
    
    public void sendCancel(long streamId, String reason) {
        writer.schedule(() -> {
            if (logMessages) {
                System.out.printf("%s/sendCancel/%d/%s%n", server ? "server" : "client", streamId, reason);
            }
            RsRpcProtocol.cancel(out, streamId, reason, writeBuffer);
            flush(out);
        });
    }

    public void sendRequested(long streamId, long requested) {
        writer.schedule(() -> {
            if (logMessages) {
                System.out.printf("%s/sendRequested/%d/%d%n", server ? "server" : "client", streamId, requested);
            }
            RsRpcProtocol.request(out, streamId, requested, writeBuffer);
            flush(out);
        });
    }

    public void deregister(long streamId) {
        if (streams.remove(streamId) == null) {
            // TODO
        }
    }

}
