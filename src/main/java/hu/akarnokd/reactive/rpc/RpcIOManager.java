package hu.akarnokd.reactive.rpc;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;

import hu.akarnokd.reactive.pc.*;
import io.reactivex.Scheduler.Worker;
import io.reactivex.plugins.RxJavaPlugins;

final class RpcIOManager implements RsRpcProtocol.RsRpcReceive, RsPcSend {
    
    final Worker reader;
    
    final Worker writer;
    
    final InputStream in;
    
    final OutputStream out;
    
    volatile boolean closed;
    
    final AtomicBoolean terminateOnce;
    
    final boolean server;
    
    final byte[] readBuffer;
    
    final byte[] writeBuffer;
    
    final RsPcReceive receive;
    
    public RpcIOManager(Worker reader, InputStream in, 
            Worker writer, OutputStream out,
            RsPcReceive receive,
            boolean server) {
        this.reader = reader;
        this.writer = writer;
        this.in = in;
        this.out = out;
        this.terminateOnce = new AtomicBoolean();
        this.server = server;
        this.receive = receive;
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
            RxJavaPlugins.onError(e);
        }
        
        try {
            out.close();
        } catch (IOException e) {
            RxJavaPlugins.onError(e);
        }
        
        reader.dispose();
        
        writer.dispose();
    }
    
    void handleRead() {
        while (!Thread.currentThread().isInterrupted() && !closed) {
            if (!RsRpcProtocol.receive(in, readBuffer, this)) {
                break;
            }
        }
    }
    
    @Override
    public void onNew(long streamId, String function) {
        receive.onNew(streamId, function);
    }

    @Override
    public void onCancel(long streamId, String reason) {
        receive.onCancel(streamId, reason);
    }

    @Override
    public void onNext(long streamId, int flags, byte[] payload, int count, int read) {
        if (count != read) {
            receive.onError(streamId, new IOException("Partial value received: expected = " + payload.length + ", actual = " + read));
        } else {
            Object o;
            
            try {
                o = decode(flags, payload, count);
            } catch (IOException | ClassNotFoundException ex) {
                sendCancel(streamId, ex.toString());
                receive.onError(streamId, ex);
                return;
            }
            
            try {
                receive.onNext(streamId, o);
            } catch (Throwable ex) {
                sendCancel(streamId, ex.toString());
                receive.onError(streamId, ex);
            }
        }
    }
    
    Object decode(int flags, byte[] payload, int len) throws IOException, ClassNotFoundException {
        if (flags == RsRpcProtocol.PAYLOAD_INT) {
            int v = (payload[0] & 0xFF)
                    | ((payload[1] & 0xFF) << 8)
                    | ((payload[2] & 0xFF) << 16)
                    | ((payload[3] & 0xFF) << 24)
                    ;
            return v;
        } else
        if (flags == RsRpcProtocol.PAYLOAD_LONG) {
            long v = (payload[0] & 0xFFL)
                    | ((payload[1] & 0xFFL) << 8)
                    | ((payload[2] & 0xFFL) << 16)
                    | ((payload[3] & 0xFFL) << 24)
                    | ((payload[4] & 0xFFL) << 32)
                    | ((payload[5] & 0xFFL) << 40)
                    | ((payload[6] & 0xFFL) << 48)
                    | ((payload[7] & 0xFFL) << 56)
                    ;
            return v;
        } else
        if (flags == RsRpcProtocol.PAYLOAD_STRING) {
            return RpcHelper.readUtf8(payload, 0, len);
        } else
        if (flags == RsRpcProtocol.PAYLOAD_BYTES) {
            if (payload == readBuffer) {
                byte[] r = new byte[len];
                System.arraycopy(payload, 0, r, 0, len);
                return r;
            }
            return payload;
        }
        
        ByteArrayInputStream bin = new ByteArrayInputStream(payload);
        ObjectInputStream oin = new ObjectInputStream(bin);
        return oin.readObject();
    }
    
    byte[] encode(Object o, IntConsumer flagOut) throws IOException {
        
        if (o instanceof Integer) {
            flagOut.accept(RsRpcProtocol.PAYLOAD_INT);
            byte[] r = new byte[4];
            int v = (Integer)o;
            r[0] = (byte)(v & 0xFF);
            r[1] = (byte)((v >> 8) & 0xFF);
            r[2] = (byte)((v >> 16) & 0xFF);
            r[3] = (byte)((v >> 24) & 0xFF);
            return r;
        } else
        if (o instanceof Long) {
            flagOut.accept(RsRpcProtocol.PAYLOAD_LONG);
            byte[] r = new byte[8];
            long v = (Long)o;
            r[0] = (byte)(v & 0xFF);
            r[1] = (byte)((v >> 8) & 0xFF);
            r[2] = (byte)((v >> 16) & 0xFF);
            r[3] = (byte)((v >> 24) & 0xFF);
            r[4] = (byte)((v >> 32) & 0xFF);
            r[5] = (byte)((v >> 40) & 0xFF);
            r[6] = (byte)((v >> 48) & 0xFF);
            r[7] = (byte)((v >> 56) & 0xFF);
            return r;
        } else
        if (o instanceof String) {
            flagOut.accept(RsRpcProtocol.PAYLOAD_STRING);
            return RsRpcProtocol.utf8((String)o);
        } else
        if (o instanceof byte[]) {
            flagOut.accept(RsRpcProtocol.PAYLOAD_BYTES);
            return ((byte[])o).clone();
        }
        
        flagOut.accept(RsRpcProtocol.PAYLOAD_OBJECT);
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        
        try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
            oout.writeObject(o);
        }

        return bout.toByteArray();
    }

    @Override
    public void onError(long streamId, String reason) {
        receive.onError(streamId, reason);
    }

    @Override
    public void onComplete(long streamId) {
        receive.onComplete(streamId);
    }

    @Override
    public void onRequested(long streamId, long requested) {
        receive.onRequested(streamId, requested);
    }

    @Override
    public void onUnknown(int type, int flags, long streamId, byte[] payload, int read) {
//        if (logMessages) {
//            System.out.printf("%s/onUnknown/%d/len=%d/%d%n", server ? "server" : "client", streamId, payload.length, read);
//        }
        // TODO Auto-generated method stub
        
    }

    static void flush(OutputStream out) {
        try {
            out.flush();
        } catch (IOException ex) {
            RxJavaPlugins.onError(ex);
        }
    }
    
    @Override
    public void sendNew(long streamId, String function) {
        writer.schedule(() -> {
            RsRpcProtocol.open(out, streamId, function, writeBuffer);
            flush(out);
        });
    }
    
    @Override
    public void sendNext(long streamId, Object o) throws IOException {
        
        OnNextTask task = new OnNextTask(streamId, out, writeBuffer, server, null);
        
        task.payload = encode(o, task);
        
        writer.schedule(task);
    }
    
    static final class OnNextTask implements Runnable, IntConsumer {
        final long streamId;
        final OutputStream out;
        final byte[] writeBuffer;
        final boolean server;
        final Object object;
        byte[] payload;
        int flags;

        public OnNextTask(long streamId, OutputStream out, byte[] writeBuffer, boolean server, Object object) {
            this.streamId = streamId;
            this.out = out;
            this.server = server;
            this.writeBuffer = writeBuffer;
            this.object = object;
        }
        
        @Override
        public void run() {
            RsRpcProtocol.next(out, streamId, flags, payload, writeBuffer);
            flush(out);
        }
        
        @Override
        public void accept(int value) {
            this.flags = value;
        }
    }

    @Override
    public void sendError(long streamId, Throwable e) {
        writer.schedule(() -> {
            RsRpcProtocol.error(out, streamId, e, writeBuffer);
            flush(out);
        });
    }
    
    @Override
    public void sendComplete(long streamId) {
        writer.schedule(() -> {
            RsRpcProtocol.complete(out, streamId, writeBuffer);
            flush(out);
        });
    }
    
    @Override
    public void sendCancel(long streamId, String reason) {
        writer.schedule(() -> {
            RsRpcProtocol.cancel(out, streamId, reason, writeBuffer);
            flush(out);
        });
    }

    @Override
    public void sendRequested(long streamId, long requested) {
        writer.schedule(() -> {
            RsRpcProtocol.request(out, streamId, requested, writeBuffer);
            flush(out);
        });
    }
    
    @Override
    public boolean isClosed() {
        return closed;
    }
}
