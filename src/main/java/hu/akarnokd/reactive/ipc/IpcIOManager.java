package hu.akarnokd.reactive.ipc;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import rsc.scheduler.Scheduler;
import rsc.scheduler.Scheduler.Worker;
import rsc.util.UnsignalledExceptions;

public final class IpcIOManager implements Closeable, IpcSend {

    IpcFileInterop input;
    
    IpcFileInterop output;
    
    /**
     * Offset 128 and 256 contain the wip counters for the client -&lt; server work
     * and server -&lt; client work to be done. 
     */
    IpcFileInterop wip;
    
    /** Signals more work towards the server. */
    static final int CLIENT_TO_SERVER_WIP = 128;
    /** Signals more work towards the client. */
    static final int SERVER_TO_CLIENT_WIP = 256;
    
    InputStream in;
    
    OutputStream out;
    
    Worker dataReader;
    
    Worker dataWriter;
    
    Worker signalReader;
    
    Worker signalWriter;
    
    byte[] iobuffer;
    
    volatile boolean closed;
    
    int readIndex;
    
    int writeIndex;

    final String fileNameBase;

    final IpcReceive receive;
    
    final int size = 16 * 1024;
    
    int fileIndex;

    final boolean server;
    
    static final byte[] EMPTY = new byte[0];
    
    public IpcIOManager(Scheduler scheduler, Socket socket, String fileNameBase, 
            int maxSize, IpcReceive receive, boolean server) throws IOException {
        
        this.fileNameBase = fileNameBase;
        
        this.receive = receive;
        this.server = server;

        in = socket.getInputStream();
        
        out = socket.getOutputStream();
        
        dataReader = scheduler.createWorker();
        
        dataWriter = scheduler.createWorker();
        
        signalReader = scheduler.createWorker();
        
        signalWriter = scheduler.createWorker();
        
        wip = new IpcFileInterop(fileNameBase + "-wip.dat", 384);
        
        if (server) {
            input = new IpcFileInterop(fileNameBase + "-server-0.dat", size);
    
            output = new IpcFileInterop(fileNameBase + "-client-0.dat", size);
        } else {
            input = new IpcFileInterop(fileNameBase + "-client-0.dat", size);
    
            output = new IpcFileInterop(fileNameBase + "-server-0.dat", size);
        }
        
        iobuffer = new byte[1];
    }
    
    public void start() {
        signalReader.schedule(this::readLoop);
    }
    
    void readLoop() {
        try {
            while (!Thread.currentThread().isInterrupted() && !closed) {
                if (in.read(iobuffer) < 0) {
                    break;
                }
                dataReader.schedule(this::dataReadLoop);
            }
        } catch (IOException ex) {
            if (!closed) {
                UnsignalledExceptions.onErrorDropped(ex);
            }
        }
    }
    
    void dataReadLoop() {
        int missed = 1;
        
        int idx = readIndex;
        IpcFileInterop inp = input;
        IpcFileInterop wip = this.wip;
        IpcReceive receive = this.receive;
        
        int wipOffset = server ? CLIENT_TO_SERVER_WIP : SERVER_TO_CLIENT_WIP;
        
        for (;;) {
            
            for (;;) {
                int len = inp.getIntVolatile(idx);
                if (len == 0) {
                    break;
                }
                
                if (dispatch(inp, idx, len, receive)) {
                    idx = 0;
                    inp = input;
                } else {
                    idx += len;
                    // messages are padded on a 4 byte boundary
                    int pad = (len & 3);
                    if (pad != 0) {
                        len += 4 - pad;
                    }
                }
            }
            
            int u = wip.getIntVolatile(wipOffset);
            if (u == missed) {
                readIndex = idx;
                missed = wip.addAndGetInt(128, -missed);
                if (missed == 0) {
                    break;
                }
            } else {
                missed = u;
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        
        closed = true;
        
        dataReader.shutdown();
        
        dataWriter.shutdown();
        
        signalReader.shutdown();
        
        signalWriter.shutdown();
        
        wip.close();
        
        input.close();
        
        output.close();
    }

    boolean dispatch(IpcFileInterop inp, int offset, int len, IpcReceive receive) {
        int typeAndFlags = inp.getInt(offset + 4);
        byte type = (byte)(typeAndFlags & 0xFF);
        int flags = typeAndFlags >> 8;
        long streamId = inp.getLong(offset + 8);
        
        switch (type) {
        case IpcReceive.TYPE_NEW: {
            if (len > 16) {
                receive.onNew(streamId, readUtf8(inp, offset + 16, offset + len));
            } else {
                receive.onNew(streamId, "");
            }
            return false;
        }
        case IpcReceive.TYPE_NEXT: {
            switch (flags) {
            case IpcReceive.PAYLOAD_INT: {
                receive.onNext(streamId, inp.getInt(offset + 16));
                break;
            }
            case IpcReceive.PAYLOAD_LONG: {
                receive.onNext(streamId, inp.getLong(offset + 16));
                break;
            }
            case IpcReceive.PAYLOAD_STRING: {
                receive.onNext(streamId, readUtf8(inp, offset + 16, offset + len));
                break;
            }
            case IpcReceive.PAYLOAD_BYTES: {
                byte[] bytes = new byte[len - 16];
                inp.get(offset + 16, bytes);
                receive.onNext(streamId, bytes);
                break;
            }
            default: {
                inp.setStreamOffset(offset + 16);
                
                try {
                    ObjectInputStream oin = new ObjectInputStream(inp);
                    Object o = oin.readObject();
                    receive.onNext(streamId, o);
                } catch (Throwable ex) {
                    receive.onError(streamId, ex);
                }
            }
            }
            return false;
        }
        case IpcReceive.TYPE_ERROR: {
            if (len > 16) {
                receive.onError(streamId, readUtf8(inp, offset + 16, offset + len));
            } else {
                receive.onError(streamId, "");
            }
            return false;
        }
        case IpcReceive.TYPE_COMPLETE: {
            receive.onComplete(streamId);
            return false;
        }
        case IpcReceive.TYPE_CANCEL: {
            if (len > 16) {
                receive.onCancel(streamId, readUtf8(inp, offset + 16, offset + len));
            } else {
                receive.onCancel(streamId, "");
            }
            return false;
        }
        case IpcReceive.TYPE_REQUEST: {
            
            long r;
            
            if (len > 16) {
                r = inp.getLong(offset + 16);
            } else {
                r = flags & 0xFFFFFF;
            }
            receive.onRequested(streamId, r);
            return false;
        }
        case IpcReceive.TYPE_SWITCH: {
            
            try {
                inp.closeFile();
                inp.delete();
                
                if (server) {
                    inp = new IpcFileInterop(fileNameBase + "-server-" + flags + ".dat", size);
                } else {
                    inp = new IpcFileInterop(fileNameBase + "-client-" + flags + ".dat", size);
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            
            return true;
        }
        default: {
            
        }
        }
        return false;
    }
    

    public static String readUtf8(IpcFileInterop inp, int start, int end) {
        StringBuilder sb = new StringBuilder(end - start);
        
        for (;;) {
            if (start == end) {
                break;
            }
            int b = inp.get(start++) & 0xFF;

            if ((b & 0x80) == 0) {
                sb.append((char)b);
            } else
            if ((b & 0b1110_0000) == 0b1100_0000) {
                
                if (start == end) {
                    break;
                }
                
                int b1 = inp.get(start++) & 0b11_1111;
                
                int c = ((b & 0b1_1111) << 6) | (b1);
                sb.append((char)c);
            } else
            if ((b & 0b1111_0000) == 0b1110_0000) {
                if (start == end) {
                    break;
                }
                int b1 = inp.get(start++) & 0b11_1111;

                if (start == end) {
                    break;
                }

                int b2 = inp.get(start++) & 0b11_1111;

                int c = ((b & 0b1111) << 12) | (b1 << 6)
                        | (b2);
                
                sb.append((char)c);
            } else
            if ((b & 0b1111_1000) == 0b1111_0000) {
                if (start == end) {
                    break;
                }
                int b1 = inp.get(start++) & 0b11_1111;

                if (start == end) {
                    break;
                }

                int b2 = inp.get(start++) & 0b11_1111;

                if (start == end) {
                    break;
                }

                int b3 = inp.get(start++) & 0b11_1111;

                int c = ((b & 0b111) << 18) 
                        | (b1 << 12)
                        | (b2 << 6)
                        | (b3);
                
                sb.append((char)c);
            }
        }
        
        return sb.toString();
    }
    
    byte[] toBytes(String function) {
        if (function == null || function.isEmpty()) {
            return EMPTY;
        }
        return function.getBytes(StandardCharsets.UTF_8);
    }
    
    boolean checkWriteFull(IpcFileInterop outp, int writeIndex, int messageLength) {
        if (writeIndex + messageLength + 16 >= size) {
            int idx = ++fileIndex;
            
            try {
                if (server) {
                    output = new IpcFileInterop(fileNameBase + "-client-" + (idx) + ".dat", size);
                } else {
                    output = new IpcFileInterop(fileNameBase + "-server-" + (idx) + ".dat", size);
                }
                this.writeIndex = 0;
                outp.setInt(writeIndex + 4, (idx << 8) | IpcReceive.TYPE_SWITCH);
                outp.setLong(writeIndex + 8, 0L);
                outp.setIntVolatile(writeIndex, 16);
                outp.closeFile();
            } catch (IOException ex) {
                UnsignalledExceptions.onErrorDropped(ex);
            }
            
            return true;
        }
        return false;
    }
    
    @Override
    public void sendNew(long streamId, String function) {
        byte[] b = toBytes(function);
        if (b.length > size - 32) {
            throw new IllegalStateException("Message too big: " + b.length + " <= " + (size - 32) + " required");
        }
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = b.length;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_NEW);
            outp.setLong(wi + 8, streamId);
            if (n != 0) {
                outp.set(wi + 16, b);
            }
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    @Override
    public void sendCancel(long streamId, String reason) {
        byte[] b = toBytes(reason);
        if (b.length > size - 32) {
            throw new IllegalStateException("Message too big: " + b.length + " <= " + (size - 32) + " required");
        }
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = b.length;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_CANCEL);
            outp.setLong(wi + 8, streamId);
            if (n != 0) {
                outp.set(wi + 16, b);
            }
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    @Override
    public void sendNext(long streamId, Object value) throws IOException {
        if (value instanceof Integer) {
            sendInt(streamId, (Integer)value);
        } else
        if (value instanceof Long) {
            sendLong(streamId, (Long)value);
        } else
        if (value instanceof String) {
            sendString(streamId, (String)value);
        } else
        if (value instanceof byte[]) {
            sendBytes(streamId, (byte[])value);
        } else {
            sendObject(streamId, value);
        }
    }
    
    void sendInt(long streamId, int value) {
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = 4;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_NEXT);
            outp.setLong(wi + 8, streamId | (IpcReceive.PAYLOAD_INT << 8));
            outp.setInt(wi + 16, value);
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    void sendLong(long streamId, long value) {
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = 8;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_NEXT);
            outp.setLong(wi + 8, streamId | (IpcReceive.PAYLOAD_LONG << 8));
            outp.setLong(wi + 16, value);
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    void sendString(long streamId, String value) {
        byte[] b;
        if (value == null || value.isEmpty()) {
            b = EMPTY;
        } else {
            b = value.getBytes(StandardCharsets.UTF_8);
        }
        if (b.length > size - 32) {
            throw new IllegalStateException("Message too big: " + b.length + " <= " + (size - 32) + " required");
        }
        
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = b.length;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_NEXT);
            outp.setLong(wi + 8, streamId | (IpcReceive.PAYLOAD_STRING << 8));
            if (n != 0) {
                outp.set(wi + 16, b);
            }
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    void sendBytes(long streamId, byte[] value) {
        if (value.length > size - 32) {
            throw new IllegalStateException("Message too big: " + value.length + " <= " + (size - 32) + " required");
        }
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = value.length;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_NEXT);
            outp.setLong(wi + 8, streamId | (IpcReceive.PAYLOAD_BYTES << 8));
            if (n != 0) {
                outp.set(wi + 16, value);
            }
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    void sendObject(long streamId, Object o) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bout);
        oos.writeObject(o);
        byte[] b = bout.toByteArray();
        if (b.length > size - 32) {
            throw new IllegalStateException("Message too big: " + b.length + " <= " + (size - 32) + " required");
        }
        
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = b.length;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_NEXT);
            outp.setLong(wi + 8, streamId | (IpcReceive.PAYLOAD_OBJECT << 8));
            if (n != 0) {
                outp.set(wi + 16, b);
            }
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    @Override
    public void sendError(long streamId, Throwable e) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(bout, StandardCharsets.UTF_8);
        PrintWriter pw = new PrintWriter(osw);
        e.printStackTrace(pw);
        pw.flush();
        
        byte[] b = bout.toByteArray();
        if (b.length > size - 32) {
            throw new IllegalStateException("Message too big: " + b.length + " <= " + (size - 32) + " required");
        }
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = b.length;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_ERROR);
            outp.setLong(wi + 8, streamId);
            if (n != 0) {
                outp.set(wi + 16, b);
            }
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    @Override
    public void sendComplete(long streamId) {
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            if (checkWriteFull(outp, wi, 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_COMPLETE);
            outp.setLong(wi + 8, streamId);
            outp.setIntVolatile(wi, 16);
            
            writeIndex = wi + 16;
            incrementWip();
        });
    }
    
    @Override
    public void sendRequested(long streamId, long r) {
        dataWriter.schedule(() -> {
            IpcFileInterop outp = output;
            int wi = writeIndex;
            int n = r <= 0xFFFFFF ? 0 : 8;

            if (checkWriteFull(outp, wi, n + 16)) {
                wi = 0;
                outp = output;
            }
            
            outp.setInt(wi + 4, IpcReceive.TYPE_REQUEST | (((int)r) << 8));
            outp.setLong(wi + 8, streamId);
            if (n != 0) {
                outp.setLong(wi + 16, r);
            }
            outp.setIntVolatile(wi, n + 16);
            
            writeIndex = wi + n + 16;
            incrementWip();
        });
    }
    
    /** 
     * Increments the wip counter for the other side and sends a byte via TCP
     * to unblock the other side if necessary.
     */
    void incrementWip() {
        int wipOffset = server ? SERVER_TO_CLIENT_WIP : CLIENT_TO_SERVER_WIP;
        if (wip.getAndAddInt(wipOffset, 1) == 0) {
            signalWriter.schedule(this::sendByte);
        }
    }
    
    void sendByte() {
        try {
            OutputStream o = out;
            o.write(iobuffer);
            o.flush();
        } catch (IOException ex) {
            if (!closed) {
                UnsignalledExceptions.onErrorDropped(ex);
            }
        }
    }
}
