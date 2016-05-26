package hu.akarnokd.reactive.rpc;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Protocol pattern:
 * <p>
 * <pre>
 * 00-03: payload length including the header (4 bytes little endian)
 * 04-04: entry type (1 bytes)
 * 05-07: entry flags (3 bytes little endian)
 * 08-0F: stream identifier (8 bytes little endian); bit 63: set for server opened streams
 */
enum RsRpcProtocol {
    ;
    /**
     * Starts a new stream numbered by the sender, the payload
     * is an UTF-8 function name if present
     */
    public static final int TYPE_NEW = 1;
    /**
     * Cancels and stops a stream, dropping all further messages
     * with the same identifier. The payload, if present may
     * contain a reason (UTF-8 error message with stacktrace).
     */
    public static final int TYPE_CANCEL = 2;
    /** The next value within a stream. */
    public static final int TYPE_NEXT = 3;
    /** 
     * The error signal. The payload, if present may
     * contain a reason (UTF-8 error message with stacktrace).
     */
    public static final int TYPE_ERROR = 4;
    /** The complete signal, stopping a stream. */
    public static final int TYPE_COMPLETE = 5;
    /** 
     * Indicate more values can be sent. If no payload present,
     * the flags holds the 3 byte positive integer amount,
     * if payload present, that indicates the request amount. Integer.MAX_VALUE and
     * negative amounts indicate unbounded mode. Zero is ignored in both cases.*/
    public static final int TYPE_REQUEST = 6;

    public static final byte PAYLOAD_OBJECT = 0;
    public static final byte PAYLOAD_INT = 1;
    public static final byte PAYLOAD_LONG = 2;
    public static final byte PAYLOAD_STRING = 3;
    public static final byte PAYLOAD_BYTES = 4;

    
    public interface RsRpcReceive {
        void onNew(long streamId, String function);
        
        void onCancel(long streamId, String reason);
        
        /**
         * Called when the stream contains an NEXT frame.
         * @param streamId the stream identifier
         * @param flags the flags associated with the NEXT frame
         * @param payload the payload bytes, its length is derived from the frame length
         * @param count the number of relevant bytes in the payload
         * @param read the number of read bytes, allows deciding what to do for partial reads
         */
        void onNext(long streamId, int flags, byte[] payload, int count, int read);
        
        void onError(long streamId, String reason);
        
        void onComplete(long streamId);
        
        void onRequested(long streamId, long requested);
        
        void onUnknown(int type, int flags, long streamId, byte[] payload, int read);
    }

    static void send(OutputStream out, long streamId, int type, int flags, byte[] payload, byte[] wb) {
        try {
            int len = 16 + (payload != null ? payload.length : 0);
            
            wb[0] = (byte)((len >> 0) & 0xFF);
            wb[1] = (byte)((len >> 8) & 0xFF);
            wb[2] = (byte)((len >> 16) & 0xFF);
            wb[3] = (byte)((len >> 24) & 0xFF);
            
            wb[4] = (byte)(type & 0xFF);
            
            wb[5] = (byte)((flags >> 0) & 0xFF);
            wb[6] = (byte)((flags >> 8) & 0xFF);
            wb[7] = (byte)((flags >> 16) & 0xFF);

            wb[8] = (byte)((int)(streamId >> 0) & 0xFF);
            wb[9] = (byte)((int)(streamId >> 8) & 0xFF);
            wb[10] = (byte)((int)(streamId >> 16) & 0xFF);
            wb[11] = (byte)((int)(streamId >> 24) & 0xFF);
            wb[12] = (byte)((int)(streamId >> 32) & 0xFF);
            wb[13] = (byte)((int)(streamId >> 40) & 0xFF);
            wb[14] = (byte)((int)(streamId >> 48) & 0xFF);
            wb[15] = (byte)((int)(streamId >> 56) & 0xFF);
            
            out.write(wb, 0, 16);
            
            if (payload != null && payload.length != 0) {
                out.write(payload);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    static void send(OutputStream out, long streamId, int type, int flags, long payload, byte[] wb) {
        try {
            int len = 24;
            
            wb[0] = (byte)((len >> 0) & 0xFF);
            wb[1] = (byte)((len >> 8) & 0xFF);
            wb[2] = (byte)((len >> 16) & 0xFF);
            wb[3] = (byte)((len >> 24) & 0xFF);
            
            wb[4] = (byte)(type & 0xFF);
            
            wb[5] = (byte)((flags >> 0) & 0xFF);
            wb[6] = (byte)((flags >> 8) & 0xFF);
            wb[7] = (byte)((flags >> 16) & 0xFF);

            wb[8] = (byte)((int)(streamId >> 0) & 0xFF);
            wb[9] = (byte)((int)(streamId >> 8) & 0xFF);
            wb[10] = (byte)((int)(streamId >> 16) & 0xFF);
            wb[11] = (byte)((int)(streamId >> 24) & 0xFF);
            wb[12] = (byte)((int)(streamId >> 32) & 0xFF);
            wb[13] = (byte)((int)(streamId >> 40) & 0xFF);
            wb[14] = (byte)((int)(streamId >> 48) & 0xFF);
            wb[15] = (byte)((int)(streamId >> 56) & 0xFF);
            
            wb[16] = (byte)((payload >> 0) & 0xFF);
            wb[17] = (byte)((payload >> 8) & 0xFF);
            wb[18] = (byte)((payload >> 16) & 0xFF);
            wb[19] = (byte)((payload >> 24) & 0xFF);
            wb[20] = (byte)((payload >> 32) & 0xFF);
            wb[21] = (byte)((payload >> 40) & 0xFF);
            wb[22] = (byte)((payload >> 48) & 0xFF);
            wb[23] = (byte)((payload >> 56) & 0xFF);

            out.write(wb, 0, len);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    static final byte[] EMPTY = new byte[0];

    public static boolean receive(InputStream in, byte[] rb, RsRpcReceive onReceive) {
        try {

            if (RpcHelper.readFully(in, rb, 16) < 16) {
                onReceive.onError(-1, "Channel/Connection closed");
                return false;
            }
            
            int len = (rb[0] & 0xFF) | ((rb[1] & 0xFF) << 8) | ((rb[2] & 0xFF) << 16) | ((rb[3] & 0xFF) << 24);
            
            byte type = rb[4];
            
            int flags = ((rb[5] & 0xFF)) | ((rb[6] & 0xFF) << 8) | ((rb[7] & 0xFF) << 16);
            
            long streamId = (rb[8] & 0xFFL) | ((rb[9] & 0xFFL) << 8) | ((rb[10] & 0xFFL) << 16) | ((rb[11] & 0xFFL) << 24)
                    | ((rb[12] & 0xFFL) << 32) | ((rb[13] & 0xFFL) << 40) | ((rb[14] & 0xFFL) << 48) | ((rb[15] & 0xFFL) << 56);
            
            switch (type) {
            case TYPE_NEW: {
                len -= 16;
                if (len != 0) {
                    String function;
                    if (len <= rb.length) {
                        int r = RpcHelper.readFully(in, rb, len);
                        if (r < len) {
                            onReceive.onError(streamId, "Channel/Connection closed (@ new)");
                            return false;
                        }
                        function = RpcHelper.readUtf8(rb, 0, len);
                    } else {
                        function = RpcHelper.readUtf8(in, len);
                    }
                    onReceive.onNew(streamId, function);
                } else {
                    onReceive.onNew(streamId, "");
                }
                break;
            }
            case TYPE_CANCEL: {
                if (len > 16) {
                    String reason = RpcHelper.readUtf8(in, len - 16);
                    onReceive.onCancel(streamId, reason);
                } else {
                    onReceive.onCancel(streamId, "");
                }
                break;
            }
            
            case TYPE_NEXT: {
                len -= 16;
                if (len != 0) {
                    byte[] payload;
                    if (len <= rb.length) {
                        payload = rb;
                    } else {
                        payload = new byte[len];
                    }
                    int r = RpcHelper.readFully(in, payload, len);
                    onReceive.onNext(streamId, flags, payload, len, r);
                } else {
                    onReceive.onNext(streamId, flags, EMPTY, 0, 0);
                }
                break;
            }
            case TYPE_ERROR: {
                if (len > 16) {
                    String reason = RpcHelper.readUtf8(in, len - 16);
                    onReceive.onError(streamId, reason);
                } else {
                    onReceive.onError(streamId, "");
                }
                break;
            }
            
            case TYPE_COMPLETE: {
                // ignore payload
                len -= 16;
                while (len != 0) {
                    int r = in.read(rb, 0, Math.min(len, rb.length));
                    if (r < 0) {
                        break;
                    }
                    len -= r;
                }
                onReceive.onComplete(streamId);
                break;
            }
            
            case TYPE_REQUEST: {
                if (len > 16) {
                    if (RpcHelper.readFully(in, rb, 8) < 8) {
                        onReceive.onError(streamId, "Channel/Connection closed (@ request)");
                        return false;
                    }
                    
                    long requested = (rb[0] & 0xFFL) | ((rb[1] & 0xFFL) << 8) | ((rb[2] & 0xFFL) << 16) | ((rb[3] & 0xFFL) << 24)
                            | ((rb[4] & 0xFFL) << 32) | ((rb[5] & 0xFFL) << 40) | ((rb[6] & 0xFFL) << 48) | ((rb[7] & 0xFFL) << 56);
                    
                    onReceive.onRequested(streamId, requested);
                } else {
                    onReceive.onRequested(streamId, flags);
                }
                break;
            }
            
            default: {
                if (len > 16) {
                    byte[] payload = new byte[len - 16];
                    int r = RpcHelper.readFully(in, payload, len - 16);
                    onReceive.onUnknown(type, flags, streamId, payload, r);
                } else {
                    onReceive.onUnknown(type, flags, streamId, EMPTY, 0);
                }
            }
            }
            
            return true;
        } catch (IOException ex) {
            onReceive.onError(-1, "I/O error while reading data: " + ex);
            return false;
        }
    }
    
    static byte[] utf8(String s) {
        if (s == null || s.isEmpty()) {
            return EMPTY;
        }
        return s.getBytes(StandardCharsets.UTF_8);
    }
    
    public static void open(OutputStream out, long streamId, String functionName, byte[] wb) {
        send(out, streamId, TYPE_NEW, 0, utf8(functionName), wb);
    }
    
    public static void cancel(OutputStream out, long streamId, String reason, byte[] wb) {
        send(out, streamId, TYPE_CANCEL, 0, utf8(reason), wb);
    }
    
    public static void cancel(OutputStream out, long streamId, Throwable reason, byte[] wb) {
        send(out, streamId, TYPE_CANCEL, 0, errorBytes(reason), wb);
    }
    
    public static byte[] errorBytes(Throwable reason) {
        if (reason == null) {
            return EMPTY;
        }
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        reason.printStackTrace(new PrintWriter(new OutputStreamWriter(bout, StandardCharsets.UTF_8)));
        return bout.toByteArray();
    }
    
    public static void next(OutputStream out, long streamId, int flags, byte[] data, byte[] wb) {
        send(out, streamId, TYPE_NEXT, flags, data, wb);
    }
    
    public static void next(OutputStream out, long streamId, int flags, String text, byte[] wb) {
        next(out, streamId, flags, utf8(text), wb);
    }
    
    public static void error(OutputStream out, long streamId, String reason, byte[] wb) {
        send(out, streamId, TYPE_ERROR, 0, utf8(reason), wb);
    }
    
    public static void error(OutputStream out, long streamId, Throwable reason, byte[] wb) {
        send(out, streamId, TYPE_ERROR, 0, errorBytes(reason), wb);
    }
    
    public static void complete(OutputStream out, long streamId, byte[] wb) {
        send(out, streamId, TYPE_COMPLETE, 0, EMPTY, wb);
    }
    
    static final byte[] REQUEST_UNBOUNDED = { (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, 
            (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0x7F };
    
    public static void request(OutputStream out, long streamId, long requested, byte[] wb) {
        if (requested < 0 || requested == Long.MAX_VALUE) {
            send(out, streamId, TYPE_REQUEST, 0, REQUEST_UNBOUNDED, wb);
        } else
        if (requested <= 0xFFFFFF) {
            send(out, streamId, TYPE_REQUEST, (int)requested & 0xFFFFFF, EMPTY, wb);
        } else {
            send(out, streamId, TYPE_REQUEST, 0, requested, wb);
        }
    }
    
}
