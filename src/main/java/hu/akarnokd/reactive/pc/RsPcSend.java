package hu.akarnokd.reactive.pc;

import java.io.IOException;

public interface RsPcSend {
    void sendNew(long streamId, String function);
    
    void sendCancel(long streamId, String reason);
    
    void sendNext(long streamId, Object o) throws IOException;
    
    void sendError(long streamId, Throwable e);
    
    void sendComplete(long streamId);
    
    void sendRequested(long streamId, long n);
    
    boolean isClosed();
}
