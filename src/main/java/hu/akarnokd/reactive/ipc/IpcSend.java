package hu.akarnokd.reactive.ipc;

public interface IpcSend {
    
    void sendNew(long streamId, String function);
    
    void sendCancel(long streamId, String reason);
    
    void sendNext(long streamId, Object o);
    
    void sendError(long streamId, Throwable e);
    
    void sendComplete(long streamId);
    
    void sendRequested(long streamId, long n);
}
