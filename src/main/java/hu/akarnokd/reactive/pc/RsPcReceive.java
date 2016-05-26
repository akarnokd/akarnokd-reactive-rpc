package hu.akarnokd.reactive.pc;

public interface RsPcReceive {
    void onNew(long streamId, String function);
    
    void onNext(long streamId, Object o);

    void onError(long streamId, String reason);

    void onError(long streamId, Throwable e);
    
    void onComplete(long streamId);
    
    void onCancel(long streamId, String reason);
    
    void onRequested(long streamId, long n);
}
