package hu.akarnokd.reactive.ipc;

public interface IpcReceive {
    int TYPE_NEW = 1;
    
    int TYPE_CANCEL = 2;
    
    int TYPE_NEXT = 3;
    
    int TYPE_ERROR = 4;
    
    int TYPE_COMPLETE = 5;
    
    int TYPE_REQUEST = 6;
    
    /** Switch the input file to the index given in the flags. */
    int TYPE_SWITCH = 8;
    
    int PAYLOAD_OBJECT = 0;
    
    int PAYLOAD_INT = 1;
    
    int PAYLOAD_LONG = 2;
    
    int PAYLOAD_STRING = 3;
    
    int PAYLOAD_BYTES = 4;
    
    void onNew(long streamId, String function);
    
    void onNext(long streamId, Object o);

    void onError(long streamId, String reason);

    void onError(long streamId, Throwable e);
    
    void onComplete(long streamId);
    
    void onCancel(long streamId, String reason);
    
    void onRequested(long streamId, long n);
}
