package hu.akarnokd.reactive.rpc;

import java.net.InetAddress;

import io.reactivex.Scheduler;

public interface RpcStreamContext<T> {

    InetAddress clientAddress();
    
    int clientPort();
    
    void set(String attribute, Object o);
    
    <U> U get(String attribute);

    <U> U get(String attribute, U defaultValue);

    void remove(String attribute);
    
    boolean has(String attribute);
    
    T remoteAPI();
    
    Scheduler scheduler();
}
