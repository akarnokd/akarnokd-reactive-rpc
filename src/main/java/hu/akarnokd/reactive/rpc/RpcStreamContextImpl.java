package hu.akarnokd.reactive.rpc;

import java.net.InetAddress;
import java.util.concurrent.*;

import io.reactivex.Scheduler;

final class RpcStreamContextImpl<T> implements RpcStreamContext<T> {
    
    final InetAddress address;
    
    final int port;
    
    final ConcurrentMap<String, Object> map;
    
    final T remoteAPI;
    
    final Scheduler scheduler;
    
    public RpcStreamContextImpl(InetAddress address, int port, T remoteAPI, Scheduler scheduler) {
        this.address = address;
        this.port = port;
        this.remoteAPI = remoteAPI;
        this.scheduler = scheduler;
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public InetAddress clientAddress() {
        return address;
    }

    @Override
    public int clientPort() {
        return port;
    }

    @Override
    public void set(String attribute, Object o) {
        map.put(attribute, o);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> U get(String attribute) {
        return (U)map.get(attribute);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> U get(String attribute, U defaultValue) {
        return (U)map.getOrDefault(attribute, defaultValue);
    }

    @Override
    public void remove(String attribute) {
        map.remove(attribute);
    }

    @Override
    public boolean has(String attribute) {
        return map.containsKey(attribute);
    }

    @Override
    public T remoteAPI() {
        return remoteAPI;
    }
    
    @Override
    public Scheduler scheduler() {
        return scheduler;
    }
}
