package hu.akarnokd.reactive.rpc;

import java.util.concurrent.atomic.*;

import org.reactivestreams.Subscription;

import io.reactivex.internal.subscriptions.SubscriptionHelper;

public class DeferredSubscription extends AtomicReference<Subscription> implements Subscription {

    private static final long serialVersionUID = 8746019642539166854L;

    final AtomicLong requested = new AtomicLong();
    
    @Override
    public void cancel() {
        SubscriptionHelper.cancel(this);
    }
    
    @Override
    public void request(long n) {
        SubscriptionHelper.deferredRequest(this, requested, n);
    }
    
    public void setSubscription(Subscription s) {
        SubscriptionHelper.deferredSetOnce(this, requested, s);
    }
}
