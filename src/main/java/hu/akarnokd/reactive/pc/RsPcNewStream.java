package hu.akarnokd.reactive.pc;

@FunctionalInterface
public interface RsPcNewStream {
    boolean onNew(long streamId, String function, RsAPIManager manager);
}
