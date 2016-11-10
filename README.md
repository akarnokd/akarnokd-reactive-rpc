# akarnokd-reactive-rpc
<a href='https://travis-ci.org/akarnokd/akarnokd-reactive-rpc/builds'><img src='https://travis-ci.org/akarnokd/akarnokd-reactive-rpc.svg?branch=master'></a>

Reactive Remote Procedure/Publisher Call/Consumption library based on Reactive-Streams

# Example

Note that currently, this library is a proof of concept and may not use the most advanced I/O opportunities beneath the
cutting edge RxJava 2.

The library composes backpressure and cancellation through but does not modulate request amounts by itself. Therefore, it is
advised one uses `Flowable.rebatchRequest` or other bounded requesting operator to limit the in-flight data amount if necessary.

## Client side

The RPC works by defining a client-side interface with methods with plain types or Publishers.

```java
interface PingPongClient {
    @RsRpc
    Integer ping(Integer value);
    
    @RsRpc
    Publisher<Integer> pings(Publisher<Integer> values);
}
```

You can define synchronous RPC calls and asynchronous RPC calls with multiple values. The underlying logic will convert the synchronous RPC calls into asynchronous calls via `Flowable.just(value)` and do a `blockingLast` on the incoming asynchronous stream. The method names matter as the service with the same name will be used on the server side to locate the relevant procedure.

Note that you can send only `Serializable` data with this library at the moment.

You create an instance of this API via the `RpcClient` class as follows:

```java
RpcClient<PingPongClientAPI> client = RpcClient.createRemote(PingPongClientAPI.class);
```

Connecting to a remote service happens as follows:

```java
// cancel support as we can't return two Objects
AtomicReference<Disposable> cancel = new AtomicReference<Disposable>();

PingPongClientAPI api = client.connect(InetAddress.getLocalHost(), 12345, cancel::set);

// later on

cancel.get().dispose();
```

Given the `api` instance you can call the defined methods:

```java
System.out.println(api.ping(1));

Flowable.fromPublisher(api.pings(Flowable.range(1, 10)))
.blockingForEach(System.out::println);
```

There is a limited support for multi-parameter synchronous calls and you can target an async remote procedure with a syncrhonous client API.

## Server side

You can have any plain class with public methods annotated with `@RsRpc` as service endpoints:

```java
public class PingPongServerAPI {
    @RsRpc
    public Integer ping(RpcStreamContext<Void> ctx, Integer value) {
        return value + 1;
    }
    
    @RsRpc
    public Publisher<Integer> pings(RpcStreamContext<Void> ctx, Publisher<Integer> values) {
        return Flowable.fromPublisher(values).map(v -> v + 1);
    }
}
```

(Currently, the `RpcStreamContext<Void> ctx` parameter is mandatory and must be the first parameter declared.)

You setup a server via the `RpcServer` class as follows:

```java
RpcServer<Void> server = RpcServer.createLocal(new PingPongServerAPI());
```

Then you start it and use it with try-with-resources:

```java
AutoCloseable c = server.start(12345);

// later

c.close();
```

## Interaction types

The library supports 4 kinds of interactions between the client and server:

### send

The client sends data and without expecting any response:

```java
interface SendExampleClient {
    @RsRpc
    void send(Integer data);
}

class SendExampleService {
    @RsRpc
    public void send(RpcStreamContext<Void> ctx, Publisher<Integer> input) {
        Flowable.fromPublisher(input).subscribe(System.out::println);
    }
}
```

### receive

The client receives data from the server without sending anything.

```java
interface ReceiveExampleClient {
    @RsRpc
    Publisher<Integer> receive();
}

class ReceiveExampleService {
    @RsRpc
    public Publisher<Integer> receive(RpcStreamContext<Void> ctx) {
        return Flowable.range(1, 10);
    }
}
```

### map

The client sends some data to the server and in return receives some other data (the amount need not match).

```java
interface MapExampleClient {
    @RsRpc
    Publisher<String> map(Publisher<Integer> values);
}

class MapExampleService {
    @RsRpc
    public Publisher<Integer> map(RpcStreamContext<Void> ctx, Publisher<Integer> input) {
        return Flowable.fromPublisher(input).map(Integer::toString);
    }
}
```

### umap

The name "umap" is a small word joke of "you map, dear client" that means that when calling the service, it will send the client
data and the client should send back the data in response. In essence, the client does some work for the server in a roundrip fashion.

```java
interface UmapExampleClient {
    void umap(Function<Publisher<String>, Publisher<Integer>> function);
}

class UmapExampleService {
    public Publisher<String> umap(RpcStreamContext<Void> ctx, Publisher<Integer> input) {
        Flowable.fromPublisher(input).subscribe(System.out::println);
        
        return Flowable.range(1, 10).map(v -> Integer.toString(-v));
    }
}
```

The client side signature is a bit odd but if you used full `Observable`/`Flowable` transforming functions before, it should be straightforward:

```java
api.umap(fromServer -> Flowable.fromPublisher(fromServer).map(Integer::parseInt));
```

When one calls the `umap` function, it asks for one sequence-level transformation via the function you have to provide where you bind the input `Publisher` to the returned `Publisher` in some form.

On the server side, the signature is the same as the `map` case, but the handling is quite different. One has to start listening to the input source return a data sequence via a `Publisher` at the end that will (usually) trigger a roundtrip of data. Note however, that binding the input to the output will likely hang indefinitely as there is no input received until data is sent out to trigger the loop.

## Bidirectional flows

The library supports - but is not tested - bidirectional method calls, i.e., if the client defines a server-like API and the server defines a client-like API, the server may initiate an RPC call against the client.

On the client side, one can create the API and service API via `RpcClient.createBidirectional`:

```java
interface ClieantAPI {
    @RsRpc
    Integer map(Integer v);
}

class ClientServicesAPI {
    @RsRpc
    public String toLowerCase(RpcStreamContext<Void> ctx, String s) {
        return s.toLowerCase();
    }
}

RpcClient.createBidirectional(ClientAPI.class, new ClientServicesAPI());
```

On the server side, one needs the implementation of the client API and needs a API definition of the client services:

```java
class ServerSideAPI {
    @RsRpc
    public Publisher<Integer> map(RpcStreamContext<Void> ctx, Publisher<Integer> input) {
        return Flowable.fromPublisher(input).map(v -> -v);
    }
    
    @RsRpcInit
    public void init(RpcStreamContext<Void> ctx) {
        Flowable.fromPublisher(ctx.remoteAPI().toLowerCase(Flowable.just("HELLO"))).subscribe(System.out::println);
    }
}

interface ClientSideAPI {
    @RsRpc
    Publisher<String> toLowerCase(Publisher<String> s);
}
```

When defining the service class, you can annotate a method with `@RsRpcInit` that will be called just after the connection between the client and server has been established and use the `RcpStreamContext.remoteAPI` to gain access to the other side's API. An `@RsRpcDone` annotated method is called when the connection ends.

# Releases

You can download any necessary JAR files manually from

https://oss.sonatype.org/content/groups/public/com/github/akarnokd/akarnokd-reactive-rpc/

Alternatively, you can use the usual maven dependency management to get the files:

**gradle**

```
dependencies {
    compile "com.github.akarnokd:akarnokd-reactive-rpc:0.2.0"
}
```

**ivy**

```
<dependencies>
		<dependency org="com.github.akarnokd" name="akarnokd-reactive-rpc" rev="0.2.0" />
</dependencies>
```

or the maven search facility

[http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.akarnokd%22)
