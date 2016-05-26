package hu.akarnokd.reactive.rpc;

import java.lang.annotation.*;

/**
 * Indicates the method, which has only an RpcStreamContext parameter and void return
 * should be called just after the connection has been established.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RsRpcInit {
    String name() default "";
}
