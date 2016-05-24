package hu.akarnokd.reactiverpc;

import java.lang.annotation.*;

/**
 * Indicates the method, which has only an RpcStreamContext parameter and void return
 * should be called just after disconnection from the client.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RsRpcDone {
    String name() default "";
}
