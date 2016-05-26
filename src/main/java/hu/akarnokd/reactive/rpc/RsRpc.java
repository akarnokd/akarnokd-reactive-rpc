package hu.akarnokd.reactive.rpc;

import java.lang.annotation.*;

/**
 * Indicates a public method is a service with possible function name.
 * <p>Can be applied to incoming and outgoing service interfaces.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RsRpc {
    String name() default "";
}
