package hu.akarnokd.reactive.ipc;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

enum UnsafeAccess {
    ;
    
    static final sun.misc.Unsafe UNSAFE;
    
    static {
        Unsafe u = null;
        try {
            /*
             * This mechanism for getting UNSAFE originally from:
             * 
             * Original License: https://github.com/JCTools/JCTools/blob/master/LICENSE
             * Original location: https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/util/UnsafeAccess.java
             */
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            u = (Unsafe) field.get(null);
        } catch (Throwable e) {
            // do nothing, hasUnsafe() will return false
        }
        UNSAFE = u;
    }
}
