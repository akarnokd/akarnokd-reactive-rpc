package hu.akarnokd.reactive.ipc;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.*;

public final class IpcFileInterop extends InputStream {
    final FileChannel channel;
    
    final MappedByteBuffer buffer;
    
    final long address;
    
    final int size;

    int streamOffset;
    
    static final long BYTE_BASE = UnsafeAccess.UNSAFE.arrayBaseOffset(byte[].class);

    final Path path;

    public IpcFileInterop(String fileName, int size) throws IOException {
        this.path = Paths.get(fileName);
        this.channel = FileChannel.open(path, StandardOpenOption.CREATE);
        this.buffer = channel.map(MapMode.READ_WRITE, 0, size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        this.size = size;
        this.address = bufferAddress(buffer);
        if (address == 0) {
            throw new IllegalStateException("Unable to get a hold onto the buffer address.");
        }
    }
    
    public int size() {
        return size;
    }
    
    public int getAndAddInt(int offset, int value) {
        return UnsafeAccess.UNSAFE.getAndAddInt(null, address + offset, value);
    }
    
    public int addAndGetInt(int offset, int value) {
        return getAndAddInt(offset, value) + value;
    }

    public int getIntVolatile(int offset) {
        return UnsafeAccess.UNSAFE.getIntVolatile(null, address + offset);
    }
    
    public void setIntVolatile(int offset, int value) {
        UnsafeAccess.UNSAFE.putIntVolatile(null, address + offset, value);
    }
    
    public void set(int offset, byte b) {
        buffer.put(offset, b);
    }
    
    public byte get(int offset) {
        return buffer.get(offset);
    }
    
    public void get(int offset, byte[] output) {
        if (offset + output.length > size) {
            throw new IndexOutOfBoundsException();
        }
        UnsafeAccess.UNSAFE.copyMemory(null, address + offset, output, BYTE_BASE, output.length);
    }
    
    public int getInt(int offset) {
        return buffer.getInt(offset);
    }
    
    public long getLong(int offset) {
        return buffer.getLong(offset);
    }
    
    public void setInt(int offset, int value) {
        buffer.putInt(offset, value);
    }

    public void setLong(int offset, long value) {
        buffer.putLong(offset, value);
    }

    public void set(int offset, byte[] b) {
        set(offset, b, 0, b.length);
    }

    public void set(int offset, byte[] b, int start, int count) {
        if (offset + count > size) {
            throw new IndexOutOfBoundsException();
        }
        UnsafeAccess.UNSAFE.copyMemory(b, BYTE_BASE, null, address + offset, count);
    }

    
    static long bufferAddress(Buffer buf) {
        try {
            Field f = Buffer.class.getDeclaredField("address");
            f.setAccessible(true);
            return f.getLong(buf);
        } catch (Throwable ex) {
            return 0L;
        }
    }
    
    @Override
    public void close() throws java.io.IOException {
        // ignoring close calls to InputStream
    }

    public void closeFile() throws java.io.IOException {
        channel.close();
    }
    
    public void delete() throws IOException {
        Files.deleteIfExists(path);
    }
    
    @Override
    public int read() throws IOException {
        int o = streamOffset;
        if (o == size) {
            return -1;
        }
        streamOffset = o + 1;
        return get(o) & 0xFF;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int o = streamOffset;
        if (o == size) {
            return -1;
        }
        if (off + len > b.length) {
            throw new IndexOutOfBoundsException();
        }
        int count = Math.min(len, size - o);
        
        UnsafeAccess.UNSAFE.copyMemory(null, address + o, b, BYTE_BASE + off, count);
        
        streamOffset = o + count;
        return count;
    }
    
    @Override
    public long skip(long n) throws IOException {
        streamOffset += n;
        return n;
    }
    
    @Override
    public int available() throws IOException {
        return size - streamOffset;
    }
    
    public void setStreamOffset(int offset) {
        this.streamOffset = offset;
    }
}
