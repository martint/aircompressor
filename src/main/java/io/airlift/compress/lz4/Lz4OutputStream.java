package io.airlift.compress.lz4;

import io.airlift.compress.BufferRecycler;
import io.airlift.compress.Lz4Compressor;
import io.airlift.compress.Snappy;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import static io.airlift.compress.SnappyInternalUtils.checkNotNull;
import static io.airlift.compress.SnappyInternalUtils.checkPositionIndexes;

public class Lz4OutputStream
        extends OutputStream
{
    private static byte[] HEADER = { 0x04, 0x22, 0x4D, 0x18 };

    private static final int STREAM_DESCRIPTOR_FLAGS_UPPER =
            1 << 6 | // version
            1 << 5 | // independent blocks
            0 << 4 | // use checksum
            0 << 3 ; // has stream size

    private static final int STREAM_DESCRIPTOR_FLAGS_LOWER = 4 << 4; // 64 KB blocks

    static final int MAX_BLOCK_SIZE = 1 << 16; // 64kb

    private final BufferRecycler recycler;
    private final byte[] buffer;
    private final byte[] outputBuffer;
    private final OutputStream out;

    private int position;
    private boolean closed;

    private Lz4OutputStream(OutputStream out)
            throws IOException
    {
        this.out = checkNotNull(out, "out is null");
        recycler = BufferRecycler.instance();
        buffer = recycler.allocOutputBuffer(MAX_BLOCK_SIZE);
        outputBuffer = recycler.allocEncodingBuffer(Lz4Compressor.maxCompressedLength(MAX_BLOCK_SIZE));

        out.write(HEADER);
        out.write(STREAM_DESCRIPTOR_FLAGS_UPPER);
        out.write(STREAM_DESCRIPTOR_FLAGS_LOWER);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (position >= MAX_BLOCK_SIZE) {
            flushBuffer();
        }
        buffer[position++] = (byte) b;
    }

    @Override
    public void write(byte[] input, int offset, int length)
            throws IOException
    {
        checkNotNull(input, "input is null");
        checkPositionIndexes(offset, offset + length, input.length);
        if (closed) {
            throw new IOException("Stream is closed");
        }

        int free = MAX_BLOCK_SIZE - position;

        // easy case: enough free space in buffer for entire input
        if (free >= length) {
            copyToBuffer(input, offset, length);
            return;
        }

        // fill partial buffer as much as possible and flush
        if (position > 0) {
            copyToBuffer(input, offset, free);
            flushBuffer();
            offset += free;
            length -= free;
        }

        // write remaining full blocks directly from input array
        while (length >= MAX_BLOCK_SIZE) {
            writeCompressed(input, offset, MAX_BLOCK_SIZE);
            offset += MAX_BLOCK_SIZE;
            length -= MAX_BLOCK_SIZE;
        }

        // copy remaining partial block into now-empty buffer
        copyToBuffer(input, offset, length);
    }

    @Override
    public void flush()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        flushBuffer();
        out.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        try {
            flush();
            out.close();
        }
        finally {
            closed = true;
            recycler.releaseOutputBuffer(outputBuffer);
            recycler.releaseEncodeBuffer(buffer);
        }
    }

    private void copyToBuffer(byte[] input, int offset, int length)
    {
        System.arraycopy(input, offset, buffer, position, length);
        position += length;
    }

    private void flushBuffer()
            throws IOException
    {
        if (position > 0) {
            writeCompressed(buffer, 0, position);
            position = 0;
        }
    }

    private void writeCompressed(byte[] input, int offset, int length)
            throws IOException
    {
        // crc is based on the user supplied input data
        int compressed = Lz4Compressor.compress(input, offset, length, outputBuffer, 0);

        // use uncompressed input if less than 12.5% compression
        if (compressed >= (length - (length / 8))) {
            writeBlock(input, offset, length, false);
        }
        else {
            writeBlock(outputBuffer, 0, compressed, true);
        }
    }

    private void writeBlock(byte[] data, int offset, int length, boolean compressed)
            throws IOException
    {
        if (!compressed) {
            out.write(length | 0x8000); // set high bit to indicate uncompressed
        }
        else {
            out.write(length);
        }

        // write data
        out.write(data, offset, length);
    }

    public static void main(String[] args)
            throws IOException
    {
        Lz4OutputStream out = new Lz4OutputStream(new FileOutputStream("/tmp/out-lz4j"));
        out.write("hello there".getBytes(Charset.forName("utf-8")));
        out.flush();
        out.close();
    }
}
