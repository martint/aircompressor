package io.airlift.compress;

import com.google.common.primitives.Longs;
import io.airlift.compress.lz4.Lz4SafeDecompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class Lz4Bench
{
    private static final int NUMBER_OF_RUNS = 30;

    public static void main(String[] args)
            throws IOException
    {
        Slice uncompressed = Slices.mapFileReadOnly(new File("testdata/html"));

//        Slice uncompressed = Slices.copiedBuffer("h", Charsets.UTF_8);
        for (int i = 0; i < 100; ++i) {
            System.out.println("uncompress lz4:    " + toHumanReadableSpeed(benchmarkUncompressLz4(uncompressed, 1000)));
        }
    }

    private static long benchmarkUncompressLz4(Slice uncompressed, int iterations)
            throws IOException
    {
        long[] runs = new long[NUMBER_OF_RUNS];
        for (int run = 0; run < NUMBER_OF_RUNS; ++run) {
            runs[run] = uncompressLz4(uncompressed, iterations);
        }
        long nanos = getMedianValue(runs);
        return (long) (1.0 * iterations * uncompressed.length() / nanosToSeconds(nanos));
    }

    private static Slice compress(Slice uncompressed)
            throws IOException
    {
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        int maxCompressedLength = compressor.maxCompressedLength(uncompressed.length());

        byte[] compressedBytes = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(uncompressed.getBytes(), 0, uncompressed.length(), compressedBytes, 0);

        return Slices.wrappedBuffer(Arrays.copyOf(compressedBytes, compressedLength));
    }

    private static long uncompressLz4(Slice uncompressed, int iterations)
            throws IOException
    {
        Slice compressed = compress(uncompressed);
        byte[] compressedBytes = compressed.getBytes();

        Slice out = Slices.allocate(uncompressed.length());
        byte[] outBytes = new byte[uncompressed.length()];
        LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

        long start = System.nanoTime();
        while (iterations-- > 0) {
//            new Lz4SafeDecompressor().uncompress(compressed, 0, compressed.length(), out, 0);
            decompressor.decompress(compressedBytes, outBytes);
        }
        long timeInNanos = System.nanoTime() - start;

        // verify results
        assertEquals(outBytes, uncompressed.getBytes());
//        if (!out.equals(uncompressed)) {
//            throw new AssertionError(String.format(
//                    "Actual   : %s\n" +
//                            "Expected : %s",
//                    Arrays.toString(out.getBytes()),
//                    Arrays.toString(uncompressed.getBytes())));
//        }

        return timeInNanos;
    }

    private static long getMedianValue(long[] benchmarkRuns)
    {
        ArrayList<Long> list = new ArrayList<Long>(Longs.asList(benchmarkRuns));
        Collections.sort(list);
        return list.get(benchmarkRuns.length / 2);
    }


    private static double nanosToSeconds(long nanos)
    {
        return 1.0 * nanos / TimeUnit.SECONDS.toNanos(1);
    }

    private static String toHumanReadableSpeed(long bytesPerSecond)
    {
        String humanReadableSpeed;
        if (bytesPerSecond < 1024) {
            humanReadableSpeed = format("%dB/s", bytesPerSecond);
        }
        else if (bytesPerSecond < 1024 * 1024) {
            humanReadableSpeed = format("%.1fkB/s", bytesPerSecond / 1024.0f);
        }
        else if (bytesPerSecond < 1024 * 1024 * 1024 * 10L) {
            humanReadableSpeed = format("%.1fMB/s", bytesPerSecond / (1024.0f * 1024.0f));
        }
        else {
            humanReadableSpeed = format("%.1fGB/s", bytesPerSecond / (1024.0f * 1024.0f * 1024.0f));
        }
        return humanReadableSpeed;
    }

}
