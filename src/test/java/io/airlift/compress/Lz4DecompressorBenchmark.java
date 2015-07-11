/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress;

import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lz4.Lz4SafeDecompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(1)
public class Lz4DecompressorBenchmark
{
    private Slice compressedSlice;
    private Slice uncompressedSlice;
    private byte[] compressedBytes;
    private byte[] uncompressedBytes;

    private Lz4SafeDecompressor safeDecompressor;
    private Lz4Decompressor decompressor;
    private LZ4SafeDecompressor jpountzDecompressor;
    private LZ4SafeDecompressor jpountzJniDecompressor;

    @Setup
    public void prepare()
            throws IOException
    {
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        Slice uncompressed = getUncompressedData();
        int maxCompressedLength = compressor.maxCompressedLength(uncompressed.length());

        byte[] compressedBytes = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(uncompressed.getBytes(), 0, uncompressed.length(), compressedBytes, 0);

        this.compressedBytes = Arrays.copyOf(compressedBytes, compressedLength);
        compressedSlice = Slices.wrappedBuffer(this.compressedBytes);

        this.uncompressedBytes = uncompressed.getBytes();
        uncompressedSlice = Slices.allocate(getUncompressedData().length());
        safeDecompressor = new Lz4SafeDecompressor();
        decompressor = new Lz4Decompressor();

        jpountzDecompressor = LZ4Factory.fastestJavaInstance().safeDecompressor();
        jpountzJniDecompressor = LZ4Factory.fastestInstance().safeDecompressor();

        safeDecompressor.uncompress(compressedSlice, 0, compressedSlice.length(), uncompressedSlice, 0);
        if (!uncompressed.equals(uncompressedSlice)) {
            throw new IllegalStateException("broken decompressor");
        }

        decompressor.uncompress(compressedSlice, 0, compressedSlice.length(), uncompressedSlice, 0);
        if (!uncompressed.equals(uncompressedSlice)) {
            throw new IllegalStateException("broken decompressor");
        }
    }

    private Slice getUncompressedData()
            throws IOException
    {
        return Slices.mapFileReadOnly(new File("testdata/html"));
    }

    @Benchmark
    public int airlift(BytesCounter counter)
    {
        int written = decompressor.uncompress(compressedSlice, 0, compressedSlice.length(), uncompressedSlice, 0);
        counter.add(uncompressedBytes.length);
        return written;
    }

    @Benchmark
    public int airliftSafe(BytesCounter counter)
    {
        int written = safeDecompressor.uncompress(compressedSlice, 0, compressedSlice.length(), uncompressedSlice, 0);
        counter.add(uncompressedBytes.length);
        return written;
    }

//    @Benchmark
    public int jpountzUnsafe(BytesCounter counter)
    {
        int read = jpountzDecompressor.decompress(compressedBytes, 0, compressedBytes.length, uncompressedBytes, 0, uncompressedBytes.length);
        counter.add(uncompressedBytes.length);
        return read;
    }

//    @Benchmark
    public int jpountzJNI(BytesCounter counter)
    {
        int read = jpountzJniDecompressor.decompress(compressedBytes, 0, compressedBytes.length, uncompressedBytes, 0, uncompressedBytes.length);
        counter.add(uncompressedBytes.length);
        return read;
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class BytesCounter
    {
        private long bytes;

        @Setup(Level.Iteration)
        public void reset()
        {
            bytes = 0;
        }

        public void add(long bytes)
        {
            this.bytes += bytes;
        }

        public long getBytes()
        {
            return bytes;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options opt = new OptionsBuilder()
//                .outputFormat(OutputFormatType.Silent)
                .include(".*" + Lz4DecompressorBenchmark.class.getSimpleName() + ".*")
//                .forks(1)
//                .warmupIterations(5)
//                .measurementIterations(10)
                .build();

        Collection<RunResult> results = new Runner(opt).run();

        for (RunResult result : results) {
            Result bytes = result.getSecondaryResults().get("getBytes");
            System.out.println(result.getPrimaryResult().getLabel() + ": " + toHumanReadableSpeed((long) bytes.getStatistics().getMean()));
        }
        System.out.println();
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


