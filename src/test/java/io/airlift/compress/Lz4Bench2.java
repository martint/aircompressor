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

import io.airlift.compress.lz4.Lz4SafeDecompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.logic.BlackHole;
import org.openjdk.jmh.logic.results.Result;
import org.openjdk.jmh.logic.results.RunResult;
import org.openjdk.jmh.output.OutputFormatType;
import org.openjdk.jmh.runner.BenchmarkRecord;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
public class Lz4Bench2
{
    private Slice compressedSlice;
    private Slice uncompressedSlice;
    private byte[] compressedBytes;
    private byte[] uncompressedBytes;

    private Lz4SafeDecompressor decompressor;
    private LZ4FastDecompressor jpountzDecompressor;
    private LZ4FastDecompressor jpountzJniDecompressor;

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
        decompressor = new Lz4SafeDecompressor();

        jpountzDecompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
        jpountzJniDecompressor = LZ4Factory.fastestInstance().fastDecompressor();
    }

    private Slice getUncompressedData()
            throws IOException
    {
        return Slices.mapFileReadOnly(new File("testdata/html"));
    }

    @GenerateMicroBenchmark
    public int airlift(BytesCounter counter)
    {
        int written = decompressor.uncompress(compressedSlice, 0, compressedSlice.length(), uncompressedSlice, 0);
        counter.add(uncompressedBytes.length);
        return written;
    }

    @GenerateMicroBenchmark
    public int jpountzUnsafe(BytesCounter counter)
    {
        int read = jpountzDecompressor.decompress(compressedBytes, 0, uncompressedBytes, 0, uncompressedBytes.length);
        counter.add(uncompressedBytes.length);
        return read;
    }

    @GenerateMicroBenchmark
    public int jpountzJNI(BytesCounter counter)
    {
        int read = jpountzJniDecompressor.decompress(compressedBytes, 0, uncompressedBytes, 0, uncompressedBytes.length);
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
                .include(".*" + Lz4Bench2.class.getSimpleName() + ".*")
                .forks(1)
                .warmupIterations(5)
                .measurementIterations(10)
                .build();

        SortedMap<BenchmarkRecord, RunResult> result = new Runner(opt).run();

        for (Map.Entry<BenchmarkRecord, RunResult> entry : result.entrySet()) {
            Result bytes = entry.getValue().getSecondaryResults().get("getBytes");
            System.out.println(entry.getKey().getUsername() + ": " + toHumanReadableSpeed((long) bytes.getStatistics().getMean()));
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


