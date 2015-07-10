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

import io.airlift.compress.lz4.Lz4CompressorNew;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
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
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(1)
public class Lz4Bench3
{
    private Slice compressedSlice;
    private Slice uncompressedSlice;
    private Lz4CompressorNew compressor;
    private LZ4Compressor jpountz;
    private byte[] uncompressedBytes;
    private byte[] compressedBytes;

    @Setup
    public void prepare()
            throws IOException
    {
        uncompressedSlice = getUncompressedData();
        uncompressedBytes = uncompressedSlice.getBytes();
        int maxCompressedLength = Lz4CompressorNew.maxCompressedLength(uncompressedSlice.length());
        compressedSlice = Slices.allocate(maxCompressedLength);
        compressedBytes = new byte[compressedSlice.length()];

        compressor = new Lz4CompressorNew();
        jpountz = LZ4Factory.fastestInstance().fastCompressor();
    }

    private Slice getUncompressedData()
            throws IOException
    {
        return Slices.copyOf(Slices.mapFileReadOnly(new File("testdata/html")));
    }

    @Benchmark
    public int airlift2(BytesCounter counter)
    {
        int written = compressor.compress(uncompressedSlice, 0, uncompressedSlice.length(), compressedSlice, 0);
        counter.add(uncompressedSlice.length());
        return written;
    }

    @Benchmark
    public int jpountz(BytesCounter counter)
    {
        int written = jpountz.compress(uncompressedBytes, 0, uncompressedSlice.length(), compressedBytes, 0);
        counter.add(uncompressedSlice.length());
        return written;
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
                .include(".*" + Lz4Bench3.class.getSimpleName() + ".*")
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


