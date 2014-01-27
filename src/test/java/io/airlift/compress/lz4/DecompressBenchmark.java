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
package io.airlift.compress.lz4;

import com.google.common.collect.ImmutableList;
import io.airlift.compress.Snappy;
import io.airlift.compress.SnappyBench;
import io.airlift.slice.Slice;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.logic.results.Result;
import org.openjdk.jmh.logic.results.RunResult;
import org.openjdk.jmh.output.OutputFormatFactory;
import org.openjdk.jmh.output.OutputFormatType;
import org.openjdk.jmh.runner.BenchmarkRecord;
import org.openjdk.jmh.runner.MicroBenchmarkList;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.parameters.TimeValue;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DecompressBenchmark
{
    private Lz4SafeDecompressor decompressor = new Lz4SafeDecompressor();
    private LZ4FastDecompressor jpountzJniDecompressor = LZ4Factory.nativeInstance().fastDecompressor();
    private LZ4FastDecompressor jpountzJavaDecompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();

    @GenerateMicroBenchmark
    public int airliftLz4(Lz4SliceFixture fixture, Counters counters)
    {
        Slice compressed = fixture.getCompressed();
        counters.addCompressed(compressed.length());
        counters.addUncompressed(fixture.getData().length());
        return decompressor.uncompress(compressed, 0, compressed.length(), fixture.getOutput(), 0);
    }

    @GenerateMicroBenchmark
    public int jpountzJni(Lz4BytesFixture fixture, Counters counters)
    {
        byte[] compressed = fixture.getCompressed();
        counters.addCompressed(compressed.length);
        counters.addUncompressed(fixture.getBytes().length);
        return jpountzJniDecompressor.decompress(compressed, 0, fixture.getOutput(), 0, fixture.getOutput().length);
    }

    @GenerateMicroBenchmark
    public int jpountzJava(Lz4BytesFixture fixture, Counters counters)
    {
        byte[] compressed = fixture.getCompressed();
        counters.addCompressed(compressed.length);
        counters.addUncompressed(fixture.getBytes().length);
        return jpountzJavaDecompressor.decompress(compressed, 0, fixture.getOutput(), 0, fixture.getOutput().length);
    }

    @GenerateMicroBenchmark
    public int xerialSnappy(SnappyBytesFixture fixture, Counters counters)
            throws IOException
    {
        byte[] compressed = fixture.getCompressed();
        counters.addCompressed(compressed.length);
        counters.addUncompressed(fixture.getBytes().length);

        return org.xerial.snappy.Snappy.uncompress(compressed, 0, compressed.length, fixture.getOutput(), 0);
    }

    @GenerateMicroBenchmark
    public int airliftSnappy(SnappyBytesFixture fixture, Counters counters)
            throws IOException
    {
        byte[] compressed = fixture.getCompressed();
        counters.addCompressed(compressed.length);
        counters.addUncompressed(fixture.getBytes().length);

        return Snappy.uncompress(compressed, 0, compressed.length, fixture.getOutput(), 0);
    }

    public static void main(String[] args)
            throws RunnerException
    {
//        "alice29.txt", "html", "asyoulik.txt"

        Set<BenchmarkRecord> benchmarks = MicroBenchmarkList.defaultList().find(OutputFormatFactory.createFormatInstance(false), DecompressBenchmark.class.getName() + ".*", ImmutableList.<String>of());

        for (SnappyBench.TestData dataset : SnappyBench.TestData.values()) {
            System.out.println("==== " + dataset.name());
            for (BenchmarkRecord benchmark : benchmarks) {
                System.out.print(benchmark.getUsername());

                Options opt = new OptionsBuilder()
//                        .verbose(true)
                        .outputFormat(OutputFormatType.Silent)
//                        .include(".*" + DecompressBenchmark.class.getSimpleName() + ".*")
                        .include(benchmark.getUsername())
                        .forks(1)
//                        .warmupIterations(5)
//                        .measurementIterations(10)
                        .warmupTime(TimeValue.seconds(5))
                        .measurementTime(TimeValue.seconds(10))
                        .jvmArgs("-Dtestdata=testdata/" + dataset.getFileName())
                        .build();

                RunResult result = new Runner(opt).runSingle();

                Result uncompressedBytes = result.getSecondaryResults().get("getUncompressedBytes");
                Result compressedBytes = result.getSecondaryResults().get("getCompressedBytes");

                double ratio = compressedBytes.getStatistics().getSum() / uncompressedBytes.getStatistics().getSum();
                System.out.println(String.format("\t%s\t%.2f%%",
                        toHumanReadableSpeed((long) uncompressedBytes.getStatistics().getMean()),
                        ratio * 100));
            }
            System.out.println();
        }
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
