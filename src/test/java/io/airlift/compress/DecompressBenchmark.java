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
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.slice.Slice;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(20)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class DecompressBenchmark
{
    private Lz4SafeDecompressor decompressor = new Lz4SafeDecompressor();
    private Lz4Decompressor decompressor2 = new Lz4Decompressor();
    private LZ4FastDecompressor jpountzJniDecompressor = LZ4Factory.nativeInstance().fastDecompressor();
    private LZ4SafeDecompressor jpountzJavaDecompressor = LZ4Factory.unsafeInstance().safeDecompressor();

    @Benchmark
    public int airliftLz4(Lz4SliceFixture fixture, Counters counters)
    {
        Slice compressed = fixture.getCompressed();
        counters.recordCompressed(compressed.length());
        counters.recordUncompressed(fixture.getUncompressed().length);

        return decompressor.uncompress(compressed, 0, compressed.length(), fixture.getOutput(), 0);
    }

    @Benchmark
    public int airliftLz4_2(Lz4SliceFixture fixture, Counters counters)
    {
        Slice compressed = fixture.getCompressed();
        counters.recordCompressed(compressed.length());
        counters.recordUncompressed(fixture.getUncompressed().length);

        return decompressor2.uncompress(compressed, 0, compressed.length(), fixture.getOutput(), 0);
    }

//    @GenerateMicroBenchmark
    public int jpountzJniLz4(Lz4BytesFixture fixture, Counters counters)
    {
        byte[] compressed = fixture.getCompressed();
        counters.recordCompressed(compressed.length);
        counters.recordUncompressed(fixture.getUncompressed().length);

        return jpountzJniDecompressor.decompress(compressed, 0, fixture.getOutput(), 0, fixture.getOutput().length);
    }

    @Benchmark
    public int jpountzJavaLz4(Lz4BytesFixture fixture, Counters counters)
    {
        byte[] compressed = fixture.getCompressed();
        counters.recordCompressed(compressed.length);
        counters.recordUncompressed(fixture.getUncompressed().length);
        return jpountzJavaDecompressor.decompress(compressed, 0, compressed.length, fixture.getOutput(), 0, fixture.getOutput().length);
    }

//    @GenerateMicroBenchmark
    public int xerialSnappy(SnappyBytesFixture fixture, Counters counters)
            throws IOException
    {
        byte[] compressed = fixture.getCompressed();
        counters.recordCompressed(compressed.length);
        counters.recordUncompressed(fixture.getUncompressed().length);

        return org.xerial.snappy.Snappy.uncompress(compressed, 0, compressed.length, fixture.getOutput(), 0);
    }

//    @GenerateMicroBenchmark
    public int airliftSnappy(SnappyBytesFixture fixture, Counters counters)
            throws IOException
    {
        byte[] compressed = fixture.getCompressed();
        counters.recordCompressed(compressed.length);
        counters.recordUncompressed(fixture.getUncompressed().length);

        return Snappy.uncompress(compressed, 0, compressed.length, fixture.getOutput(), 0);
    }

    public static void main(String[] args)
            throws RunnerException
    {
//        Set<BenchmarkRecord> benchmarks = MicroBenchmarkList.defaultList()
//                .find(OutputFormatFactory.createFormatInstance(System.out, VerboseMode.SILENT), DecompressBenchmark.class.getName() + ".*", ImmutableList.<String>of());
//
//        for (SnappyBench.TestData dataset : SnappyBench.TestData.values()) {
////        for (SnappyBench.TestData dataset : ImmutableList.of(SnappyBench.TestData.jpg)) {//SnappyBench.TestData.values()) {
//            System.out.printf("%-8s (size = %d)\n", dataset.name(), dataset.getContents().length);
//            for (BenchmarkRecord benchmark : benchmarks) {
//                Options opt = new OptionsBuilder()
//                        .verbosity(VerboseMode.SILENT)
//                        .include(benchmark.getUsername())
//                        .jvmArgs("-Dtestdata=testdata/" + dataset.getFileName())
//                        .build();
//
//                RunResult result = new Runner(opt).runSingle();
//                Result uncompressedBytes = result.getSecondaryResults().get("getUncompressedBytes");
//
//                Statistics stats = uncompressedBytes.getStatistics();
//                System.out.printf("  %-14s %10s ± %10s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
//                        getBenchmarkName(benchmark),
//                        Util.toHumanReadableSpeed((long) stats.getMean()),
//                        Util.toHumanReadableSpeed((long) stats.getMeanErrorAt(0.999)),
//                        stats.getMeanErrorAt(0.999) * 100 / stats.getMean(),
//                        stats.getN());
//            }
//            System.out.println();
//        }
    }

//    private static String getBenchmarkName(BenchmarkRecord benchmark)
//    {
//        String name = benchmark.getUsername();
//        return name.substring(name.lastIndexOf('.') + 1);
//    }
}
