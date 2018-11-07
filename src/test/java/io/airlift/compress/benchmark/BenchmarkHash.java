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
package io.airlift.compress.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(1)
public class BenchmarkHash
{
    private static final long PRIME_5_BYTES = 0xCF_1B_BC_DC_BBL;

    int bits = 16;
    long value = ThreadLocalRandom.current().nextLong();

//    @Benchmark
    public long hash1()
    {
        return ((value << (Long.SIZE - 40)) * PRIME_5_BYTES) >>> (Long.SIZE - bits);
    }

//    @Benchmark
    public long hash2()
    {
        return ((value & ((1L << 40) - 1)) * PRIME_5_BYTES >>> 40) & ((1L << bits) - 1);
    }

    @Benchmark
    public long hash3()
    {
        return ((value & ((1L << 40) - 1)) * PRIME_5_BYTES >>> 40) >>> (Long.SIZE - bits);
    }


    public static void main(String[] args)
            throws RunnerException, CommandLineOptionException
    {
        CommandLineOptions parsedOptions = new CommandLineOptions(args);
        ChainedOptionsBuilder options = new OptionsBuilder()
                .parent(parsedOptions);

        if (parsedOptions.getIncludes().isEmpty()) {
            options = options.include(".*\\." + BenchmarkHash.class.getSimpleName() + ".*");
        }

        Collection<RunResult> results = new Runner(options.build()).run();

//        for (RunResult result : results) {
//            Statistics stats = result.getPrimaryResult().getStatistics();
//            System.out.println(result.getPrimaryResult().getLabel() + ": " + stats.getMean() + " +/- " + stats.getStandardDeviation());
//        }
    }
}
