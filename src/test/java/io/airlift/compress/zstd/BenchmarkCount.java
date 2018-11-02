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
package io.airlift.compress.zstd;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(1)
public class BenchmarkCount
{
    private byte[] input;
    private long start;
    private long matchStart;
    private long matchLimit;

    @Setup
    public void setup()
    {
        input = new byte[1000];
        matchLimit = input.length + 16;
        matchStart = 500;
        start = 0;
        
        int matchSize = 100;
        byte[] match = new byte[matchSize];
        ThreadLocalRandom.current().nextBytes(match);

        System.arraycopy(match, 0, input, (int) start, matchSize);
        input[(int) (start + matchSize)] = 1;

        System.arraycopy(match, 0, input, (int) matchStart, matchSize);
        input[(int) (matchStart + matchSize)] = 2;

        matchStart += 16;
        start += 16;
    }

    @Benchmark
    public long count()
    {
        int count = DoubleFastBlockCompressor.count(input, start, matchLimit, matchStart);
        return count;
    }

    @Benchmark
    public long count2()
    {
        int count = DoubleFastBlockCompressor.count2(input, start, matchLimit, matchStart);
        return count;
    }


    public static void main(String[] args)
            throws RunnerException, CommandLineOptionException
    {
        BenchmarkCount bench = new BenchmarkCount();
        bench.setup();
        System.out.println(bench.count());

//        if (true) {
//            return;
//        }

        CommandLineOptions parsedOptions = new CommandLineOptions(args);
        ChainedOptionsBuilder options = new OptionsBuilder()
                .parent(parsedOptions);

        if (parsedOptions.getIncludes().isEmpty()) {
            options = options.include(".*\\." + BenchmarkCount.class.getSimpleName() + ".*");
        }

        Collection<RunResult> results = new Runner(options.build()).run();
    }
}
