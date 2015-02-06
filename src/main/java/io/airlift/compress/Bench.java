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
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.lang.String.format;

public class Bench
{
    public static void main(String[] args)
            throws IOException
    {
        int loops = Integer.parseInt(args[0]);
        String file = args[1];

//        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
//        Slice uncompressed = Slices.copyOf(Slices.mapFileReadOnly(new File(file)));

//        int maxCompressedLength = compressor.maxCompressedLength(uncompressed.length());

//        byte[] compressedBytes = new byte[maxCompressedLength];
//        int compressedLength = compressor.compress(uncompressed.getBytes(), 0, uncompressed.length(), compressedBytes, 0);

//        Slice compressedSlice = Slices.wrappedBuffer(Arrays.copyOf(compressedBytes, compressedLength));
//
//        Slice uncompressedSlice = Slices.allocate(uncompressed.length());
//        byte[] uncompressedBytes = new byte[uncompressed.length()];

        benchmarkAirlift(loops, file);
        benchmarkJPountz(loops, file);
//        LZ4SafeDecompressor jpountz = LZ4Factory.fastestInstance().safeDecompressor();


//        long sum = 0;
//        for (int i = 0; i < 1_000_000; i++) {
////            sum += jpountz.decompress(compressedBytes, 0, compressedLength, uncompressedBytes, 0);
////            sum += decompressor.uncompress(compressedSlice, 0, compressedSlice.length(), uncompressedSlice, 0);
//        }

//        long start = System.nanoTime();
//        for (int i = 0; i < loops; i++) {
////            sum += jpountz.decompress(compressedBytes, 0, compressedLength, uncompressedBytes, 0);
//            sum += decompressor.uncompress(compressedSlice, 0, compressedSlice.length(), uncompressedSlice, 0);
//        }

//        double bytes = 1.0 * uncompressed.length() * loops;
//        double seconds = (System.nanoTime() - start) / 1e9;
//        System.out.println(toHumanReadableSpeed((long) (bytes / seconds)));
//
//        System.out.println(sum);
    }

    private static void benchmarkJPountz(int loops, String file)
            throws IOException
    {
        System.out.println("Benchmarking jpountz");

        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();

        byte[] uncompressed = Files.readAllBytes(Paths.get(file));
        byte[] compressed = new byte[compressor.maxCompressedLength(uncompressed.length)];

        int compressedSize = compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0);

        long sum = 0;
        System.out.println("Warming up compressor");
        // warmup
        for (int i = 0; i < loops; i++) {
            sum += compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0);
        }

        System.out.println("Benchmarking compressor");
        long start = System.nanoTime();
        for (int i = 0; i < loops; i++) {
            sum += compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0);
        }
        double compressSeconds = (System.nanoTime() - start) / 1e9;

        // warmup
        System.out.println("Warming up decompressor");
        for (int i = 0; i < loops; i++) {
            sum += decompressor.decompress(compressed, 0, compressedSize, uncompressed, 0);
        }

        System.out.println("Benchmarking decompressor");
        start = System.nanoTime();
        for (int i = 0; i < loops; i++) {
            sum += decompressor.decompress(compressed, 0, compressedSize, uncompressed, 0);
        }
        double decompressSeconds = (System.nanoTime() - start) / 1e9;

        double bytes = 1.0 * uncompressed.length * loops;
        System.out.printf("Compress: %s, Uncompress: %s\n",
                toHumanReadableSpeed((long) (bytes / compressSeconds)),
                toHumanReadableSpeed((long) (bytes / decompressSeconds)));

    }

    private static void benchmarkAirlift(int loops, String file)
            throws IOException
    {
        System.out.println("Benchmarking airlift");
        Slice uncompressed = Slices.copyOf(Slices.mapFileReadOnly(new File(file)));
        Slice compressed = Slices.allocate(Lz4CompressorNew.maxCompressedLength(uncompressed.length()));

        Lz4CompressorNew compressor = new Lz4CompressorNew();
        Lz4Decompressor decompressor = new Lz4Decompressor();

        int compressedSize = compressor.compress(uncompressed, 0, uncompressed.length(), compressed, 0);

        long sum = 0;
        System.out.println("Warming up compressor");
        // warmup
        for (int i = 0; i < loops; i++) {
            sum += compressor.compress(uncompressed, 0, uncompressed.length(), compressed, 0);
        }

        System.out.println("Benchmarking compressor");
        long start = System.nanoTime();
        for (int i = 0; i < loops; i++) {
            sum += compressor.compress(uncompressed, 0, uncompressed.length(), compressed, 0);
        }
        double compressSeconds = (System.nanoTime() - start) / 1e9;

        // warmup
        System.out.println("Warming up decompressor");
        for (int i = 0; i < loops; i++) {
            sum += decompressor.uncompress(compressed, 0, compressedSize, uncompressed, 0);
        }

        System.out.println("Benchmarking decompressor");
        start = System.nanoTime();
        for (int i = 0; i < loops; i++) {
            sum += decompressor.uncompress(compressed, 0, compressedSize, uncompressed, 0);
        }
        double decompressSeconds = (System.nanoTime() - start) / 1e9;

        double bytes = 1.0 * uncompressed.length() * loops;
        System.out.printf("Compress: %s, Uncompress: %s\n",
                toHumanReadableSpeed((long) (bytes / compressSeconds)),
                toHumanReadableSpeed((long) (bytes / decompressSeconds)));
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
