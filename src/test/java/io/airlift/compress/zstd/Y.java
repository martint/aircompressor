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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Y
{
    public static void main(String[] args)
            throws FileNotFoundException
    {
        BufferedReader reader = new BufferedReader(new FileReader("files.txt"));
        ZstdCompressor compressor = new ZstdCompressor();
        ZstdDecompressor decompressor = new ZstdDecompressor();

        reader.lines().forEach(name -> {
            Path path = Paths.get(name);

            byte[] input = null;
            try {
                if (Files.size(path) > Integer.MAX_VALUE / 4) {
                    System.out.println("File too large: " + path);
                    return;
                }

                input = Files.readAllBytes(path);
            }
            catch (Exception e) {
                System.out.println("Error opening: " + path);
                // ignore
            }

            if (input != null) {

                try {
                    byte[] output = new byte[compressor.maxCompressedLength(input.length)];
                    int compressedSize = compressor.compress(input, 0, input.length, output, 0, output.length);
                    decompressor.decompress(output, 0, compressedSize, input, 0, input.length);
                }
                catch (Exception e) {
                    System.err.println("Failed: " + name);
                    e.printStackTrace();
                }
            }
        });
    }
}
