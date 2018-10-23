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

import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class DebugLog
{
    private static final boolean ENABLED = false;
    private static final PrintWriter out;

    static {
        try {
            out = new PrintWriter("debug.txt");
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void print(String format, Object... args)
    {
        if (ENABLED) {
            System.err.printf(format + "\n", args);
            out.printf(format + "\n", args);
            out.flush();
        }
    }
}
