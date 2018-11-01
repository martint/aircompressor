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

    public static void print(String format)
    {
        if (ENABLED) {
            printWithArgs(format);
        }
    }

    public static void print(String format, long arg1)
    {
        if (ENABLED) {
            printWithArgs(format, arg1);
        }
    }

    public static void print(String format, long arg1, long arg2)
    {
        if (ENABLED) {
            printWithArgs(format, arg1, arg2);
        }
    }

    public static void print(String format, long arg1, long arg2, long arg3)
    {
        if (ENABLED) {
            printWithArgs(format, arg1, arg2, arg3);
        }
    }

    public static void print(String format, long arg1, long arg2, long arg3, long arg4)
    {
        if (ENABLED) {
            printWithArgs(format, arg1, arg2, arg3, arg4);
        }
    }

    public static void print(String format, long arg1, long arg2, long arg3, long arg4, long arg5)
    {
        if (ENABLED) {
            printWithArgs(format, arg1, arg2, arg3, arg4, arg5);
        }
    }

    private static void printWithArgs(String format, Object... args)
    {
        if (ENABLED) {
            System.err.printf(format + "\n", args);
            out.printf(format + "\n", args);
            out.flush();
        }
    }
}
