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

import com.google.common.io.Files;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.io.IOException;

@State(Scope.Thread)
public class DataSet
{
    @Param({
            "silesia/dickens",
            "silesia/mozilla",
            "silesia/mr",
            "silesia/nci",
            "silesia/ooffice",
            "silesia/osdb",
            "silesia/reymont",
            "silesia/samba",
            "silesia/sao",
            "silesia/webster",
            "silesia/x-ray",
            "silesia/xml",
    })
    private String name;
    private byte[] uncompressed;

    public DataSet()
    {
    }

    public DataSet(String name)
    {
        this.name = name;
    }

    public DataSet(String name, byte[] uncompressed)
    {
        this.name = name;
        this.uncompressed = uncompressed;
    }

    @Setup
    public void loadFile()
            throws IOException
    {
        uncompressed = Files.toByteArray(new File("testdata", name));
    }

    public byte[] getUncompressed()
    {
        return uncompressed;
    }

    public String getName()
    {
        return name;
    }

    public String toString()
    {
        return name;
    }
}
