package io.airlift.compress;

import io.airlift.compress.slice.UnsafeSlice;

public class Lz4
{
    public static int uncompressDirectMemory(UnsafeSlice compressed, int compressedOffset, int compressedSize, UnsafeSlice uncompressed, int uncompressedOffset)
            throws CorruptionException
    {
        return Lz4DirectMemoryDecompressor.uncompress(compressed, compressedOffset, compressedSize, uncompressed, uncompressedOffset);
    }

}
