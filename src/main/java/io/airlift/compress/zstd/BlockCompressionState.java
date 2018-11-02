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

import java.util.Arrays;

class BlockCompressionState
{
    public final int[] hashTable;
    public final int[] chainTable;

    public final long baseAddress;

    // starting point of the window with respect to baseAddress
    int windowBaseOffset;

    public BlockCompressionState(CompressionParameters parameters, long baseAddress)
    {
        this.baseAddress = baseAddress;
        hashTable = new int[1 << parameters.getHashLog()];
        chainTable = new int[1 << parameters.getChainLog()]; // TODO: chain table not used by Strategy.FAST
    }

    public void reset()
    {
        Arrays.fill(hashTable, 0);
        Arrays.fill(chainTable, 0);
    }

    public void enforceMaxDistance(long inputLimit, int maxDistance)
    {
        int distance = (int) (inputLimit - baseAddress);

        int newOffset = distance - maxDistance;
        if (windowBaseOffset < newOffset) {
            windowBaseOffset = newOffset;
        }
    }

//    void reset(CompressionParameters parameters, boolean forContext)
//    {
//        int hashSize = 1 << parameters.getHashLog();
//        int chainSize = 1 << parameters.getChainLog();  // TODO: chain table not used by Strategy.FAST
//
//        int hashLog3 = (forContext && parameters.getSearchLength() == 3) ? Math.min(ZSTD_HASHLOG3_MAX, parameters.getWindowLog()) : 0;
//        int h3Size = 1 << hashLog3;
//        int tableSpace = (chainSize + hashSize + h3Size) * SizeOf.SIZE_OF_INT;

//        ms->hashLog3 = hashLog3;
//        memset(&ms->window, 0, sizeof(ms->window));
//        ZSTD_invalidateMatchState(ms);

        /* opt parser space */
//        if (forContext && (parameters.getStrategy() == CompressionParameters.Strategy.BTOPT || (parameters.getStrategy() == CompressionParameters.Strategy.BTULTRA))) {
//            ms->opt.litFreq = (U32*)ptr;
//            ms->opt.litLengthFreq = ms->opt.litFreq + (1<<Litbits);
//            ms->opt.matchLengthFreq = ms->opt.litLengthFreq + (MaxLL+1);
//            ms->opt.offCodeFreq = ms->opt.matchLengthFreq + (MaxML+1);
//            ptr = ms->opt.offCodeFreq + (MaxOff+1);
//            ms->opt.matchTable = (ZSTD_match_t*)ptr;
//            ptr = ms->opt.matchTable + ZSTD_OPT_NUM+1;
//            ms->opt.priceTable = (ZSTD_optimal_t*)ptr;
//            ptr = ms->opt.priceTable + ZSTD_OPT_NUM+1;
//        }

//        /* table Space */
//        int[] hashTable = new int[hashSize];
//        int[] chainTable = new int[chainSize];
//        int[] hashTable3 = new int[h3Size];
//    }
}
