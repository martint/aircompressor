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

import static io.airlift.compress.zstd.ZstdFrameCompressor.MAX_BLOCK_SIZE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class CompressionContext
{
    private static final int ZSTD_BLOCKSIZELOG_MAX = 17;
    private static final int ZSTD_BLOCKSIZE_MAX = (1 << ZSTD_BLOCKSIZELOG_MAX);   /* define, for static allocation */
    private static final int HUF_WORKSPACE_SIZE = (6 << 10);

    public BlockState blockState;
    public MatchState matchState;
    public SequenceStore sequenceStore;

    public final int blockSize;

    public CompressionContext(CompressionParameters parameters, int inputSize)
    {
        int windowSize = Math.max(1, Math.min(1 << parameters.getWindowLog(), inputSize));
        int blockSize = Math.min(MAX_BLOCK_SIZE, windowSize);
        int divider = (parameters.getSearchLength() == 3) ? 3 : 4;

        int maxSequences = blockSize / divider;

        this.blockSize = blockSize;
        sequenceStore = new SequenceStore(blockSize, maxSequences);

        matchState = new MatchState(parameters);
        matchState.window = new Window();
        matchState.window.baseAddress = ARRAY_BYTE_BASE_OFFSET; // TODO: needs to be set to the beginning of input buffer/offset ?

        blockState = new BlockState();
    }

//    public void initialize(CompressionParameters parameters, int inputSize)
//    {
//        int maxNbSeq = blockSize / divider;
//        int tokenSpace = blockSize + 11*maxNbSeq;
//        int matchStateSize = 0; // TODO: ZSTD_sizeof_matchState(&params.cParams, /* forCCtx */ 1);
//
//        int entropySpace = HUF_WORKSPACE_SIZE;
//        int blockStateSpace = 2 * sizeof(ZSTD_compressedBlockState_t);

//        int neededSpace = entropySpace + blockStateSpace + matchStateSize + tokenSpace;

        /*

//        int const workSpaceTooLarge = zc->workSpaceSize > ZSTD_WORKSPACETOOLARGE_FACTOR * neededSpace;
//        int const workSpaceWasteful = workSpaceTooLarge && (zc->workSpaceOversizedDuration > ZSTD_WORKSPACETOOLARGE_MAXDURATION);
//        zc->workSpaceOversizedDuration = workSpaceTooLarge ? zc->workSpaceOversizedDuration+1 : 0;

//        if (workSpaceWasteful) {
//            if (zc->staticSize) return ERROR(memory_allocation);
//
//            zc->workSpaceSize = 0;
//            ZSTD_free(zc->workSpace, zc->customMem);
//            zc->workSpaceSize = neededSpace;
//            zc->workSpaceOversizedDuration = 0;
//            ptr = zc->workSpace;
//
//            assert(zc->workSpaceSize >= 2 * sizeof(ZSTD_compressedBlockState_t));
//            zc->blockState.prevCBlock = (ZSTD_compressedBlockState_t*)zc->workSpace;
//            zc->blockState.nextCBlock = zc->blockState.prevCBlock + 1;
//            ptr = zc->blockState.nextCBlock + 1;
//            zc->entropyWorkspace = (U32*)ptr;
//        }


//        zc->appliedParams = params;
//        zc->pledgedSrcSizePlusOne = pledgedSrcSize+1;
        zc->consumedSrcSize = 0;
        zc->producedCSize = 0;

        if (pledgedSrcSize == ZSTD_CONTENTSIZE_UNKNOWN) {
            zc->appliedParams.fParams.contentSizeFlag = 0;
        }
        zc->blockSize = blockSize;

//        XXH64_reset(&zc->xxhState, 0);
        zc->stage = ZSTDcs_init;
        zc->dictID = 0;

        ZSTD_reset_compressedBlockState(zc->blockState.prevCBlock);

        ptr = zc->entropyWorkspace + HUF_WORKSPACE_SIZE_U32;

        ptr = ZSTD_reset_matchState(&zc->blockState.matchState, ptr, &params.cParams, crp,  1);

        zc->seqStore.sequencesStart = (seqDef*)ptr;
        ptr = zc->seqStore.sequencesStart + maxNbSeq;
        zc->seqStore.llCode = (BYTE*) ptr;
        zc->seqStore.mlCode = zc->seqStore.llCode + maxNbSeq;
        zc->seqStore.ofCode = zc->seqStore.mlCode + maxNbSeq;
        zc->seqStore.litStart = zc->seqStore.ofCode + maxNbSeq;
        ptr = zc->seqStore.litStart + blockSize;

        */
//    }
}
