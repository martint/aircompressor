GIT commit: 90ae50224d15e8dbcb9fa26b9be096366733db8e

#define ZSTD_CURRENT_MAX ((3U << 29) + (1U << ZSTD_WINDOWLOG_MAX))

typedef enum { ZSTD_noDict = 0, ZSTD_extDict = 1, ZSTD_dictMatchState = 2 } ZSTD_dictMode_e;

typedef struct {
    size_t bitContainer;
    unsigned bitPos;
    char*  startPtr;
    char*  ptr;
    char*  endPtr;
} BIT_CStream_t;

typedef struct {
    ptrdiff_t   value;
    const void* stateTable;
    const void* symbolTT;
    unsigned    stateLog;
} FSE_CState_t;

typedef struct {
    ZSTD_compressedBlockState_t* prevCBlock;
    ZSTD_compressedBlockState_t* nextCBlock;
    ZSTD_matchState_t matchState;
} ZSTD_blockState_t;

typedef struct seqDef_s {
    U32 offset;
    U16 litLength;
    U16 matchLength;
} seqDef;

typedef struct {
    seqDef* sequencesStart;
    seqDef* sequences;
    BYTE* litStart;
    BYTE* lit;
    BYTE* llCode;
    BYTE* mlCode;
    BYTE* ofCode;
    U32   longLengthID;   /* 0 == no longLength; 1 == Lit.longLength; 2 == Match.longLength; */
    U32   longLengthPos;
} seqStore_t;

struct ZSTD_CCtx_s {
    ZSTD_compressionStage_e stage;
    ZSTD_CCtx_params requestedParams;
    ZSTD_CCtx_params appliedParams;
    U32   dictID;

    int workSpaceOversizedDuration;
    void* workSpace;
    size_t workSpaceSize;
    size_t blockSize;
    unsigned long long pledgedSrcSizePlusOne;  /* this way, 0 (default) == unknown */
    unsigned long long consumedSrcSize;
    unsigned long long producedCSize;
    XXH64_state_t xxhState;
    size_t staticSize;

    seqStore_t seqStore;      /* sequences storage ptrs */
    ldmState_t ldmState;      /* long distance matching state */
    rawSeq* ldmSequences;     /* Storage for the ldm output sequences */
    size_t maxNbLdmSequences;
    ZSTD_blockState_t blockState;
    U32* entropyWorkspace;  /* entropy workspace of HUF_WORKSPACE_SIZE bytes */

    /* streaming */
    char*  inBuff;
    size_t inBuffSize;
};

static const ZSTD_compressionParameters ZSTD_defaultCParameters[4][ZSTD_MAX_CLEVEL+1] = {
{   /* "default" - guarantees a monotonically increasing memory budget */
    /* W,  C,  H,  S,  L, TL, strat */
    { 19, 12, 13,  1,  6,  1, ZSTD_fast    },  /* base for negative levels */
    { 19, 13, 14,  1,  7,  0, ZSTD_fast    },  /* level  1 */
    { 19, 15, 16,  1,  6,  0, ZSTD_fast    },  /* level  2 */
    { 20, 16, 17,  1,  5,  1, ZSTD_dfast   },  /* level  3 */
    { 20, 18, 18,  1,  5,  1, ZSTD_dfast   },  /* level  4 */
    { 20, 18, 18,  2,  5,  2, ZSTD_greedy  },  /* level  5 */
    { 21, 18, 19,  2,  5,  4, ZSTD_lazy    },  /* level  6 */
    { 21, 18, 19,  3,  5,  8, ZSTD_lazy2   },  /* level  7 */
    { 21, 19, 19,  3,  5, 16, ZSTD_lazy2   },  /* level  8 */
    { 21, 19, 20,  4,  5, 16, ZSTD_lazy2   },  /* level  9 */
    { 21, 20, 21,  4,  5, 16, ZSTD_lazy2   },  /* level 10 */
    { 21, 21, 22,  4,  5, 16, ZSTD_lazy2   },  /* level 11 */
    { 22, 20, 22,  5,  5, 16, ZSTD_lazy2   },  /* level 12 */
    { 22, 21, 22,  4,  5, 32, ZSTD_btlazy2 },  /* level 13 */
    { 22, 21, 22,  5,  5, 32, ZSTD_btlazy2 },  /* level 14 */
    { 22, 22, 22,  6,  5, 32, ZSTD_btlazy2 },  /* level 15 */
    { 22, 21, 22,  4,  5, 48, ZSTD_btopt   },  /* level 16 */
    { 23, 22, 22,  4,  4, 64, ZSTD_btopt   },  /* level 17 */
    { 23, 23, 22,  6,  3,256, ZSTD_btopt   },  /* level 18 */
    { 23, 24, 22,  7,  3,256, ZSTD_btultra },  /* level 19 */
    { 25, 25, 23,  7,  3,256, ZSTD_btultra },  /* level 20 */
    { 26, 26, 24,  7,  3,512, ZSTD_btultra },  /* level 21 */
    { 27, 27, 25,  9,  3,999, ZSTD_btultra },  /* level 22 */
},
{   /* for srcSize <= 256 KB */
    /* W,  C,  H,  S,  L,  T, strat */
    { 18, 12, 13,  1,  5,  1, ZSTD_fast    },  /* base for negative levels */
    { 18, 13, 14,  1,  6,  0, ZSTD_fast    },  /* level  1 */
    { 18, 14, 14,  1,  5,  1, ZSTD_dfast   },  /* level  2 */
    { 18, 16, 16,  1,  4,  1, ZSTD_dfast   },  /* level  3 */
    { 18, 16, 17,  2,  5,  2, ZSTD_greedy  },  /* level  4.*/
    { 18, 18, 18,  3,  5,  2, ZSTD_greedy  },  /* level  5.*/
    { 18, 18, 19,  3,  5,  4, ZSTD_lazy    },  /* level  6.*/
    { 18, 18, 19,  4,  4,  4, ZSTD_lazy    },  /* level  7 */
    { 18, 18, 19,  4,  4,  8, ZSTD_lazy2   },  /* level  8 */
    { 18, 18, 19,  5,  4,  8, ZSTD_lazy2   },  /* level  9 */
    { 18, 18, 19,  6,  4,  8, ZSTD_lazy2   },  /* level 10 */
    { 18, 18, 19,  5,  4, 16, ZSTD_btlazy2 },  /* level 11.*/
    { 18, 19, 19,  6,  4, 16, ZSTD_btlazy2 },  /* level 12.*/
    { 18, 19, 19,  8,  4, 16, ZSTD_btlazy2 },  /* level 13 */
    { 18, 18, 19,  4,  4, 24, ZSTD_btopt   },  /* level 14.*/
    { 18, 18, 19,  4,  3, 24, ZSTD_btopt   },  /* level 15.*/
    { 18, 19, 19,  6,  3, 64, ZSTD_btopt   },  /* level 16.*/
    { 18, 19, 19,  8,  3,128, ZSTD_btopt   },  /* level 17.*/
    { 18, 19, 19, 10,  3,256, ZSTD_btopt   },  /* level 18.*/
    { 18, 19, 19, 10,  3,256, ZSTD_btultra },  /* level 19.*/
    { 18, 19, 19, 11,  3,512, ZSTD_btultra },  /* level 20.*/
    { 18, 19, 19, 12,  3,512, ZSTD_btultra },  /* level 21.*/
    { 18, 19, 19, 13,  3,999, ZSTD_btultra },  /* level 22.*/
},
{   /* for srcSize <= 128 KB */
    /* W,  C,  H,  S,  L,  T, strat */
    { 17, 12, 12,  1,  5,  1, ZSTD_fast    },  /* base for negative levels */
    { 17, 12, 13,  1,  6,  0, ZSTD_fast    },  /* level  1 */
    { 17, 13, 15,  1,  5,  0, ZSTD_fast    },  /* level  2 */
    { 17, 15, 16,  2,  5,  1, ZSTD_dfast   },  /* level  3 */
    { 17, 17, 17,  2,  4,  1, ZSTD_dfast   },  /* level  4 */
    { 17, 16, 17,  3,  4,  2, ZSTD_greedy  },  /* level  5 */
    { 17, 17, 17,  3,  4,  4, ZSTD_lazy    },  /* level  6 */
    { 17, 17, 17,  3,  4,  8, ZSTD_lazy2   },  /* level  7 */
    { 17, 17, 17,  4,  4,  8, ZSTD_lazy2   },  /* level  8 */
    { 17, 17, 17,  5,  4,  8, ZSTD_lazy2   },  /* level  9 */
    { 17, 17, 17,  6,  4,  8, ZSTD_lazy2   },  /* level 10 */
    { 17, 17, 17,  7,  4,  8, ZSTD_lazy2   },  /* level 11 */
    { 17, 18, 17,  6,  4, 16, ZSTD_btlazy2 },  /* level 12 */
    { 17, 18, 17,  8,  4, 16, ZSTD_btlazy2 },  /* level 13.*/
    { 17, 18, 17,  4,  4, 32, ZSTD_btopt   },  /* level 14.*/
    { 17, 18, 17,  6,  3, 64, ZSTD_btopt   },  /* level 15.*/
    { 17, 18, 17,  7,  3,128, ZSTD_btopt   },  /* level 16.*/
    { 17, 18, 17,  7,  3,256, ZSTD_btopt   },  /* level 17.*/
    { 17, 18, 17,  8,  3,256, ZSTD_btopt   },  /* level 18.*/
    { 17, 18, 17,  8,  3,256, ZSTD_btultra },  /* level 19.*/
    { 17, 18, 17,  9,  3,256, ZSTD_btultra },  /* level 20.*/
    { 17, 18, 17, 10,  3,256, ZSTD_btultra },  /* level 21.*/
    { 17, 18, 17, 11,  3,512, ZSTD_btultra },  /* level 22.*/
},
{   /* for srcSize <= 16 KB */
    /* W,  C,  H,  S,  L,  T, strat */
    { 14, 12, 13,  1,  5,  1, ZSTD_fast    },  /* base for negative levels */
    { 14, 14, 15,  1,  5,  0, ZSTD_fast    },  /* level  1 */
    { 14, 14, 15,  1,  4,  0, ZSTD_fast    },  /* level  2 */
    { 14, 14, 14,  2,  4,  1, ZSTD_dfast   },  /* level  3.*/
    { 14, 14, 14,  4,  4,  2, ZSTD_greedy  },  /* level  4.*/
    { 14, 14, 14,  3,  4,  4, ZSTD_lazy    },  /* level  5.*/
    { 14, 14, 14,  4,  4,  8, ZSTD_lazy2   },  /* level  6 */
    { 14, 14, 14,  6,  4,  8, ZSTD_lazy2   },  /* level  7 */
    { 14, 14, 14,  8,  4,  8, ZSTD_lazy2   },  /* level  8.*/
    { 14, 15, 14,  5,  4,  8, ZSTD_btlazy2 },  /* level  9.*/
    { 14, 15, 14,  9,  4,  8, ZSTD_btlazy2 },  /* level 10.*/
    { 14, 15, 14,  3,  4, 12, ZSTD_btopt   },  /* level 11.*/
    { 14, 15, 14,  6,  3, 16, ZSTD_btopt   },  /* level 12.*/
    { 14, 15, 14,  6,  3, 24, ZSTD_btopt   },  /* level 13.*/
    { 14, 15, 15,  6,  3, 48, ZSTD_btopt   },  /* level 14.*/
    { 14, 15, 15,  6,  3, 64, ZSTD_btopt   },  /* level 15.*/
    { 14, 15, 15,  6,  3, 96, ZSTD_btopt   },  /* level 16.*/
    { 14, 15, 15,  6,  3,128, ZSTD_btopt   },  /* level 17.*/
    { 14, 15, 15,  8,  3,256, ZSTD_btopt   },  /* level 18.*/
    { 14, 15, 15,  6,  3,256, ZSTD_btultra },  /* level 19.*/
    { 14, 15, 15,  8,  3,256, ZSTD_btultra },  /* level 20.*/
    { 14, 15, 15,  9,  3,256, ZSTD_btultra },  /* level 21.*/
    { 14, 15, 15, 10,  3,512, ZSTD_btultra },  /* level 22.*/
},
};

typedef struct {
    unsigned windowLog;      /**< largest match distance : larger == more compression, more memory needed during decompression */
    unsigned chainLog;       /**< fully searched segment : larger == more compression, slower, more memory (useless for fast) */
    unsigned hashLog;        /**< dispatch table : larger == faster, more memory */
    unsigned searchLog;      /**< nb of searches : larger == more compression, slower */
    unsigned searchLength;   /**< match length searched : larger == faster decompression, sometimes less compression */
    unsigned targetLength;   /**< acceptable match size for optimal parser (only) : larger == more compression, slower */
    ZSTD_strategy strategy;
} ZSTD_compressionParameters;

typedef struct {
    unsigned contentSizeFlag; /**< 1: content size will be in frame header (when known) */
    unsigned checksumFlag;    /**< 1: generate a 32-bits checksum at end of frame, for error detection */
    unsigned noDictIDFlag;    /**< 1: no dictID will be saved into frame header (if dictionary compression) */
} ZSTD_frameParameters;

typedef struct {
    ZSTD_compressionParameters cParams;
    ZSTD_frameParameters fParams;
} ZSTD_parameters;

typedef struct {
    BYTE const* nextSrc;    /* next block here to continue on current prefix */
    BYTE const* base;       /* All regular indexes relative to this position */
    BYTE const* dictBase;   /* extDict indexes relative to this position */
    U32 dictLimit;          /* below that point, need extDict */
    U32 lowLimit;           /* below that point, no more data */
} ZSTD_window_t;

typedef struct ZSTD_matchState_t ZSTD_matchState_t;
struct ZSTD_matchState_t {
    ZSTD_window_t window;   /* State for window round buffer management */
    U32 loadedDictEnd;      /* index of end of dictionary */
    U32 nextToUpdate;       /* index from which to continue table update */
    U32 nextToUpdate3;      /* index from which to continue table update */
    U32 hashLog3;           /* dispatch table : larger == faster, more memory */
    U32* hashTable;
    U32* hashTable3;
    U32* chainTable;
    optState_t opt;         /* optimal parser state */
    const ZSTD_matchState_t *dictMatchState;
};

typedef enum { zop_dynamic=0, zop_predef } ZSTD_OptPrice_e;

typedef struct {
    /* All tables are allocated inside cctx->workspace by ZSTD_resetCCtx_internal() */
    U32* litFreq;                /* table of literals statistics, of size 256 */
    U32* litLengthFreq;          /* table of litLength statistics, of size (MaxLL+1) */
    U32* matchLengthFreq;        /* table of matchLength statistics, of size (MaxML+1) */
    U32* offCodeFreq;            /* table of offCode statistics, of size (MaxOff+1) */
    ZSTD_match_t* matchTable;    /* list of found matches, of size ZSTD_OPT_NUM+1 */
    ZSTD_optimal_t* priceTable;  /* All positions tracked by optimal parser, of size ZSTD_OPT_NUM+1 */

    U32  litSum;                 /* nb of literals */
    U32  litLengthSum;           /* nb of litLength codes */
    U32  matchLengthSum;         /* nb of matchLength codes */
    U32  offCodeSum;             /* nb of offset codes */
    U32  litSumBasePrice;        /* to compare to log2(litfreq) */
    U32  litLengthSumBasePrice;  /* to compare to log2(llfreq)  */
    U32  matchLengthSumBasePrice;/* to compare to log2(mlfreq)  */
    U32  offCodeSumBasePrice;    /* to compare to log2(offreq)  */
    ZSTD_OptPrice_e priceType;   /* prices can be determined dynamically, or follow a pre-defined cost structure */
    const ZSTD_entropyCTables_t* symbolCosts;  /* pre-calculated dictionary statistics */
} optState_t;

typedef struct {
    U32 CTable[HUF_CTABLE_SIZE_U32(255)];
    HUF_repeat repeatMode;
} ZSTD_hufCTables_t;

typedef struct {
    FSE_CTable offcodeCTable[FSE_CTABLE_SIZE_U32(OffFSELog, MaxOff)];
    FSE_CTable matchlengthCTable[FSE_CTABLE_SIZE_U32(MLFSELog, MaxML)];
    FSE_CTable litlengthCTable[FSE_CTABLE_SIZE_U32(LLFSELog, MaxLL)];
    FSE_repeat offcode_repeatMode;
    FSE_repeat matchlength_repeatMode;
    FSE_repeat litlength_repeatMode;
} ZSTD_fseCTables_t;

typedef struct {
    ZSTD_hufCTables_t huf;
    ZSTD_fseCTables_t fse;
} ZSTD_entropyCTables_t;

typedef struct {
  ZSTD_entropyCTables_t entropy;
  U32 rep[ZSTD_REP_NUM];
} ZSTD_compressedBlockState_t;

typedef enum {
   FSE_repeat_none,  /**< Cannot use the previous table */
   FSE_repeat_check, /**< Can use the previous table but it must be checked */
   FSE_repeat_valid  /**< Can use the previous table and it is asumed to be valid */
 }

typedef unsigned FSE_CTable;   /* don't allocate that. It's only meant to be more restrictive than void* */

#define FSE_CTABLE_SIZE_U32(maxTableLog, maxSymbolValue)   (1 + (1<<(maxTableLog-1)) + ((maxSymbolValue+1)*2))

#ifndef ZSTD_CLEVEL_DEFAULT
#  define ZSTD_CLEVEL_DEFAULT 3
#endif

#define ZSTD_REP_NUM      3                 /* number of repcodes */

#define ZSTD_MAX_CLEVEL     22

#define ZSTD_CONTENTSIZE_UNKNOWN (0ULL - 1)

#define ZSTD_WINDOWLOG_ABSOLUTEMIN 10

#define ZSTD_WINDOWLOG_MAX_32   30
#define ZSTD_WINDOWLOG_MAX_64   31
#define ZSTD_WINDOWLOG_MAX    ((unsigned)(sizeof(size_t) == 4 ? ZSTD_WINDOWLOG_MAX_32 : ZSTD_WINDOWLOG_MAX_64))
#define ZSTD_WINDOWLOG_MIN      10
#define ZSTD_HASHLOG_MAX      ((ZSTD_WINDOWLOG_MAX < 30) ? ZSTD_WINDOWLOG_MAX : 30)
#define ZSTD_HASHLOG_MIN         6
#define ZSTD_CHAINLOG_MAX_32    29
#define ZSTD_CHAINLOG_MAX_64    30
#define ZSTD_CHAINLOG_MAX     ((unsigned)(sizeof(size_t) == 4 ? ZSTD_CHAINLOG_MAX_32 : ZSTD_CHAINLOG_MAX_64))
#define ZSTD_CHAINLOG_MIN       ZSTD_HASHLOG_MIN
#define ZSTD_HASHLOG3_MAX       17
#define ZSTD_SEARCHLOG_MAX     (ZSTD_WINDOWLOG_MAX-1)
#define ZSTD_SEARCHLOG_MIN       1
#define ZSTD_SEARCHLENGTH_MAX    7   /* only for ZSTD_fast, other strategies are limited to 6 */
#define ZSTD_SEARCHLENGTH_MIN    3   /* only for ZSTD_btopt, other strategies are limited to 4 */

#define FSE_FUNCTION_TYPE BYTE

#define FSE_MAX_TABLELOG  (FSE_MAX_MEMORY_USAGE-2)
#define FSE_MAX_TABLESIZE (1U<<FSE_MAX_TABLELOG)
#define FSE_MAXTABLESIZE_MASK (FSE_MAX_TABLESIZE-1)
#define FSE_DEFAULT_TABLELOG (FSE_DEFAULT_MEMORY_USAGE-2)
#define FSE_MIN_TABLELOG 5

#define ZSTD_OPT_NUM    (1<<12)

#define ZSTD_BLOCKSIZELOG_MAX 17
#define ZSTD_BLOCKSIZE_MAX   (1<<ZSTD_BLOCKSIZELOG_MAX)   /* define, for static allocation */

#define ZSTD_COMPRESSBOUND(srcSize)   ((srcSize) + ((srcSize)>>8) + (((srcSize) < (128<<10)) ? (((128<<10) - (srcSize)) >> 11) /* margin, from 64 to 0 */ : 0))  /* this formula ensures tha
t bound(A) + bound(B) <= bound(A+B) as long as A and B >= 128 KB */

typedef struct {
    int deltaFindState;
    U32 deltaNbBits;
} FSE_symbolCompressionTransform; /* total 8 bytes */

typedef struct {
    U32 off;
    U32 len;
} ZSTD_match_t;

typedef struct {
    int price;
    U32 off;
    U32 mlen;
    U32 litlen;
    U32 rep[ZSTD_REP_NUM];
} ZSTD_optimal_t;


typedef enum { ZSTD_fast=1, ZSTD_dfast, ZSTD_greedy, ZSTD_lazy, ZSTD_lazy2,
               ZSTD_btlazy2, ZSTD_btopt, ZSTD_btultra } ZSTD_strategy;   /* from faster to stronger */


size_t ZSTD_compress(void* dst, size_t dstCapacity, const void* src, size_t srcSize, int compressionLevel)
{
    ZSTD_CCtx cctx;
    memset(&cctx, 0, sizeof(cctx));

    cctx.requestedParams.compressionLevel = ZSTD_CLEVEL_DEFAULT;
    cctx.requestedParams.fParams.contentSizeFlag = 1;

    ZSTD_CCtx_params cctxParams = cctx.requestedParams;
    cctxParams.cParams = ZSTD_getCParams(compressionLevel, srcSize + (!srcSize), 0);
    cctxParams.fParams.contentSizeFlag = 1;
    cctxParams.compressionLevel = ZSTD_CLEVEL_DEFAULT;




    ZSTD_resetCCtx_internal(cctx, cctxParams, srcSize, ZSTDcrp_continue, ZSTDb_not_buffered);
    cctx.dictID = 0;



    size_t const compressedSize;

    ZSTD_matchState_t* ms = &cctx->blockState.matchState;
    size_t fhSize = 0;

    if (cctx->stage==ZSTDcs_created) {
        return ERROR(stage_wrong);   /* missing init (ZSTD_compressBegin) */
    }

    if ((cctx->stage==ZSTDcs_init)) {
        fhSize = ZSTD_writeFrameHeader(dst, dstCapacity, cctx->appliedParams, cctx->pledgedSrcSizePlusOne-1, cctx->dictID);

        if (ZSTD_isError(fhSize)) {
            return fhSize;
        }

        dstCapacity -= fhSize;
        dst = (char*)dst + fhSize;
        cctx->stage = ZSTDcs_ongoing;
    }

    if (!srcSize) {
        return fhSize;  /* do not generate an empty block if no input */
    }
    else {
        if (!ZSTD_window_update(&ms->window, src, srcSize)) {
            ms->nextToUpdate = ms->window.dictLimit;
        }

        size_t const cSize =  ZSTD_compress_frameChunk (cctx, dst, dstCapacity, src, srcSize, 1)

        if (ZSTD_isError(cSize)) {
            compressedSize = cSize;
        }
        else {
            cctx->consumedSrcSize += srcSize;
            cctx->producedCSize += (cSize + fhSize);
            assert(!(cctx->appliedParams.fParams.contentSizeFlag && cctx->pledgedSrcSizePlusOne == 0));
            if (cctx->pledgedSrcSizePlusOne != 0) {  /* control src size */
                if (cctx->consumedSrcSize+1 > cctx->pledgedSrcSizePlusOne) {
                    return ERROR(srcSize_wrong);
                }
            }
            compressedSize = cSize + fhSize;
        }
    }

    if (ZSTD_isError(compressedSize)) {
        return compressedSize;
    }

    size_t endResult;
    endResult = ZSTD_writeEpilogue(cctx, (char*)dst + compressedSize, dstCapacity-compressedSize);
    
    if (ZSTD_isError(endResult)) {
        return endResult;
    }

    assert(!(cctx->appliedParams.fParams.contentSizeFlag && cctx->pledgedSrcSizePlusOne == 0));
    if (cctx->pledgedSrcSizePlusOne != 0) {  /* control src size */
        ZSTD_STATIC_ASSERT(ZSTD_CONTENTSIZE_UNKNOWN == (unsigned long long)-1);
        if (cctx->pledgedSrcSizePlusOne != cctx->consumedSrcSize+1) {
            return ERROR(srcSize_wrong);
        }
    }
    return compressedSize + endResult;
}

/*
*   Ends a frame.
*   @return : nb of bytes written into dst (or an error code) */
static size_t ZSTD_writeEpilogue(ZSTD_CCtx* cctx, void* dst, size_t dstCapacity)
{
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    size_t fhSize = 0;

    if (cctx->stage == ZSTDcs_created) return ERROR(stage_wrong);  /* init missing */

    if (cctx->stage != ZSTDcs_ending) {
        /* write one last empty block, make it the "last" block */
        U32 const cBlockHeader24 = 1 /* last block */ + (((U32)bt_raw)<<1) + 0;
        if (dstCapacity<4) return ERROR(dstSize_tooSmall);
        MEM_writeLE32(op, cBlockHeader24);
        op += ZSTD_blockHeaderSize;
        dstCapacity -= ZSTD_blockHeaderSize;
    }

    if (cctx->appliedParams.fParams.checksumFlag) {
        U32 const checksum = (U32) XXH64_digest(&cctx->xxhState);
        if (dstCapacity<4) return ERROR(dstSize_tooSmall);
        MEM_writeLE32(op, checksum);
        op += 4;
    }

    cctx->stage = ZSTDcs_created;  /* return to "created but no init" status */
    return op-ostart;
}


/*
*  @return ZSTD_compressionParameters structure for a selected compression level, srcSize and dictSize.
*   Size values are optional, provide 0 if not known or unused */
ZSTD_compressionParameters ZSTD_getCParams(int compressionLevel, unsigned long long srcSizeHint, size_t dictSize)
{
    size_t const addedSize = srcSizeHint ? 0 : 500;
    U64 const rSize = srcSizeHint ? srcSizeHint+addedSize : (U64)-1;
    U32 const tableID = (rSize <= 256 KB) + (rSize <= 128 KB) + (rSize <= 16 KB);   /* intentional underflow for srcSizeHint == 0 */
    int row = compressionLevel;
    if (compressionLevel == 0) row = ZSTD_CLEVEL_DEFAULT;   /* 0 == default */
    if (compressionLevel < 0) row = 0;   /* entry 0 is baseline for fast mode */
    if (compressionLevel > ZSTD_MAX_CLEVEL) row = ZSTD_MAX_CLEVEL;

    ZSTD_compressionParameters cp = ZSTD_defaultCParameters[tableID][row];
    if (compressionLevel < 0) {
        cp.targetLength = (unsigned)(-compressionLevel);   /* acceleration factor */
    }

    static const U64 minSrcSize = 513; /* (1<<9) + 1 */
    static const U64 maxWindowResize = 1ULL << (ZSTD_WINDOWLOG_MAX-1);

    if (srcSizeHint == 0) {
        srcSizeHint = ZSTD_CONTENTSIZE_UNKNOWN;  /* 0 == unknown : presumed large */
    }

    /* resize windowLog if input is small enough, to use less memory */
    if ( (srcSizeHint < maxWindowResize) && (0 < maxWindowResize) )  {
        U32 const tSize = (U32)(srcSizeHint);
        static U32 const hashSizeMin = 1 << ZSTD_HASHLOG_MIN;
        U32 const srcLog = (tSize < hashSizeMin) ? ZSTD_HASHLOG_MIN : ZSTD_highbit32(tSize-1) + 1;
        if (cp.windowLog > srcLog) cp.windowLog = srcLog;
    }
    if (cp.hashLog > cp.windowLog+1) cp.hashLog = cp.windowLog+1;
    {   U32 const cycleLog = ZSTD_cycleLog(cp.chainLog, cp.strategy);
        if (cycleLog > cp.windowLog)
            cp.chainLog -= (cycleLog - cp.windowLog);
    }

    if (cp.windowLog < ZSTD_WINDOWLOG_ABSOLUTEMIN) {
        cp.windowLog = ZSTD_WINDOWLOG_ABSOLUTEMIN;  /* required for frame header */
    }

    return cp;
}

static U32 ZSTD_cycleLog(U32 hashLog, ZSTD_strategy strat)
{
    U32 const btScale = ((U32)strat >= (U32)ZSTD_btlazy2);
    return hashLog - btScale;
}

/*
 * check internal buffers exist for streaming if buffPol == ZSTDb_buffered .
 * Note : they are assumed to be correctly sized if ZSTD_equivalentCParams()==1 */
static U32 ZSTD_sufficientBuff(size_t bufferSize1, size_t blockSize1, ZSTD_buffered_policy_e buffPol2, ZSTD_compressionParameters cParams2, U64 pledgedSrcSize)
{
    size_t const windowSize2 = MAX(1, (size_t)MIN(((U64)1 << cParams2.windowLog), pledgedSrcSize));
    size_t const blockSize2 = MIN(ZSTD_BLOCKSIZE_MAX, windowSize2);
//    size_t const neededBufferSize2 = (buffPol2==ZSTDb_buffered) ? windowSize2 + blockSize2 : 0;
    size_t const neededBufferSize2 = 0;

//    return (blockSize2 <= blockSize1) & (neededBufferSize2 <= bufferSize1);
    return (blockSize2 <= blockSize1) & (neededBufferSize2 <= bufferSize1);
}

static U32 ZSTD_equivalentCParams(ZSTD_compressionParameters cParams1,
                                  ZSTD_compressionParameters cParams2)
{
    return (cParams1.hashLog  == cParams2.hashLog)
         & (cParams1.chainLog == cParams2.chainLog)
         & (cParams1.strategy == cParams2.strategy)   /* opt parser space */
         & ((cParams1.searchLength==3) == (cParams2.searchLength==3));  /* hashlog3 space */
}

/** Equivalence for resetCCtx purposes */
static U32 ZSTD_equivalentParams(ZSTD_CCtx_params params1, ZSTD_CCtx_params params2, size_t buffSize1, size_t blockSize1, ZSTD_buffered_policy_e buffPol2, U64 pledgedSrcSize)
{
    return ZSTD_equivalentCParams(params1.cParams, params2.cParams) &&
           ZSTD_equivalentLdmParams(params1.ldmParams, params2.ldmParams) &&
           ZSTD_sufficientBuff(buffSize1, blockSize1, buffPol2, params2.cParams, pledgedSrcSize);
}

//(cctx, cctxParams, srcSize, ZSTDcrp_continue, ZSTDb_not_buffered);
/*
    note : `params` are assumed fully validated at this stage */
static size_t ZSTD_resetCCtx_internal(ZSTD_CCtx* zc,
                                      ZSTD_CCtx_params params,
                                      U64 pledgedSrcSize,
                                      ZSTD_compResetPolicy_e const crp, // ZSTDcrp_continue
                                      ZSTD_buffered_policy_e const zbuff) //ZSTDb_not_buffered
{
//    if (ZSTD_equivalentParams(zc->appliedParams, params, zc->inBuffSize, zc->blockSize, zbuff, pledgedSrcSize)) {
//        zc->workSpaceOversizedDuration += (zc->workSpaceOversizedDuration > 0);   /* if it was too large, it still is */
//        if (zc->workSpaceOversizedDuration <= ZSTD_WORKSPACETOOLARGE_MAXDURATION) {}
//            return ZSTD_continueCCtx(zc, params, pledgedSrcSize);
//        }
//    }

    {
        size_t const windowSize = MAX(1, (size_t)MIN(((U64)1 << params.cParams.windowLog), pledgedSrcSize));
        size_t const blockSize = MIN(ZSTD_BLOCKSIZE_MAX, windowSize);
        U32    const divider = (params.cParams.searchLength==3) ? 3 : 4;
        size_t const maxNbSeq = blockSize / divider;
        size_t const tokenSpace = blockSize + 11*maxNbSeq;
        size_t const matchStateSize = ZSTD_sizeof_matchState(&params.cParams, /* forCCtx */ 1);
        void* ptr;   /* used to partition workSpace */

        /* Check if workSpace is large enough, alloc a new one if needed */
        {   size_t const entropySpace = HUF_WORKSPACE_SIZE;
            size_t const blockStateSpace = 2 * sizeof(ZSTD_compressedBlockState_t);

            size_t const neededSpace = entropySpace + blockStateSpace + matchStateSize + tokenSpace;

            int const workSpaceTooSmall = zc->workSpaceSize < neededSpace;
            int const workSpaceTooLarge = zc->workSpaceSize > ZSTD_WORKSPACETOOLARGE_FACTOR * neededSpace;
            int const workSpaceWasteful = workSpaceTooLarge && (zc->workSpaceOversizedDuration > ZSTD_WORKSPACETOOLARGE_MAXDURATION);
            zc->workSpaceOversizedDuration = workSpaceTooLarge ? zc->workSpaceOversizedDuration+1 : 0;

            if (workSpaceTooSmall || workSpaceWasteful) {
                DEBUGLOG(4, "Need to resize workSpaceSize from %zuKB to %zuKB", zc->workSpaceSize >> 10, neededSpace >> 10);
                /* static cctx : no resize, error out */
                if (zc->staticSize) return ERROR(memory_allocation);

                zc->workSpaceSize = 0;
                ZSTD_free(zc->workSpace, zc->customMem);
                zc->workSpace = ZSTD_malloc(neededSpace, zc->customMem);
                if (zc->workSpace == NULL) return ERROR(memory_allocation);
                zc->workSpaceSize = neededSpace;
                zc->workSpaceOversizedDuration = 0;
                ptr = zc->workSpace;

                /* Statically sized space.
                 * entropyWorkspace never moves,
                 * though prev/next block swap places */
                assert(((size_t)zc->workSpace & 3) == 0);   /* ensure correct alignment */
                assert(zc->workSpaceSize >= 2 * sizeof(ZSTD_compressedBlockState_t));
                zc->blockState.prevCBlock = (ZSTD_compressedBlockState_t*)zc->workSpace;
                zc->blockState.nextCBlock = zc->blockState.prevCBlock + 1;
                ptr = zc->blockState.nextCBlock + 1;
                zc->entropyWorkspace = (U32*)ptr;
        }   }

        /* init params */
        zc->appliedParams = params;
        zc->pledgedSrcSizePlusOne = pledgedSrcSize+1;
        zc->consumedSrcSize = 0;
        zc->producedCSize = 0;
        if (pledgedSrcSize == ZSTD_CONTENTSIZE_UNKNOWN) {
            zc->appliedParams.fParams.contentSizeFlag = 0;
        }
        DEBUGLOG(4, "pledged content size : %u ; flag : %u", (U32)pledgedSrcSize, zc->appliedParams.fParams.contentSizeFlag);
        zc->blockSize = blockSize;

        XXH64_reset(&zc->xxhState, 0);
        zc->stage = ZSTDcs_init;
        zc->dictID = 0;

        ZSTD_reset_compressedBlockState(zc->blockState.prevCBlock);

        ptr = zc->entropyWorkspace + HUF_WORKSPACE_SIZE_U32;

        /* ldm hash table */
        /* initialize bucketOffsets table later for pointer alignment */
        assert(((size_t)ptr & 3) == 0); /* ensure ptr is properly aligned */

        ptr = ZSTD_reset_matchState(&zc->blockState.matchState, ptr, &params.cParams, crp, /* forCCtx */ 1);

        /* sequences storage */
        zc->seqStore.sequencesStart = (seqDef*)ptr;
        ptr = zc->seqStore.sequencesStart + maxNbSeq;
        zc->seqStore.llCode = (BYTE*) ptr;
        zc->seqStore.mlCode = zc->seqStore.llCode + maxNbSeq;
        zc->seqStore.ofCode = zc->seqStore.mlCode + maxNbSeq;
        zc->seqStore.litStart = zc->seqStore.ofCode + maxNbSeq;
        ptr = zc->seqStore.litStart + blockSize;

        /* buffers */
        zc->inBuffSize = 0;
        zc->inBuff = (char*)ptr;

        return 0;
    }
}

/**
 * ZSTD_window_clear():
 * Clears the window containing the history by simply setting it to empty.
 */
MEM_STATIC void ZSTD_window_clear(ZSTD_window_t* window)
{
    size_t const endT = (size_t)(window->nextSrc - window->base);
    U32 const end = (U32)endT;

    window->lowLimit = end;
    window->dictLimit = end;
}

size_t ZSTD_compressBound(size_t srcSize) {
    return ZSTD_COMPRESSBOUND(srcSize);
}

ZSTD_sizeof_matchState(const ZSTD_compressionParameters* const cParams,
                       const U32 forCCtx)
{
    size_t const chainSize = (cParams->strategy == ZSTD_fast) ? 0 : ((size_t)1 << cParams->chainLog);
    size_t const hSize = ((size_t)1) << cParams->hashLog;
    U32    const hashLog3 = (forCCtx && cParams->searchLength==3) ? MIN(ZSTD_HASHLOG3_MAX, cParams->windowLog) : 0;
    size_t const h3Size = ((size_t)1) << hashLog3;
    size_t const tableSpace = (chainSize + hSize + h3Size) * sizeof(U32);
    size_t const optPotentialSpace = ((MaxML+1) + (MaxLL+1) + (MaxOff+1) + (1<<Litbits)) * sizeof(U32)
                          + (ZSTD_OPT_NUM+1) * (sizeof(ZSTD_match_t)+sizeof(ZSTD_optimal_t));
    size_t const optSpace = (forCCtx && ((cParams->strategy == ZSTD_btopt) ||
                                         (cParams->strategy == ZSTD_btultra)))
                                ? optPotentialSpace
                                : 0;
    DEBUGLOG(4, "chainSize: %u - hSize: %u - h3Size: %u",
                (U32)chainSize, (U32)hSize, (U32)h3Size);
    return tableSpace + optSpace;
}

static void ZSTD_reset_compressedBlockState(ZSTD_compressedBlockState_t* bs)
{
    int i;
    for (i = 0; i < ZSTD_REP_NUM; ++i)
        bs->rep[i] = repStartValue[i];
    bs->entropy.huf.repeatMode = HUF_repeat_none;
    bs->entropy.fse.offcode_repeatMode = FSE_repeat_none;
    bs->entropy.fse.matchlength_repeatMode = FSE_repeat_none;
    bs->entropy.fse.litlength_repeatMode = FSE_repeat_none;
}


ZSTD_reset_matchState(ZSTD_matchState_t* ms, void* ptr, const ZSTD_compressionParameters* cParams, ZSTD_compResetPolicy_e const crp, U32 const forCCtx)
{
    size_t const chainSize = (cParams->strategy == ZSTD_fast) ? 0 : ((size_t)1 << cParams->chainLog);
    size_t const hSize = ((size_t)1) << cParams->hashLog;
    U32    const hashLog3 = (forCCtx && cParams->searchLength==3) ? MIN(ZSTD_HASHLOG3_MAX, cParams->windowLog) : 0;
    size_t const h3Size = ((size_t)1) << hashLog3;
    size_t const tableSpace = (chainSize + hSize + h3Size) * sizeof(U32);

    assert(((size_t)ptr & 3) == 0);

    ms->hashLog3 = hashLog3;
    memset(&ms->window, 0, sizeof(ms->window));
    ZSTD_invalidateMatchState(ms);

    /* opt parser space */
    if (forCCtx && ((cParams->strategy == ZSTD_btopt) | (cParams->strategy == ZSTD_btultra))) {
        DEBUGLOG(4, "reserving optimal parser space");
        ms->opt.litFreq = (U32*)ptr;
        ms->opt.litLengthFreq = ms->opt.litFreq + (1<<Litbits);
        ms->opt.matchLengthFreq = ms->opt.litLengthFreq + (MaxLL+1);
        ms->opt.offCodeFreq = ms->opt.matchLengthFreq + (MaxML+1);
        ptr = ms->opt.offCodeFreq + (MaxOff+1);
        ms->opt.matchTable = (ZSTD_match_t*)ptr;
        ptr = ms->opt.matchTable + ZSTD_OPT_NUM+1;
        ms->opt.priceTable = (ZSTD_optimal_t*)ptr;
        ptr = ms->opt.priceTable + ZSTD_OPT_NUM+1;
    }

    /* table Space */
    DEBUGLOG(4, "reset table : %u", crp!=ZSTDcrp_noMemset);
    assert(((size_t)ptr & 3) == 0);  /* ensure ptr is properly aligned */
    if (crp!=ZSTDcrp_noMemset) memset(ptr, 0, tableSpace);   /* reset tables only */
    ms->hashTable = (U32*)(ptr);
    ms->chainTable = ms->hashTable + hSize;
    ms->hashTable3 = ms->chainTable + chainSize;
    ptr = ms->hashTable3 + h3Size;

    assert(((size_t)ptr & 3) == 0);
    return ptr;
}

static void ZSTD_invalidateMatchState(ZSTD_matchState_t* ms)
{
    ZSTD_window_clear(&ms->window);

    ms->nextToUpdate = ms->window.dictLimit + 1;
    ms->nextToUpdate3 = ms->window.dictLimit + 1;
    ms->loadedDictEnd = 0;
    ms->opt.litLengthSum = 0;  /* force reset of btopt stats */
    ms->dictMatchState = NULL;
}

/**
 * ZSTD_window_update():
 * Updates the window by appending [src, src + srcSize) to the window.
 * If it is not contiguous, the current prefix becomes the extDict, and we
 * forget about the extDict. Handles overlap of the prefix and extDict.
 * Returns non-zero if the segment is contiguous.
 */
MEM_STATIC U32 ZSTD_window_update(ZSTD_window_t* window, void const* src, size_t srcSize)
{
    BYTE const* const ip = (BYTE const*)src;
    U32 contiguous = 1;

    /* Check if blocks follow each other */
    if (src != window->nextSrc) {
        /* not contiguous */
        size_t const distanceFromBase = (size_t)(window->nextSrc - window->base);
        DEBUGLOG(5, "Non contiguous blocks, new segment starts at %u", window->dictLimit);
        window->lowLimit = window->dictLimit;
        assert(distanceFromBase == (size_t)(U32)distanceFromBase);  /* should never overflow */
        window->dictLimit = (U32)distanceFromBase;
        window->dictBase = window->base;
        window->base = ip - distanceFromBase;
        // ms->nextToUpdate = window->dictLimit;
        if (window->dictLimit - window->lowLimit < HASH_READ_SIZE) window->lowLimit = window->dictLimit;   /* too small extDict */
        contiguous = 0;
    }

    window->nextSrc = ip + srcSize;
    /* if input and dictionary overlap : reduce dictionary (area presumed modified by input) */
    if ( (ip+srcSize > window->dictBase + window->lowLimit) & (ip < window->dictBase + window->dictLimit)) {
        ptrdiff_t const highInputIdx = (ip + srcSize) - window->dictBase;
        U32 const lowLimitMax = (highInputIdx > (ptrdiff_t)window->dictLimit) ? window->dictLimit : (U32)highInputIdx;
        window->lowLimit = lowLimitMax;
        DEBUGLOG(5, "Overlapping extDict and input : new lowLimit = %u", window->lowLimit);
    }
    return contiguous;
}

void ZSTD_fillHashTable(ZSTD_matchState_t* ms,
                        ZSTD_compressionParameters const* cParams,
                        void const* end, ZSTD_dictTableLoadMethod_e dtlm)
{
    U32* const hashTable = ms->hashTable;
    U32  const hBits = cParams->hashLog;
    U32  const mls = cParams->searchLength;
    const BYTE* const base = ms->window.base;
    const BYTE* ip = base + ms->nextToUpdate;
    const BYTE* const iend = ((const BYTE*)end) - HASH_READ_SIZE;
    const U32 fastHashFillStep = 3;

    /* Always insert every fastHashFillStep position into the hash table.
     * Insert the other positions if their hash entry is empty.
     */
    for (; ip + fastHashFillStep - 1 <= iend; ip += fastHashFillStep) {
        U32 const current = (U32)(ip - base);
        U32 i;
        for (i = 0; i < fastHashFillStep; ++i) {
            size_t const hash = ZSTD_hashPtr(ip + i, hBits, mls);
            if (i == 0 || hashTable[hash] == 0)
                hashTable[hash] = current + i;
            /* Only load extra positions for ZSTD_dtlm_full */
            if (dtlm == ZSTD_dtlm_fast)
                break;
        }
    }
}

void ZSTD_fillDoubleHashTable(ZSTD_matchState_t* ms,
                              ZSTD_compressionParameters const* cParams,
                              void const* end, ZSTD_dictTableLoadMethod_e dtlm)
{
    U32* const hashLarge = ms->hashTable;
    U32  const hBitsL = cParams->hashLog;
    U32  const mls = cParams->searchLength;
    U32* const hashSmall = ms->chainTable;
    U32  const hBitsS = cParams->chainLog;
    const BYTE* const base = ms->window.base;
    const BYTE* ip = base + ms->nextToUpdate;
    const BYTE* const iend = ((const BYTE*)end) - HASH_READ_SIZE;
    const U32 fastHashFillStep = 3;

    /* Always insert every fastHashFillStep position into the hash tables.
     * Insert the other positions into the large hash table if their entry
     * is empty.
     */
    for (; ip + fastHashFillStep - 1 <= iend; ip += fastHashFillStep) {
        U32 const current = (U32)(ip - base);
        U32 i;
        for (i = 0; i < fastHashFillStep; ++i) {
            size_t const smHash = ZSTD_hashPtr(ip + i, hBitsS, mls);
            size_t const lgHash = ZSTD_hashPtr(ip + i, hBitsL, 8);
            if (i == 0)
                hashSmall[smHash] = current + i;
            if (i == 0 || hashLarge[lgHash] == 0)
                hashLarge[lgHash] = current + i;
            /* Only load extra positions for ZSTD_dtlm_full */
            if (dtlm == ZSTD_dtlm_fast)
                break;
        }
    }
}

U32 ZSTD_insertAndFindFirstIndex(
                        ZSTD_matchState_t* ms, ZSTD_compressionParameters const* cParams,
                        const BYTE* ip)
{
    return ZSTD_insertAndFindFirstIndex_internal(ms, cParams, ip, cParams->searchLength);
}

static U32 ZSTD_insertAndFindFirstIndex_internal(
                        ZSTD_matchState_t* ms, ZSTD_compressionParameters const* cParams,
                        const BYTE* ip, U32 const mls)
{
    U32* const hashTable  = ms->hashTable;
    const U32 hashLog = cParams->hashLog;
    U32* const chainTable = ms->chainTable;
    const U32 chainMask = (1 << cParams->chainLog) - 1;
    const BYTE* const base = ms->window.base;
    const U32 target = (U32)(ip - base);
    U32 idx = ms->nextToUpdate;

    while(idx < target) { /* catch up */
        size_t const h = ZSTD_hashPtr(base+idx, hashLog, mls);
        NEXT_IN_CHAIN(idx, chainMask) = hashTable[h];
        hashTable[h] = idx;
        idx++;
    }

    ms->nextToUpdate = target;
    return hashTable[ZSTD_hashPtr(ip, hashLog, mls)];
}

void ZSTD_updateTree(
                ZSTD_matchState_t* ms, ZSTD_compressionParameters const* cParams,
                const BYTE* ip, const BYTE* iend)
{
    ZSTD_updateTree_internal(ms, cParams, ip, iend, cParams->searchLength, ZSTD_noDict);
}

void ZSTD_updateTree_internal(
                ZSTD_matchState_t* ms, ZSTD_compressionParameters const* cParams,
                const BYTE* const ip, const BYTE* const iend,
                const U32 mls, const ZSTD_dictMode_e dictMode)
{
    const BYTE* const base = ms->window.base;
    U32 const target = (U32)(ip - base);
    U32 idx = ms->nextToUpdate;
    DEBUGLOG(5, "ZSTD_updateTree_internal, from %u to %u  (dictMode:%u)",
                idx, target, dictMode);

    while(idx < target)
        idx += ZSTD_insertBt1(ms, cParams, base+idx, iend, mls, dictMode == ZSTD_extDict);
    ms->nextToUpdate = target;
}

size_t HUF_readCTable (HUF_CElt* CTable, U32* maxSymbolValuePtr, const void* src, size_t srcSize)
{
    BYTE huffWeight[HUF_SYMBOLVALUE_MAX + 1];   /* init not required, even though some static analyzer may complain */
    U32 rankVal[HUF_TABLELOG_ABSOLUTEMAX + 1];   /* large enough for values from 0 to 16 */
    U32 tableLog = 0;
    U32 nbSymbols = 0;

    /* get symbol weights */
    CHECK_V_F(readSize, HUF_readStats(huffWeight, HUF_SYMBOLVALUE_MAX+1, rankVal, &nbSymbols, &tableLog, src, srcSize));

    /* check result */
    if (tableLog > HUF_TABLELOG_MAX) return ERROR(tableLog_tooLarge);
    if (nbSymbols > *maxSymbolValuePtr+1) return ERROR(maxSymbolValue_tooSmall);

    /* Prepare base value per rank */
    {   U32 n, nextRankStart = 0;
        for (n=1; n<=tableLog; n++) {
            U32 current = nextRankStart;
            nextRankStart += (rankVal[n] << (n-1));
            rankVal[n] = current;
    }   }

    /* fill nbBits */
    {   U32 n; for (n=0; n<nbSymbols; n++) {
            const U32 w = huffWeight[n];
            CTable[n].nbBits = (BYTE)(tableLog + 1 - w);
    }   }

    /* fill val */
    {   U16 nbPerRank[HUF_TABLELOG_MAX+2]  = {0};  /* support w=0=>n=tableLog+1 */
        U16 valPerRank[HUF_TABLELOG_MAX+2] = {0};
        { U32 n; for (n=0; n<nbSymbols; n++) nbPerRank[CTable[n].nbBits]++; }
        /* determine stating value per rank */
        valPerRank[tableLog+1] = 0;   /* for w==0 */
        {   U16 min = 0;
            U32 n; for (n=tableLog; n>0; n--) {  /* start at n=tablelog <-> w=1 */
                valPerRank[n] = min;     /* get starting value within each rank */
                min += nbPerRank[n];
                min >>= 1;
        }   }
        /* assign value within rank, symbol order */
        { U32 n; for (n=0; n<nbSymbols; n++) CTable[n].val = valPerRank[CTable[n].nbBits]++; }
    }

    *maxSymbolValuePtr = nbSymbols - 1;
    return readSize;
}

size_t FSE_readNCount (short* normalizedCounter, unsigned* maxSVPtr, unsigned* tableLogPtr,
                 const void* headerBuffer, size_t hbSize)
{
    const BYTE* const istart = (const BYTE*) headerBuffer;
    const BYTE* const iend = istart + hbSize;
    const BYTE* ip = istart;
    int nbBits;
    int remaining;
    int threshold;
    U32 bitStream;
    int bitCount;
    unsigned charnum = 0;
    int previous0 = 0;

    if (hbSize < 4) {
        /* This function only works when hbSize >= 4 */
        char buffer[4];
        memset(buffer, 0, sizeof(buffer));
        memcpy(buffer, headerBuffer, hbSize);
        {   size_t const countSize = FSE_readNCount(normalizedCounter, maxSVPtr, tableLogPtr,
                                                    buffer, sizeof(buffer));
            if (FSE_isError(countSize)) return countSize;
            if (countSize > hbSize) return ERROR(corruption_detected);
            return countSize;
    }   }
    assert(hbSize >= 4);

    /* init */
    memset(normalizedCounter, 0, (*maxSVPtr+1) * sizeof(normalizedCounter[0]));   /* all symbols not present in NCount have a frequency of 0 */
    bitStream = MEM_readLE32(ip);
    nbBits = (bitStream & 0xF) + FSE_MIN_TABLELOG;   /* extract tableLog */
    if (nbBits > FSE_TABLELOG_ABSOLUTE_MAX) return ERROR(tableLog_tooLarge);
    bitStream >>= 4;
    bitCount = 4;
    *tableLogPtr = nbBits;
    remaining = (1<<nbBits)+1;
    threshold = 1<<nbBits;
    nbBits++;

    while ((remaining>1) & (charnum<=*maxSVPtr)) {
        if (previous0) {
            unsigned n0 = charnum;
            while ((bitStream & 0xFFFF) == 0xFFFF) {
                n0 += 24;
                if (ip < iend-5) {
                    ip += 2;
                    bitStream = MEM_readLE32(ip) >> bitCount;
                } else {
                    bitStream >>= 16;
                    bitCount   += 16;
            }   }
            while ((bitStream & 3) == 3) {
                n0 += 3;
                bitStream >>= 2;
                bitCount += 2;
            }
            n0 += bitStream & 3;
            bitCount += 2;
            if (n0 > *maxSVPtr) return ERROR(maxSymbolValue_tooSmall);
            while (charnum < n0) normalizedCounter[charnum++] = 0;
            if ((ip <= iend-7) || (ip + (bitCount>>3) <= iend-4)) {
                assert((bitCount >> 3) <= 3); /* For first condition to work */
                ip += bitCount>>3;
                bitCount &= 7;
                bitStream = MEM_readLE32(ip) >> bitCount;
            } else {
                bitStream >>= 2;
        }   }
        {   int const max = (2*threshold-1) - remaining;
            int count;

            if ((bitStream & (threshold-1)) < (U32)max) {
                count = bitStream & (threshold-1);
                bitCount += nbBits-1;
            } else {
                count = bitStream & (2*threshold-1);
                if (count >= threshold) count -= max;
                bitCount += nbBits;
            }

            count--;   /* extra accuracy */
            remaining -= count < 0 ? -count : count;   /* -1 means +1 */
            normalizedCounter[charnum++] = (short)count;
            previous0 = !count;
            while (remaining < threshold) {
                nbBits--;
                threshold >>= 1;
            }

            if ((ip <= iend-7) || (ip + (bitCount>>3) <= iend-4)) {
                ip += bitCount>>3;
                bitCount &= 7;
            } else {
                bitCount -= (int)(8 * (iend - 4 - ip));
                ip = iend - 4;
            }
            bitStream = MEM_readLE32(ip) >> (bitCount & 31);
    }   }   /* while ((remaining>1) & (charnum<=*maxSVPtr)) */
    if (remaining != 1) return ERROR(corruption_detected);
    if (bitCount > 32) return ERROR(corruption_detected);
    *maxSVPtr = charnum-1;

    ip += (bitCount+7)>>3;
    return ip-istart;
}

/* FSE_buildCTable_wksp() :
 * Same as FSE_buildCTable(), but using an externally allocated scratch buffer (`workSpace`).
 * wkspSize should be sized to handle worst case situation, which is `1<<max_tableLog * sizeof(FSE_FUNCTION_TYPE)`
 * workSpace must also be properly aligned with FSE_FUNCTION_TYPE requirements
 */
size_t FSE_buildCTable_wksp(FSE_CTable* ct, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog, void* workSpace, size_t wkspSize)
{
    U32 const tableSize = 1 << tableLog;
    U32 const tableMask = tableSize - 1;
    void* const ptr = ct;
    U16* const tableU16 = ( (U16*) ptr) + 2;
    void* const FSCT = ((U32*)ptr) + 1 /* header */ + (tableLog ? tableSize>>1 : 1) ;
    FSE_symbolCompressionTransform* const symbolTT = (FSE_symbolCompressionTransform*) (FSCT);
    U32 const step = FSE_TABLESTEP(tableSize);
    U32 cumul[FSE_MAX_SYMBOL_VALUE+2];

    FSE_FUNCTION_TYPE* const tableSymbol = (FSE_FUNCTION_TYPE*)workSpace;
    U32 highThreshold = tableSize-1;

    /* CTable header */
    if (((size_t)1 << tableLog) * sizeof(FSE_FUNCTION_TYPE) > wkspSize) return ERROR(tableLog_tooLarge);
    tableU16[-2] = (U16) tableLog;
    tableU16[-1] = (U16) maxSymbolValue;
    assert(tableLog < 16);   /* required for the threshold strategy to work */

    /* For explanations on how to distribute symbol values over the table :
    *  http://fastcompression.blogspot.fr/2014/02/fse-distributing-symbol-values.html */

    /* symbol start positions */
    {   U32 u;
        cumul[0] = 0;
        for (u=1; u<=maxSymbolValue+1; u++) {
            if (normalizedCounter[u-1]==-1) {  /* Low proba symbol */
                cumul[u] = cumul[u-1] + 1;
                tableSymbol[highThreshold--] = (FSE_FUNCTION_TYPE)(u-1);
            } else {
                cumul[u] = cumul[u-1] + normalizedCounter[u-1];
        }   }
        cumul[maxSymbolValue+1] = tableSize+1;
    }

    /* Spread symbols */
    {   U32 position = 0;
        U32 symbol;
        for (symbol=0; symbol<=maxSymbolValue; symbol++) {
            int nbOccurences;
            for (nbOccurences=0; nbOccurences<normalizedCounter[symbol]; nbOccurences++) {
                tableSymbol[position] = (FSE_FUNCTION_TYPE)symbol;
                position = (position + step) & tableMask;
                while (position > highThreshold) position = (position + step) & tableMask;   /* Low proba area */
        }   }

        if (position!=0) return ERROR(GENERIC);   /* Must have gone through all positions */
    }

    /* Build table */
    {   U32 u; for (u=0; u<tableSize; u++) {
        FSE_FUNCTION_TYPE s = tableSymbol[u];   /* note : static analyzer may not understand tableSymbol is properly initialized */
        tableU16[cumul[s]++] = (U16) (tableSize+u);   /* TableU16 : sorted by symbol order; gives next state value */
    }   }

    /* Build Symbol Transformation Table */
    {   unsigned total = 0;
        unsigned s;
        for (s=0; s<=maxSymbolValue; s++) {
            switch (normalizedCounter[s])
            {
            case  0:
                /* filling nonetheless, for compatibility with FSE_getMaxNbBits() */
                symbolTT[s].deltaNbBits = ((tableLog+1) << 16) - (1<<tableLog);
                break;

            case -1:
            case  1:
                symbolTT[s].deltaNbBits = (tableLog << 16) - (1<<tableLog);
                symbolTT[s].deltaFindState = total - 1;
                total ++;
                break;
            default :
                {
                    U32 const maxBitsOut = tableLog - BIT_highbit32 (normalizedCounter[s]-1);
                    U32 const minStatePlus = normalizedCounter[s] << maxBitsOut;
                    symbolTT[s].deltaNbBits = (maxBitsOut << 16) - minStatePlus;
                    symbolTT[s].deltaFindState = total - normalizedCounter[s];
                    total +=  normalizedCounter[s];
    }   }   }   }

#if 0  /* debug : symbol costs */
    DEBUGLOG(5, "\n --- table statistics : ");
    {   U32 symbol;
        for (symbol=0; symbol<=maxSymbolValue; symbol++) {
            DEBUGLOG(5, "%3u: w=%3i,   maxBits=%u, fracBits=%.2f",
                symbol, normalizedCounter[symbol],
                FSE_getMaxNbBits(symbolTT, symbol),
                (double)FSE_bitCost(symbolTT, tableLog, symbol, 8) / 256);
        }
    }
#endif

    return 0;
}

static size_t ZSTD_checkDictNCount(short* normalizedCounter, unsigned dictMaxSymbolValue, unsigned maxSymbolValue) {
    U32 s;
    if (dictMaxSymbolValue < maxSymbolValue) return ERROR(dictionary_corrupted);
    for (s = 0; s <= maxSymbolValue; ++s) {
        if (normalizedCounter[s] == 0) return ERROR(dictionary_corrupted);
    }
    return 0;
}

MEM_STATIC size_t ZSTD_hashPtr(const void* p, U32 hBits, U32 mls)
{
    switch(mls)
    {
    default:
    case 4: return ZSTD_hash4Ptr(p, hBits);
    case 5: return ZSTD_hash5Ptr(p, hBits);
    case 6: return ZSTD_hash6Ptr(p, hBits);
    case 7: return ZSTD_hash7Ptr(p, hBits);
    case 8: return ZSTD_hash8Ptr(p, hBits);
    }
}

static const U32 prime3bytes = 506832829U;
static U32    ZSTD_hash3(U32 u, U32 h) { return ((u << (32-24)) * prime3bytes)  >> (32-h) ; }
MEM_STATIC size_t ZSTD_hash3Ptr(const void* ptr, U32 h) { return ZSTD_hash3(MEM_readLE32(ptr), h); } /* only in zstd_opt.h */

static const U32 prime4bytes = 2654435761U;
static U32    ZSTD_hash4(U32 u, U32 h) { return (u * prime4bytes) >> (32-h) ; }
static size_t ZSTD_hash4Ptr(const void* ptr, U32 h) { return ZSTD_hash4(MEM_read32(ptr), h); }

static const U64 prime5bytes = 889523592379ULL;
static size_t ZSTD_hash5(U64 u, U32 h) { return (size_t)(((u  << (64-40)) * prime5bytes) >> (64-h)) ; }
static size_t ZSTD_hash5Ptr(const void* p, U32 h) { return ZSTD_hash5(MEM_readLE64(p), h); }

static const U64 prime6bytes = 227718039650203ULL;
static size_t ZSTD_hash6(U64 u, U32 h) { return (size_t)(((u  << (64-48)) * prime6bytes) >> (64-h)) ; }
static size_t ZSTD_hash6Ptr(const void* p, U32 h) { return ZSTD_hash6(MEM_readLE64(p), h); }

static const U64 prime7bytes = 58295818150454627ULL;
static size_t ZSTD_hash7(U64 u, U32 h) { return (size_t)(((u  << (64-56)) * prime7bytes) >> (64-h)) ; }
static size_t ZSTD_hash7Ptr(const void* p, U32 h) { return ZSTD_hash7(MEM_readLE64(p), h); }

static const U64 prime8bytes = 0xCF1BBCDCB7A56463ULL;
static size_t ZSTD_hash8(U64 u, U32 h) { return (size_t)(((u) * prime8bytes) >> (64-h)) ; }
static size_t ZSTD_hash8Ptr(const void* p, U32 h) { return ZSTD_hash8(MEM_readLE64(p), h); }

/** ZSTD_insertBt1() : add one or multiple positions to tree.
 *  ip : assumed <= iend-8 .
 * @return : nb of positions added */
static U32 ZSTD_insertBt1(
                ZSTD_matchState_t* ms, ZSTD_compressionParameters const* cParams,
                const BYTE* const ip, const BYTE* const iend,
                U32 const mls, const int extDict)
{
    U32*   const hashTable = ms->hashTable;
    U32    const hashLog = cParams->hashLog;
    size_t const h  = ZSTD_hashPtr(ip, hashLog, mls);
    U32*   const bt = ms->chainTable;
    U32    const btLog  = cParams->chainLog - 1;
    U32    const btMask = (1 << btLog) - 1;
    U32 matchIndex = hashTable[h];
    size_t commonLengthSmaller=0, commonLengthLarger=0;
    const BYTE* const base = ms->window.base;
    const BYTE* const dictBase = ms->window.dictBase;
    const U32 dictLimit = ms->window.dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const BYTE* match;
    const U32 current = (U32)(ip-base);
    const U32 btLow = btMask >= current ? 0 : current - btMask;
    U32* smallerPtr = bt + 2*(current&btMask);
    U32* largerPtr  = smallerPtr + 1;
    U32 dummy32;   /* to be nullified at the end */
    U32 const windowLow = ms->window.lowLimit;
    U32 const matchLow = windowLow ? windowLow : 1;
    U32 matchEndIdx = current+8+1;
    size_t bestLength = 8;
    U32 nbCompares = 1U << cParams->searchLog;
#ifdef ZSTD_C_PREDICT
    U32 predictedSmall = *(bt + 2*((current-1)&btMask) + 0);
    U32 predictedLarge = *(bt + 2*((current-1)&btMask) + 1);
    predictedSmall += (predictedSmall>0);
    predictedLarge += (predictedLarge>0);
#endif /* ZSTD_C_PREDICT */

    DEBUGLOG(8, "ZSTD_insertBt1 (%u)", current);

    assert(ip <= iend-8);   /* required for h calculation */
    hashTable[h] = current;   /* Update Hash Table */

    while (nbCompares-- && (matchIndex >= matchLow)) {
        U32* const nextPtr = bt + 2*(matchIndex & btMask);
        size_t matchLength = MIN(commonLengthSmaller, commonLengthLarger);   /* guaranteed minimum nb of common bytes */
        assert(matchIndex < current);

#ifdef ZSTD_C_PREDICT   /* note : can create issues when hlog small <= 11 */
        const U32* predictPtr = bt + 2*((matchIndex-1) & btMask);   /* written this way, as bt is a roll buffer */
        if (matchIndex == predictedSmall) {
            /* no need to check length, result known */
            *smallerPtr = matchIndex;
            if (matchIndex <= btLow) { smallerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            smallerPtr = nextPtr+1;               /* new "smaller" => larger of match */
            matchIndex = nextPtr[1];              /* new matchIndex larger than previous (closer to current) */
            predictedSmall = predictPtr[1] + (predictPtr[1]>0);
            continue;
        }
        if (matchIndex == predictedLarge) {
            *largerPtr = matchIndex;
            if (matchIndex <= btLow) { largerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            largerPtr = nextPtr;
            matchIndex = nextPtr[0];
            predictedLarge = predictPtr[0] + (predictPtr[0]>0);
            continue;
        }
#endif

        if (!extDict || (matchIndex+matchLength >= dictLimit)) {
            assert(matchIndex+matchLength >= dictLimit);   /* might be wrong if actually extDict */
            match = base + matchIndex;
            matchLength += ZSTD_count(ip+matchLength, match+matchLength, iend);
        } else {
            match = dictBase + matchIndex;
            matchLength += ZSTD_count_2segments(ip+matchLength, match+matchLength, iend, dictEnd, prefixStart);
            if (matchIndex+matchLength >= dictLimit)
                match = base + matchIndex;   /* to prepare for next usage of match[matchLength] */
        }

        if (matchLength > bestLength) {
            bestLength = matchLength;
            if (matchLength > matchEndIdx - matchIndex)
                matchEndIdx = matchIndex + (U32)matchLength;
        }

        if (ip+matchLength == iend) {   /* equal : no way to know if inf or sup */
            break;   /* drop , to guarantee consistency ; miss a bit of compression, but other solutions can corrupt tree */
        }

        if (match[matchLength] < ip[matchLength]) {  /* necessarily within buffer */
            /* match is smaller than current */
            *smallerPtr = matchIndex;             /* update smaller idx */
            commonLengthSmaller = matchLength;    /* all smaller will now have at least this guaranteed common length */
            if (matchIndex <= btLow) { smallerPtr=&dummy32; break; }   /* beyond tree size, stop searching */
            smallerPtr = nextPtr+1;               /* new "candidate" => larger than match, which was smaller than target */
            matchIndex = nextPtr[1];              /* new matchIndex, larger than previous and closer to current */
        } else {
            /* match is larger than current */
            *largerPtr = matchIndex;
            commonLengthLarger = matchLength;
            if (matchIndex <= btLow) { largerPtr=&dummy32; break; }   /* beyond tree size, stop searching */
            largerPtr = nextPtr;
            matchIndex = nextPtr[0];
    }   }

    *smallerPtr = *largerPtr = 0;
    if (bestLength > 384) return MIN(192, (U32)(bestLength - 384));   /* speed optimization */
    assert(matchEndIdx > current + 8);
    return matchEndIdx - (current + 8);
}

/*! HUF_readStats() :
    Read compact Huffman tree, saved by HUF_writeCTable().
    `huffWeight` is destination buffer.
    `rankStats` is assumed to be a table of at least HUF_TABLELOG_MAX U32.
    @return : size read from `src` , or an error Code .
    Note : Needed by HUF_readCTable() and HUF_readDTableX?() .
*/
size_t HUF_readStats(BYTE* huffWeight, size_t hwSize, U32* rankStats,
                     U32* nbSymbolsPtr, U32* tableLogPtr,
                     const void* src, size_t srcSize)
{
    U32 weightTotal;
    const BYTE* ip = (const BYTE*) src;
    size_t iSize;
    size_t oSize;

    if (!srcSize) return ERROR(srcSize_wrong);
    iSize = ip[0];
    /* memset(huffWeight, 0, hwSize);   *//* is not necessary, even though some analyzer complain ... */

    if (iSize >= 128) {  /* special header */
        oSize = iSize - 127;
        iSize = ((oSize+1)/2);
        if (iSize+1 > srcSize) return ERROR(srcSize_wrong);
        if (oSize >= hwSize) return ERROR(corruption_detected);
        ip += 1;
        {   U32 n;
            for (n=0; n<oSize; n+=2) {
                huffWeight[n]   = ip[n/2] >> 4;
                huffWeight[n+1] = ip[n/2] & 15;
    }   }   }
    else  {   /* header compressed with FSE (normal case) */
        FSE_DTable fseWorkspace[FSE_DTABLE_SIZE_U32(6)];  /* 6 is max possible tableLog for HUF header (maybe even 5, to be tested) */
        if (iSize+1 > srcSize) return ERROR(srcSize_wrong);
        oSize = FSE_decompress_wksp(huffWeight, hwSize-1, ip+1, iSize, fseWorkspace, 6);   /* max (hwSize-1) values decoded, as last one is implied */
        if (FSE_isError(oSize)) return oSize;
    }

    /* collect weight stats */
    memset(rankStats, 0, (HUF_TABLELOG_MAX + 1) * sizeof(U32));
    weightTotal = 0;
    {   U32 n; for (n=0; n<oSize; n++) {
            if (huffWeight[n] >= HUF_TABLELOG_MAX) return ERROR(corruption_detected);
            rankStats[huffWeight[n]]++;
            weightTotal += (1 << huffWeight[n]) >> 1;
    }   }
    if (weightTotal == 0) return ERROR(corruption_detected);

    /* get last non-null symbol weight (implied, total must be 2^n) */
    {   U32 const tableLog = BIT_highbit32(weightTotal) + 1;
        if (tableLog > HUF_TABLELOG_MAX) return ERROR(corruption_detected);
        *tableLogPtr = tableLog;
        /* determine last weight */
        {   U32 const total = 1 << tableLog;
            U32 const rest = total - weightTotal;
            U32 const verif = 1 << BIT_highbit32(rest);
            U32 const lastWeight = BIT_highbit32(rest) + 1;
            if (verif != rest) return ERROR(corruption_detected);    /* last value must be a clean power of 2 */
            huffWeight[oSize] = (BYTE)lastWeight;
            rankStats[lastWeight]++;
    }   }

    /* check tree construction validity */
    if ((rankStats[1] < 2) || (rankStats[1] & 1)) return ERROR(corruption_detected);   /* by construction : at least 2 elts of rank 1, must be even */

    /* results */
    *nbSymbolsPtr = (U32)(oSize+1);
    return iSize+1;
}

MEM_STATIC size_t ZSTD_count(const BYTE* pIn, const BYTE* pMatch, const BYTE* const pInLimit)
{
    const BYTE* const pStart = pIn;
    const BYTE* const pInLoopLimit = pInLimit - (sizeof(size_t)-1);

    if (pIn < pInLoopLimit) {
        { size_t const diff = MEM_readST(pMatch) ^ MEM_readST(pIn);
          if (diff) return ZSTD_NbCommonBytes(diff); }
        pIn+=sizeof(size_t); pMatch+=sizeof(size_t);
        while (pIn < pInLoopLimit) {
            size_t const diff = MEM_readST(pMatch) ^ MEM_readST(pIn);
            if (!diff) { pIn+=sizeof(size_t); pMatch+=sizeof(size_t); continue; }
            pIn += ZSTD_NbCommonBytes(diff);
            return (size_t)(pIn - pStart);
    }   }
    if (MEM_64bits() && (pIn<(pInLimit-3)) && (MEM_read32(pMatch) == MEM_read32(pIn))) { pIn+=4; pMatch+=4; }
    if ((pIn<(pInLimit-1)) && (MEM_read16(pMatch) == MEM_read16(pIn))) { pIn+=2; pMatch+=2; }
    if ((pIn<pInLimit) && (*pMatch == *pIn)) pIn++;
    return (size_t)(pIn - pStart);
}

/** ZSTD_count_2segments() :
 *  can count match length with `ip` & `match` in 2 different segments.
 *  convention : on reaching mEnd, match count continue starting from iStart
 */
MEM_STATIC size_t
ZSTD_count_2segments(const BYTE* ip, const BYTE* match,
                     const BYTE* iEnd, const BYTE* mEnd, const BYTE* iStart)
{
    const BYTE* const vEnd = MIN( ip + (mEnd - match), iEnd);
    size_t const matchLength = ZSTD_count(ip, match, vEnd);
    if (match + matchLength != mEnd) return matchLength;
    DEBUGLOG(7, "ZSTD_count_2segments: found a 2-parts match (current length==%zu)", matchLength);
    DEBUGLOG(7, "distance from match beginning to end dictionary = %zi", mEnd - match);
    DEBUGLOG(7, "distance from current pos to end buffer = %zi", iEnd - ip);
    DEBUGLOG(7, "next byte : ip==%02X, istart==%02X", ip[matchLength], *iStart);
    DEBUGLOG(7, "final match length = %zu", matchLength + ZSTD_count(ip+matchLength, iStart, iEnd));
    return matchLength + ZSTD_count(ip+matchLength, iStart, iEnd);
}

size_t FSE_decompress_wksp(void* dst, size_t dstCapacity, const void* cSrc, size_t cSrcSize, FSE_DTable* workSpace, unsigned maxLog)
{
    const BYTE* const istart = (const BYTE*)cSrc;
    const BYTE* ip = istart;
    short counting[FSE_MAX_SYMBOL_VALUE+1];
    unsigned tableLog;
    unsigned maxSymbolValue = FSE_MAX_SYMBOL_VALUE;

    /* normal FSE decoding mode */
    size_t const NCountLength = FSE_readNCount (counting, &maxSymbolValue, &tableLog, istart, cSrcSize);
    if (FSE_isError(NCountLength)) return NCountLength;
    //if (NCountLength >= cSrcSize) return ERROR(srcSize_wrong);   /* too small input size; supposed to be already checked in NCountLength, only remaining case : NCountLength==cSrcSize */
    if (tableLog > maxLog) return ERROR(tableLog_tooLarge);
    ip += NCountLength;
    cSrcSize -= NCountLength;

    CHECK_F( FSE_buildDTable (workSpace, counting, maxSymbolValue, tableLog) );

    return FSE_decompress_usingDTable (dst, dstCapacity, ip, cSrcSize, workSpace);   /* always return, even if it is an error code */
}

static unsigned ZSTD_NbCommonBytes (size_t val)
{
    if (MEM_isLittleEndian()) {
        if (MEM_64bits()) {
#       if defined(_MSC_VER) && defined(_WIN64)
            unsigned long r = 0;
            _BitScanForward64( &r, (U64)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 4)
            return (__builtin_ctzll((U64)val) >> 3);
#       else
            static const int DeBruijnBytePos[64] = { 0, 0, 0, 0, 0, 1, 1, 2,
                                                     0, 3, 1, 3, 1, 4, 2, 7,
                                                     0, 2, 3, 6, 1, 5, 3, 5,
                                                     1, 3, 4, 4, 2, 5, 6, 7,
                                                     7, 0, 1, 2, 3, 3, 4, 6,
                                                     2, 6, 5, 5, 3, 4, 5, 6,
                                                     7, 1, 2, 4, 6, 4, 4, 5,
                                                     7, 2, 6, 5, 7, 6, 7, 7 };
            return DeBruijnBytePos[((U64)((val & -(long long)val) * 0x0218A392CDABBD3FULL)) >> 58];
#       endif
        } else { /* 32 bits */
#       if defined(_MSC_VER)
            unsigned long r=0;
            _BitScanForward( &r, (U32)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_ctz((U32)val) >> 3);
#       else
            static const int DeBruijnBytePos[32] = { 0, 0, 3, 0, 3, 1, 3, 0,
                                                     3, 2, 2, 1, 3, 2, 0, 1,
                                                     3, 3, 1, 2, 2, 2, 2, 0,
                                                     3, 1, 2, 0, 1, 0, 1, 1 };
            return DeBruijnBytePos[((U32)((val & -(S32)val) * 0x077CB531U)) >> 27];
#       endif
        }
    } else {  /* Big Endian CPU */
        if (MEM_64bits()) {
#       if defined(_MSC_VER) && defined(_WIN64)
            unsigned long r = 0;
            _BitScanReverse64( &r, val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 4)
            return (__builtin_clzll(val) >> 3);
#       else
            unsigned r;
            const unsigned n32 = sizeof(size_t)*4;   /* calculate this way due to compiler complaining in 32-bits mode */
            if (!(val>>n32)) { r=4; } else { r=0; val>>=n32; }
            if (!(val>>16)) { r+=2; val>>=8; } else { val>>=24; }
            r += (!val);
            return r;
#       endif
        } else { /* 32 bits */
#       if defined(_MSC_VER)
            unsigned long r = 0;
            _BitScanReverse( &r, (unsigned long)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_clz((U32)val) >> 3);
#       else
            unsigned r;
            if (!(val>>16)) { r=2; val>>=8; } else { r=0; val>>=24; }
            r += (!val);
            return r;
#       endif
    }   }
}

size_t FSE_buildDTable(FSE_DTable* dt, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    void* const tdPtr = dt+1;   /* because *dt is unsigned, 32-bits aligned on 32-bits */
    FSE_DECODE_TYPE* const tableDecode = (FSE_DECODE_TYPE*) (tdPtr);
    U16 symbolNext[FSE_MAX_SYMBOL_VALUE+1];

    U32 const maxSV1 = maxSymbolValue + 1;
    U32 const tableSize = 1 << tableLog;
    U32 highThreshold = tableSize-1;

    /* Sanity Checks */
    if (maxSymbolValue > FSE_MAX_SYMBOL_VALUE) return ERROR(maxSymbolValue_tooLarge);
    if (tableLog > FSE_MAX_TABLELOG) return ERROR(tableLog_tooLarge);

    /* Init, lay down lowprob symbols */
    {   FSE_DTableHeader DTableH;
        DTableH.tableLog = (U16)tableLog;
        DTableH.fastMode = 1;
        {   S16 const largeLimit= (S16)(1 << (tableLog-1));
            U32 s;
            for (s=0; s<maxSV1; s++) {
                if (normalizedCounter[s]==-1) {
                    tableDecode[highThreshold--].symbol = (FSE_FUNCTION_TYPE)s;
                    symbolNext[s] = 1;
                } else {
                    if (normalizedCounter[s] >= largeLimit) DTableH.fastMode=0;
                    symbolNext[s] = normalizedCounter[s];
        }   }   }
        memcpy(dt, &DTableH, sizeof(DTableH));
    }

    /* Spread symbols */
    {   U32 const tableMask = tableSize-1;
        U32 const step = FSE_TABLESTEP(tableSize);
        U32 s, position = 0;
        for (s=0; s<maxSV1; s++) {
            int i;
            for (i=0; i<normalizedCounter[s]; i++) {
                tableDecode[position].symbol = (FSE_FUNCTION_TYPE)s;
                position = (position + step) & tableMask;
                while (position > highThreshold) position = (position + step) & tableMask;   /* lowprob area */
        }   }
        if (position!=0) return ERROR(GENERIC);   /* position must reach all cells once, otherwise normalizedCounter is incorrect */
    }

    /* Build Decoding table */
    {   U32 u;
        for (u=0; u<tableSize; u++) {
            FSE_FUNCTION_TYPE const symbol = (FSE_FUNCTION_TYPE)(tableDecode[u].symbol);
            U32 const nextState = symbolNext[symbol]++;
            tableDecode[u].nbBits = (BYTE) (tableLog - BIT_highbit32(nextState) );
            tableDecode[u].newState = (U16) ( (nextState << tableDecode[u].nbBits) - tableSize);
    }   }

    return 0;
}

size_t FSE_decompress_usingDTable(void* dst, size_t originalSize,
                            const void* cSrc, size_t cSrcSize,
                            const FSE_DTable* dt)
{
    const void* ptr = dt;
    const FSE_DTableHeader* DTableH = (const FSE_DTableHeader*)ptr;
    const U32 fastMode = DTableH->fastMode;

    /* select fast mode (static) */
    if (fastMode) return FSE_decompress_usingDTable_generic(dst, originalSize, cSrc, cSrcSize, dt, 1);
    return FSE_decompress_usingDTable_generic(dst, originalSize, cSrc, cSrcSize, dt, 0);
}

FORCE_INLINE_TEMPLATE size_t FSE_decompress_usingDTable_generic(
          void* dst, size_t maxDstSize,
    const void* cSrc, size_t cSrcSize,
    const FSE_DTable* dt, const unsigned fast)
{
    BYTE* const ostart = (BYTE*) dst;
    BYTE* op = ostart;
    BYTE* const omax = op + maxDstSize;
    BYTE* const olimit = omax-3;

    BIT_DStream_t bitD;
    FSE_DState_t state1;
    FSE_DState_t state2;

    /* Init */
    CHECK_F(BIT_initDStream(&bitD, cSrc, cSrcSize));

    FSE_initDState(&state1, &bitD, dt);
    FSE_initDState(&state2, &bitD, dt);

#define FSE_GETSYMBOL(statePtr) fast ? FSE_decodeSymbolFast(statePtr, &bitD) : FSE_decodeSymbol(statePtr, &bitD)

    /* 4 symbols per loop */
    for ( ; (BIT_reloadDStream(&bitD)==BIT_DStream_unfinished) & (op<olimit) ; op+=4) {
        op[0] = FSE_GETSYMBOL(&state1);

        if (FSE_MAX_TABLELOG*2+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            BIT_reloadDStream(&bitD);

        op[1] = FSE_GETSYMBOL(&state2);

        if (FSE_MAX_TABLELOG*4+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            { if (BIT_reloadDStream(&bitD) > BIT_DStream_unfinished) { op+=2; break; } }

        op[2] = FSE_GETSYMBOL(&state1);

        if (FSE_MAX_TABLELOG*2+7 > sizeof(bitD.bitContainer)*8)    /* This test must be static */
            BIT_reloadDStream(&bitD);

        op[3] = FSE_GETSYMBOL(&state2);
    }

    /* tail */
    /* note : BIT_reloadDStream(&bitD) >= FSE_DStream_partiallyFilled; Ends at exactly BIT_DStream_completed */
    while (1) {
        if (op>(omax-2)) return ERROR(dstSize_tooSmall);
        *op++ = FSE_GETSYMBOL(&state1);
        if (BIT_reloadDStream(&bitD)==BIT_DStream_overflow) {
            *op++ = FSE_GETSYMBOL(&state2);
            break;
        }

        if (op>(omax-2)) return ERROR(dstSize_tooSmall);
        *op++ = FSE_GETSYMBOL(&state2);
        if (BIT_reloadDStream(&bitD)==BIT_DStream_overflow) {
            *op++ = FSE_GETSYMBOL(&state1);
            break;
    }   }

    return op-ostart;
}


static size_t ZSTD_writeFrameHeader(void* dst, size_t dstCapacity, ZSTD_CCtx_params params, U64 pledgedSrcSize, U32 dictID)
{
    BYTE* const op = (BYTE*)dst;
    U32   const dictIDSizeCodeLength = (dictID>0) + (dictID>=256) + (dictID>=65536);   /* 0-3 */
    U32   const dictIDSizeCode = params.fParams.noDictIDFlag ? 0 : dictIDSizeCodeLength;   /* 0-3 */
    U32   const checksumFlag = params.fParams.checksumFlag>0;
    U32   const windowSize = (U32)1 << params.cParams.windowLog;
    U32   const singleSegment = params.fParams.contentSizeFlag && (windowSize >= pledgedSrcSize);
    BYTE  const windowLogByte = (BYTE)((params.cParams.windowLog - ZSTD_WINDOWLOG_ABSOLUTEMIN) << 3);
    U32   const fcsCode = params.fParams.contentSizeFlag ?
                     (pledgedSrcSize>=256) + (pledgedSrcSize>=65536+256) + (pledgedSrcSize>=0xFFFFFFFFU) : 0;  /* 0-3 */
    BYTE  const frameHeaderDecriptionByte = (BYTE)(dictIDSizeCode + (checksumFlag<<2) + (singleSegment<<5) + (fcsCode<<6) );
    size_t pos=0;

    assert(!(params.fParams.contentSizeFlag && pledgedSrcSize == ZSTD_CONTENTSIZE_UNKNOWN));
    if (dstCapacity < ZSTD_frameHeaderSize_max) return ERROR(dstSize_tooSmall);
    DEBUGLOG(4, "ZSTD_writeFrameHeader : dictIDFlag : %u ; dictID : %u ; dictIDSizeCode : %u",
                !params.fParams.noDictIDFlag, dictID,  dictIDSizeCode);

    if (params.format == ZSTD_f_zstd1) {
        MEM_writeLE32(dst, ZSTD_MAGICNUMBER);
        pos = 4;
    }
    op[pos++] = frameHeaderDecriptionByte;
    if (!singleSegment) op[pos++] = windowLogByte;
    switch(dictIDSizeCode)
    {
        default:  assert(0); /* impossible */
        case 0 : break;
        case 1 : op[pos] = (BYTE)(dictID); pos++; break;
        case 2 : MEM_writeLE16(op+pos, (U16)dictID); pos+=2; break;
        case 3 : MEM_writeLE32(op+pos, dictID); pos+=4; break;
    }
    switch(fcsCode)
    {
        default:  assert(0); /* impossible */
        case 0 : if (singleSegment) op[pos++] = (BYTE)(pledgedSrcSize); break;
        case 1 : MEM_writeLE16(op+pos, (U16)(pledgedSrcSize-256)); pos+=2; break;
        case 2 : MEM_writeLE32(op+pos, (U32)(pledgedSrcSize)); pos+=4; break;
        case 3 : MEM_writeLE64(op+pos, (U64)(pledgedSrcSize)); pos+=8; break;
    }
    return pos;
}

static size_t ZSTD_compress_frameChunk (ZSTD_CCtx* cctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize, U32 lastFrameChunk)
{
    size_t blockSize = cctx->blockSize;
    size_t remaining = srcSize;
    const BYTE* ip = (const BYTE*)src;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* op = ostart;
    U32 const maxDist = (U32)1 << cctx->appliedParams.cParams.windowLog;
    assert(cctx->appliedParams.cParams.windowLog <= 31);

    while (remaining) {
        ZSTD_matchState_t* const ms = &cctx->blockState.matchState;
        U32 const lastBlock = lastFrameChunk & (blockSize >= remaining);

        if (dstCapacity < ZSTD_blockHeaderSize + MIN_CBLOCK_SIZE)
            return ERROR(dstSize_tooSmall);   /* not enough space to store compressed block */
        if (remaining < blockSize) blockSize = remaining;

        if (ZSTD_window_needOverflowCorrection(ms->window, ip + blockSize)) {
            U32 const cycleLog = ZSTD_cycleLog(cctx->appliedParams.cParams.chainLog, cctx->appliedParams.cParams.strategy);
            U32 const correction = ZSTD_window_correctOverflow(&ms->window, cycleLog, maxDist, ip);
            ZSTD_STATIC_ASSERT(ZSTD_CHAINLOG_MAX <= 30);
            ZSTD_STATIC_ASSERT(ZSTD_WINDOWLOG_MAX_32 <= 30);
            ZSTD_STATIC_ASSERT(ZSTD_WINDOWLOG_MAX <= 31);

            ZSTD_reduceIndex(cctx, correction);
            if (ms->nextToUpdate < correction) ms->nextToUpdate = 0;
            else ms->nextToUpdate -= correction;
            ms->loadedDictEnd = 0;
            ms->dictMatchState = NULL;
        }

        ZSTD_window_enforceMaxDist(&ms->window, ip + blockSize, maxDist, &ms->loadedDictEnd, &ms->dictMatchState);

        if (ms->nextToUpdate < ms->window.lowLimit) {
            ms->nextToUpdate = ms->window.lowLimit;
        }

        size_t cSize = ZSTD_compressBlock_internal(cctx, op+ZSTD_blockHeaderSize, dstCapacity-ZSTD_blockHeaderSize, ip, blockSize);

        if (ZSTD_isError(cSize)) {
            return cSize;
        }

        if (cSize == 0) {  /* block is not compressible */
            U32 const cBlockHeader24 = lastBlock + (((U32)bt_raw)<<1) + (U32)(blockSize << 3);
            if (blockSize + ZSTD_blockHeaderSize > dstCapacity) return ERROR(dstSize_tooSmall);
            MEM_writeLE32(op, cBlockHeader24);   /* 4th byte will be overwritten */
            memcpy(op + ZSTD_blockHeaderSize, ip, blockSize);
            cSize = ZSTD_blockHeaderSize + blockSize;
        }
        else {
            U32 const cBlockHeader24 = lastBlock + (((U32)bt_compressed)<<1) + (U32)(cSize << 3);
            MEM_writeLE24(op, cBlockHeader24);
            cSize += ZSTD_blockHeaderSize;
        }

        ip += blockSize;
        assert(remaining >= blockSize);
        remaining -= blockSize;
        op += cSize;
        assert(dstCapacity >= cSize);
        dstCapacity -= cSize;
    }

    if (lastFrameChunk && (op>ostart)) {
        cctx->stage = ZSTDcs_ending;
    }
    return op-ostart;
}

static size_t ZSTD_compressBlock_internal(ZSTD_CCtx* zc, void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    ZSTD_matchState_t* const ms = &zc->blockState.matchState;

    if (srcSize < MIN_CBLOCK_SIZE+ZSTD_blockHeaderSize+1) {
        return 0;   /* don't even attempt compression below a certain srcSize */
    }
    ZSTD_resetSeqStore(&(zc->seqStore));
    ms->opt.symbolCosts = &zc->blockState.prevCBlock->entropy;   /* required for optimal parser to read stats from dictionary */

    /* a gap between an attached dict and the current window is not safe,
     * they must remain adjacent, and when that stops being the case, the dict
     * must be unset */
    assert(ms->dictMatchState == NULL || ms->loadedDictEnd == ms->window.dictLimit);

    /* limited update after a very long match */
    {   const BYTE* const base = ms->window.base;
        const BYTE* const istart = (const BYTE*)src;
        const U32 current = (U32)(istart-base);
        if (sizeof(ptrdiff_t)==8) assert(istart - base < (ptrdiff_t)(U32)(-1));   /* ensure no overflow */
        if (current > ms->nextToUpdate + 384)
            ms->nextToUpdate = current - MIN(192, (U32)(current - ms->nextToUpdate - 384));
    }

    /* select and store sequences */
    ZSTD_dictMode_e const dictMode = ZSTD_matchState_dictMode(ms);
    size_t lastLLSize;
    int i;
    for (i = 0; i < ZSTD_REP_NUM; ++i) {
        zc->blockState.nextCBlock->rep[i] = zc->blockState.prevCBlock->rep[i];
    }

    ZSTD_blockCompressor const blockCompressor = ZSTD_selectBlockCompressor(zc->appliedParams.cParams.strategy, dictMode);
    lastLLSize = blockCompressor(ms, &zc->seqStore, zc->blockState.nextCBlock->rep, &zc->appliedParams.cParams, src, srcSize);

    const BYTE* const lastLiterals = (const BYTE*)src + srcSize - lastLLSize;
    ZSTD_storeLastLiterals(&zc->seqStore, lastLiterals, lastLLSize);


    /* encode sequences and literals */
    size_t const cSize = ZSTD_compressSequences(&zc->seqStore,
                                &zc->blockState.prevCBlock->entropy, &zc->blockState.nextCBlock->entropy,
                                &zc->appliedParams,
                                dst, dstCapacity,
                                srcSize, zc->entropyWorkspace, zc->bmi2);

    if (ZSTD_isError(cSize) || cSize == 0) {
        return cSize;
    }

    /* confirm repcodes and entropy tables */
    ZSTD_compressedBlockState_t* const tmp = zc->blockState.prevCBlock;
    zc->blockState.prevCBlock = zc->blockState.nextCBlock;
    zc->blockState.nextCBlock = tmp;

    return cSize;
}

/**
 * ZSTD_window_correctOverflow():
 * Reduces the indices to protect from index overflow.
 * Returns the correction made to the indices, which must be applied to every
 * stored index.
 *
 * The least significant cycleLog bits of the indices must remain the same,
 * which may be 0. Every index up to maxDist in the past must be valid.
 * NOTE: (maxDist & cycleMask) must be zero.
 */
MEM_STATIC U32 ZSTD_window_correctOverflow(ZSTD_window_t* window, U32 cycleLog,
                                           U32 maxDist, void const* src)
{
    /* preemptive overflow correction:
     * 1. correction is large enough:
     *    lowLimit > (3<<29) ==> current > 3<<29 + 1<<windowLog
     *    1<<windowLog <= newCurrent < 1<<chainLog + 1<<windowLog
     *
     *    current - newCurrent
     *    > (3<<29 + 1<<windowLog) - (1<<windowLog + 1<<chainLog)
     *    > (3<<29) - (1<<chainLog)
     *    > (3<<29) - (1<<30)             (NOTE: chainLog <= 30)
     *    > 1<<29
     *
     * 2. (ip+ZSTD_CHUNKSIZE_MAX - cctx->base) doesn't overflow:
     *    After correction, current is less than (1<<chainLog + 1<<windowLog).
     *    In 64-bit mode we are safe, because we have 64-bit ptrdiff_t.
     *    In 32-bit mode we are safe, because (chainLog <= 29), so
     *    ip+ZSTD_CHUNKSIZE_MAX - cctx->base < 1<<32.
     * 3. (cctx->lowLimit + 1<<windowLog) < 1<<32:
     *    windowLog <= 31 ==> 3<<29 + 1<<windowLog < 7<<29 < 1<<32.
     */
    U32 const cycleMask = (1U << cycleLog) - 1;
    U32 const current = (U32)((BYTE const*)src - window->base);
    U32 const newCurrent = (current & cycleMask) + maxDist;
    U32 const correction = current - newCurrent;
    assert((maxDist & cycleMask) == 0);
    assert(current > newCurrent);
    /* Loose bound, should be around 1<<29 (see above) */
    assert(correction > 1<<28);

    window->base += correction;
    window->dictBase += correction;
    window->lowLimit -= correction;
    window->dictLimit -= correction;

    DEBUGLOG(4, "Correction of 0x%x bytes to lowLimit=0x%x", correction,
             window->lowLimit);
    return correction;
}

/*! ZSTD_reduceIndex() :
*   rescale all indexes to avoid future overflow (indexes are U32) */
static void ZSTD_reduceIndex (ZSTD_CCtx* zc, const U32 reducerValue)
{
    ZSTD_matchState_t* const ms = &zc->blockState.matchState;
    {   U32 const hSize = (U32)1 << zc->appliedParams.cParams.hashLog;
        ZSTD_reduceTable(ms->hashTable, hSize, reducerValue);
    }

    if (zc->appliedParams.cParams.strategy != ZSTD_fast) {
        U32 const chainSize = (U32)1 << zc->appliedParams.cParams.chainLog;
        if (zc->appliedParams.cParams.strategy == ZSTD_btlazy2)
            ZSTD_reduceTable_btlazy2(ms->chainTable, chainSize, reducerValue);
        else
            ZSTD_reduceTable(ms->chainTable, chainSize, reducerValue);
    }

    if (ms->hashLog3) {
        U32 const h3Size = (U32)1 << ms->hashLog3;
        ZSTD_reduceTable(ms->hashTable3, h3Size, reducerValue);
    }
}

/**
 * ZSTD_window_enforceMaxDist():
 * Updates lowLimit so that:
 *    (srcEnd - base) - lowLimit == maxDist + loadedDictEnd
 *
 * This allows a simple check that index >= lowLimit to see if index is valid.
 * This must be called before a block compression call, with srcEnd as the block
 * source end.
 *
 * If loadedDictEndPtr is not NULL, we set it to zero once we update lowLimit.
 * This is because dictionaries are allowed to be referenced as long as the last
 * byte of the dictionary is in the window, but once they are out of range,
 * they cannot be referenced. If loadedDictEndPtr is NULL, we use
 * loadedDictEnd == 0.
 *
 * In normal dict mode, the dict is between lowLimit and dictLimit. In
 * dictMatchState mode, lowLimit and dictLimit are the same, and the dictionary
 * is below them. forceWindow and dictMatchState are therefore incompatible.
 */
MEM_STATIC void ZSTD_window_enforceMaxDist(ZSTD_window_t* window, void const* srcEnd, U32 maxDist) //, U32* loadedDictEndPtr, const ZSTD_matchState_t** dictMatchStatePtr)
{
    U32 const current = (U32)((BYTE const*)srcEnd - window->base);
    if (current > maxDist) {
        U32 const newLowLimit = current - maxDist;
        if (window->lowLimit < newLowLimit) {
            window->lowLimit = newLowLimit;
        }
        if (window->dictLimit < window->lowLimit) {
            window->dictLimit = window->lowLimit;
        }
    }
}

void ZSTD_resetSeqStore(seqStore_t* ssPtr)
{
    ssPtr->lit = ssPtr->litStart;
    ssPtr->sequences = ssPtr->sequencesStart;
    ssPtr->longLengthID = 0;
}

/* ZSTD_selectBlockCompressor() :
 * Not static, but internal use only (used by long distance matcher)
 * assumption : strat is a valid strategy */
ZSTD_blockCompressor ZSTD_selectBlockCompressor(ZSTD_strategy strat, ZSTD_dictMode_e dictMode)
{
    static const ZSTD_blockCompressor blockCompressor[3][(unsigned)ZSTD_btultra+1] = {
        { ZSTD_compressBlock_fast  /* default for 0 */,
          ZSTD_compressBlock_fast,
          ZSTD_compressBlock_doubleFast,
          ZSTD_compressBlock_greedy,
          ZSTD_compressBlock_lazy,
          ZSTD_compressBlock_lazy2,
          ZSTD_compressBlock_btlazy2,
          ZSTD_compressBlock_btopt,
          ZSTD_compressBlock_btultra },
        { ZSTD_compressBlock_fast_extDict  /* default for 0 */,
          ZSTD_compressBlock_fast_extDict,
          ZSTD_compressBlock_doubleFast_extDict,
          ZSTD_compressBlock_greedy_extDict,
          ZSTD_compressBlock_lazy_extDict,
          ZSTD_compressBlock_lazy2_extDict,
          ZSTD_compressBlock_btlazy2_extDict,
          ZSTD_compressBlock_btopt_extDict,
          ZSTD_compressBlock_btultra_extDict },
        { ZSTD_compressBlock_fast_dictMatchState  /* default for 0 */,
          ZSTD_compressBlock_fast_dictMatchState,
          ZSTD_compressBlock_doubleFast_dictMatchState,
          ZSTD_compressBlock_greedy_dictMatchState,
          ZSTD_compressBlock_lazy_dictMatchState,
          ZSTD_compressBlock_lazy2_dictMatchState,
          ZSTD_compressBlock_btlazy2_dictMatchState,
          ZSTD_compressBlock_btopt_dictMatchState,
          ZSTD_compressBlock_btultra_dictMatchState }
    };
    ZSTD_blockCompressor selectedCompressor;
    ZSTD_STATIC_ASSERT((unsigned)ZSTD_fast == 1);

    assert((U32)strat >= (U32)ZSTD_fast);
    assert((U32)strat <= (U32)ZSTD_btultra);
    selectedCompressor = blockCompressor[(int)dictMode][(U32)strat];
    assert(selectedCompressor != NULL);
    return selectedCompressor;
}

static void ZSTD_storeLastLiterals(seqStore_t* seqStorePtr,
                                   const BYTE* anchor, size_t lastLLSize)
{
    memcpy(seqStorePtr->lit, anchor, lastLLSize);
    seqStorePtr->lit += lastLLSize;
}

MEM_STATIC size_t ZSTD_compressSequences(seqStore_t* seqStorePtr, const ZSTD_entropyCTables_t* prevEntropy, ZSTD_entropyCTables_t* nextEntropy, const ZSTD_CCtx_params* cctxParams, void* dst, size_t dstCapacity, size_t srcSize, U32* workspace, int bmi2)
{
    size_t const cSize = ZSTD_compressSequences_internal( seqStorePtr, prevEntropy, nextEntropy, cctxParams, dst, dstCapacity, workspace, bmi2);
    if (cSize == 0) return 0;
    /* When srcSize <= dstCapacity, there is enough space to write a raw uncompressed block.
     * Since we ran out of space, block must be not compressible, so fall back to raw uncompressed block.
     */
    if ((cSize == ERROR(dstSize_tooSmall)) & (srcSize <= dstCapacity)) {}
        return 0;  /* block not compressed */
    }

    if (ZSTD_isError(cSize)) {
        return cSize;
    }

    /* Check compressibility */
    size_t const maxCSize = srcSize - ZSTD_minGain(srcSize, cctxParams->cParams.strategy);
    if (cSize >= maxCSize) {
        return 0;  /* block not compressed */
    }


    /* We check that dictionaries have offset codes available for the first
     * block. After the first block, the offcode table might not have large
     * enough codes to represent the offsets in the data.
     */
    if (nextEntropy->fse.offcode_repeatMode == FSE_repeat_valid) {
        nextEntropy->fse.offcode_repeatMode = FSE_repeat_check;
    }

    return cSize;
}

static void ZSTD_reduceTable_btlazy2(U32* const table, U32 const size, U32 const reducerValue)
{
    ZSTD_reduceTable_internal(table, size, reducerValue, 1);
}

ZSTD_reduceTable_internal (U32* const table, U32 const size, U32 const reducerValue, int const preserveMark)
{
    int const nbRows = (int)size / ZSTD_ROWSIZE;
    int cellNb = 0;
    int rowNb;
    assert((size & (ZSTD_ROWSIZE-1)) == 0);  /* multiple of ZSTD_ROWSIZE */
    assert(size < (1U<<31));   /* can be casted to int */
    for (rowNb=0 ; rowNb < nbRows ; rowNb++) {
        int column;
        for (column=0; column<ZSTD_ROWSIZE; column++) {
            if (preserveMark) {
                U32 const adder = (table[cellNb] == ZSTD_DUBT_UNSORTED_MARK) ? reducerValue : 0;
                table[cellNb] += adder;
            }
            if (table[cellNb] < reducerValue) table[cellNb] = 0;
            else table[cellNb] -= reducerValue;
            cellNb++;
    }   }
}

static void ZSTD_reduceTable_btlazy2(U32* const table, U32 const size, U32 const reducerValue)
{
    ZSTD_reduceTable_internal(table, size, reducerValue, 1);
}

/*! ZSTD_storeSeq() :
 *  Store a sequence (literal length, literals, offset code and match length code) into seqStore_t.
 *  `offsetCode` : distance to match + 3 (values 1-3 are repCodes).
 *  `mlBase` : matchLength - MINMATCH
*/
MEM_STATIC void ZSTD_storeSeq(seqStore_t* seqStorePtr, size_t litLength, const void* literals, U32 offsetCode, size_t mlBase)
{
    /* copy Literals */
    assert(seqStorePtr->lit + litLength <= seqStorePtr->litStart + 128 KB);
    ZSTD_wildcopy(seqStorePtr->lit, literals, litLength);
    seqStorePtr->lit += litLength;

    /* literal Length */
    if (litLength>0xFFFF) {
        assert(seqStorePtr->longLengthID == 0); /* there can only be a single long length */
        seqStorePtr->longLengthID = 1;
        seqStorePtr->longLengthPos = (U32)(seqStorePtr->sequences - seqStorePtr->sequencesStart);
    }
    seqStorePtr->sequences[0].litLength = (U16)litLength;

    /* match offset */
    seqStorePtr->sequences[0].offset = offsetCode + 1;

    /* match Length */
    if (mlBase>0xFFFF) {
        assert(seqStorePtr->longLengthID == 0); /* there can only be a single long length */
        seqStorePtr->longLengthID = 2;
        seqStorePtr->longLengthPos = (U32)(seqStorePtr->sequences - seqStorePtr->sequencesStart);
    }
    seqStorePtr->sequences[0].matchLength = (U16)mlBase;

    seqStorePtr->sequences++;
}

/**
 * ZSTD_window_needOverflowCorrection():
 * Returns non-zero if the indices are getting too large and need overflow
 * protection.
 */
MEM_STATIC U32 ZSTD_window_needOverflowCorrection(ZSTD_window_t const window,
                                                  void const* srcEnd)
{
    U32 const current = (U32)((BYTE const*)srcEnd - window.base);
    return current > ZSTD_CURRENT_MAX;
}

MEM_STATIC size_t ZSTD_compressSequences_internal(seqStore_t* seqStorePtr, ZSTD_entropyCTables_t const* prevEntropy, ZSTD_entropyCTables_t* nextEntropy, ZSTD_CCtx_params const* cctxParams, void* dst, size_t dstCapacity, U32* workspace, const int bmi2)
{
    const int longOffsets = cctxParams->cParams.windowLog > STREAM_ACCUMULATOR_MIN;
    ZSTD_strategy const strategy = cctxParams->cParams.strategy;
    U32 count[MaxSeq+1];
    FSE_CTable* CTable_LitLength = nextEntropy->fse.litlengthCTable;
    FSE_CTable* CTable_OffsetBits = nextEntropy->fse.offcodeCTable;
    FSE_CTable* CTable_MatchLength = nextEntropy->fse.matchlengthCTable;
    U32 LLtype, Offtype, MLtype;   /* compressed, raw or rle */
    const seqDef* const sequences = seqStorePtr->sequencesStart;
    const BYTE* const ofCodeTable = seqStorePtr->ofCode;
    const BYTE* const llCodeTable = seqStorePtr->llCode;
    const BYTE* const mlCodeTable = seqStorePtr->mlCode;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* const oend = ostart + dstCapacity;
    BYTE* op = ostart;
    size_t const nbSeq = seqStorePtr->sequences - seqStorePtr->sequencesStart;
    BYTE* seqHead;
    BYTE* lastNCount = NULL;

    ZSTD_STATIC_ASSERT(HUF_WORKSPACE_SIZE >= (1<<MAX(MLFSELog,LLFSELog)));

    /* Compress literals */
    {   const BYTE* const literals = seqStorePtr->litStart;
        size_t const litSize = seqStorePtr->lit - literals;
        int const disableLiteralCompression = (cctxParams->cParams.strategy == ZSTD_fast) && (cctxParams->cParams.targetLength > 0);
        size_t const cSize = ZSTD_compressLiterals(
                                    &prevEntropy->huf, &nextEntropy->huf,
                                    cctxParams->cParams.strategy, disableLiteralCompression,
                                    op, dstCapacity,
                                    literals, litSize,
                                    workspace, bmi2);
        if (ZSTD_isError(cSize))
          return cSize;
        assert(cSize <= dstCapacity);
        op += cSize;
    }

    /* Sequences Header */
    if ((oend-op) < 3 /*max nbSeq Size*/ + 1 /*seqHead*/) return ERROR(dstSize_tooSmall);
    if (nbSeq < 0x7F)
        *op++ = (BYTE)nbSeq;
    else if (nbSeq < LONGNBSEQ)
        op[0] = (BYTE)((nbSeq>>8) + 0x80), op[1] = (BYTE)nbSeq, op+=2;
    else
        op[0]=0xFF, MEM_writeLE16(op+1, (U16)(nbSeq - LONGNBSEQ)), op+=3;
    if (nbSeq==0) {
        /* Copy the old tables over as if we repeated them */
        memcpy(&nextEntropy->fse, &prevEntropy->fse, sizeof(prevEntropy->fse));
        return op - ostart;
    }

    /* seqHead : flags for FSE encoding type */
    seqHead = op++;

    /* convert length/distances into codes */
    ZSTD_seqToCodes(seqStorePtr);
    /* build CTable for Literal Lengths */
    {   U32 max = MaxLL;
        size_t const mostFrequent = HIST_countFast_wksp(count, &max, llCodeTable, nbSeq, workspace);   /* can't fail */
        DEBUGLOG(5, "Building LL table");
        nextEntropy->fse.litlength_repeatMode = prevEntropy->fse.litlength_repeatMode;
        LLtype = ZSTD_selectEncodingType(&nextEntropy->fse.litlength_repeatMode, count, max, mostFrequent, nbSeq, LLFSELog, prevEntropy->fse.litlengthCTable, LL_defaultNorm, LL_defaultNormLog, ZSTD_defaultAllowed, strategy);
        assert(set_basic < set_compressed && set_rle < set_compressed);
        assert(!(LLtype < set_compressed && nextEntropy->fse.litlength_repeatMode != FSE_repeat_none)); /* We don't copy tables */
        {   size_t const countSize = ZSTD_buildCTable(op, oend - op, CTable_LitLength, LLFSELog, (symbolEncodingType_e)LLtype,
                                                    count, max, llCodeTable, nbSeq, LL_defaultNorm, LL_defaultNormLog, MaxLL,
                                                    prevEntropy->fse.litlengthCTable, sizeof(prevEntropy->fse.litlengthCTable),
                                                    workspace, HUF_WORKSPACE_SIZE);
            if (ZSTD_isError(countSize)) return countSize;
            if (LLtype == set_compressed)
                lastNCount = op;
            op += countSize;
    }   }
    /* build CTable for Offsets */
    {   U32 max = MaxOff;
        size_t const mostFrequent = HIST_countFast_wksp(count, &max, ofCodeTable, nbSeq, workspace);  /* can't fail */
        /* We can only use the basic table if max <= DefaultMaxOff, otherwise the offsets are too large */
        ZSTD_defaultPolicy_e const defaultPolicy = (max <= DefaultMaxOff) ? ZSTD_defaultAllowed : ZSTD_defaultDisallowed;
        DEBUGLOG(5, "Building OF table");
        nextEntropy->fse.offcode_repeatMode = prevEntropy->fse.offcode_repeatMode;
        Offtype = ZSTD_selectEncodingType(&nextEntropy->fse.offcode_repeatMode, count, max, mostFrequent, nbSeq, OffFSELog, prevEntropy->fse.offcodeCTable, OF_defaultNorm, OF_defaultNormLog, defaultPolicy, strategy);
        assert(!(Offtype < set_compressed && nextEntropy->fse.offcode_repeatMode != FSE_repeat_none)); /* We don't copy tables */
        {   size_t const countSize = ZSTD_buildCTable(op, oend - op, CTable_OffsetBits, OffFSELog, (symbolEncodingType_e)Offtype,
                                                    count, max, ofCodeTable, nbSeq, OF_defaultNorm, OF_defaultNormLog, DefaultMaxOff,
                                                    prevEntropy->fse.offcodeCTable, sizeof(prevEntropy->fse.offcodeCTable),
                                                    workspace, HUF_WORKSPACE_SIZE);
            if (ZSTD_isError(countSize)) return countSize;
            if (Offtype == set_compressed)
                lastNCount = op;
            op += countSize;
    }   }
    /* build CTable for MatchLengths */
    {   U32 max = MaxML;
        size_t const mostFrequent = HIST_countFast_wksp(count, &max, mlCodeTable, nbSeq, workspace);   /* can't fail */
        DEBUGLOG(5, "Building ML table");
        nextEntropy->fse.matchlength_repeatMode = prevEntropy->fse.matchlength_repeatMode;
        MLtype = ZSTD_selectEncodingType(&nextEntropy->fse.matchlength_repeatMode, count, max, mostFrequent, nbSeq, MLFSELog, prevEntropy->fse.matchlengthCTable, ML_defaultNorm, ML_defaultNormLog, ZSTD_defaultAllowed, strategy);
        assert(!(MLtype < set_compressed && nextEntropy->fse.matchlength_repeatMode != FSE_repeat_none)); /* We don't copy tables */
        {   size_t const countSize = ZSTD_buildCTable(op, oend - op, CTable_MatchLength, MLFSELog, (symbolEncodingType_e)MLtype,
                                                    count, max, mlCodeTable, nbSeq, ML_defaultNorm, ML_defaultNormLog, MaxML,
                                                    prevEntropy->fse.matchlengthCTable, sizeof(prevEntropy->fse.matchlengthCTable),
                                                    workspace, HUF_WORKSPACE_SIZE);
            if (ZSTD_isError(countSize)) return countSize;
            if (MLtype == set_compressed)
                lastNCount = op;
            op += countSize;
    }   }

    *seqHead = (BYTE)((LLtype<<6) + (Offtype<<4) + (MLtype<<2));

    {   size_t const bitstreamSize = ZSTD_encodeSequences(
                                        op, oend - op,
                                        CTable_MatchLength, mlCodeTable,
                                        CTable_OffsetBits, ofCodeTable,
                                        CTable_LitLength, llCodeTable,
                                        sequences, nbSeq,
                                        longOffsets, bmi2);
        if (ZSTD_isError(bitstreamSize)) return bitstreamSize;
        op += bitstreamSize;
        /* zstd versions <= 1.3.4 mistakenly report corruption when
         * FSE_readNCount() recieves a buffer < 4 bytes.
         * Fixed by https://github.com/facebook/zstd/pull/1146.
         * This can happen when the last set_compressed table present is 2
         * bytes and the bitstream is only one byte.
         * In this exceedingly rare case, we will simply emit an uncompressed
         * block, since it isn't worth optimizing.
         */
        if (lastNCount && (op - lastNCount) < 4) {
            /* NCountSize >= 2 && bitstreamSize > 0 ==> lastCountSize == 3 */
            assert(op - lastNCount == 3);
            DEBUGLOG(5, "Avoiding bug in zstd decoder in versions <= 1.3.4 by "
                        "emitting an uncompressed block.");
            return 0;
        }
    }

    return op - ostart;
}

static size_t ZSTD_minGain(size_t srcSize, ZSTD_strategy strat)
{
    U32 const minlog = (strat==ZSTD_btultra) ? 7 : 6;
    return (srcSize >> minlog) + 2;
}

/**
 * ZSTD_window_hasExtDict():
 * Returns non-zero if the window has a non-empty extDict.
 */
MEM_STATIC U32 ZSTD_window_hasExtDict(ZSTD_window_t const window)
{
    return window.lowLimit < window.dictLimit;
}

static size_t ZSTD_compressLiterals (ZSTD_hufCTables_t const* prevHuf,
                                     ZSTD_hufCTables_t* nextHuf,
                                     ZSTD_strategy strategy, int disableLiteralCompression,
                                     void* dst, size_t dstCapacity,
                               const void* src, size_t srcSize,
                                     U32* workspace, const int bmi2)
{
    size_t const minGain = ZSTD_minGain(srcSize, strategy);
    size_t const lhSize = 3 + (srcSize >= 1 KB) + (srcSize >= 16 KB);
    BYTE*  const ostart = (BYTE*)dst;
    U32 singleStream = srcSize < 256;
    symbolEncodingType_e hType = set_compressed;
    size_t cLitSize;

    DEBUGLOG(5,"ZSTD_compressLiterals (disableLiteralCompression=%i)",
                disableLiteralCompression);

    /* Prepare nextEntropy assuming reusing the existing table */
    memcpy(nextHuf, prevHuf, sizeof(*prevHuf));

    if (disableLiteralCompression)
        return ZSTD_noCompressLiterals(dst, dstCapacity, src, srcSize);

    /* small ? don't even attempt compression (speed opt) */
#   define COMPRESS_LITERALS_SIZE_MIN 63
    {   size_t const minLitSize = (prevHuf->repeatMode == HUF_repeat_valid) ? 6 : COMPRESS_LITERALS_SIZE_MIN;
        if (srcSize <= minLitSize) return ZSTD_noCompressLiterals(dst, dstCapacity, src, srcSize);
    }

    if (dstCapacity < lhSize+1) return ERROR(dstSize_tooSmall);   /* not enough space for compression */
    {   HUF_repeat repeat = prevHuf->repeatMode;
        int const preferRepeat = strategy < ZSTD_lazy ? srcSize <= 1024 : 0;
        if (repeat == HUF_repeat_valid && lhSize == 3) singleStream = 1;
        cLitSize = singleStream ? HUF_compress1X_repeat(ostart+lhSize, dstCapacity-lhSize, src, srcSize, 255, 11,
                                      workspace, HUF_WORKSPACE_SIZE, (HUF_CElt*)nextHuf->CTable, &repeat, preferRepeat, bmi2)
                                : HUF_compress4X_repeat(ostart+lhSize, dstCapacity-lhSize, src, srcSize, 255, 11,
                                      workspace, HUF_WORKSPACE_SIZE, (HUF_CElt*)nextHuf->CTable, &repeat, preferRepeat, bmi2);
        if (repeat != HUF_repeat_none) {
            /* reused the existing table */
            hType = set_repeat;
        }
    }

    if ((cLitSize==0) | (cLitSize >= srcSize - minGain) | ERR_isError(cLitSize)) {
        memcpy(nextHuf, prevHuf, sizeof(*prevHuf));
        return ZSTD_noCompressLiterals(dst, dstCapacity, src, srcSize);
    }
    if (cLitSize==1) {
        memcpy(nextHuf, prevHuf, sizeof(*prevHuf));
        return ZSTD_compressRleLiteralsBlock(dst, dstCapacity, src, srcSize);
    }

    if (hType == set_compressed) {
        /* using a newly constructed table */
        nextHuf->repeatMode = HUF_repeat_check;
    }

    /* Build header */
    switch(lhSize)
    {
    case 3: /* 2 - 2 - 10 - 10 */
        {   U32 const lhc = hType + ((!singleStream) << 2) + ((U32)srcSize<<4) + ((U32)cLitSize<<14);
            MEM_writeLE24(ostart, lhc);
            break;
        }
    case 4: /* 2 - 2 - 14 - 14 */
        {   U32 const lhc = hType + (2 << 2) + ((U32)srcSize<<4) + ((U32)cLitSize<<18);
            MEM_writeLE32(ostart, lhc);
            break;
        }
    case 5: /* 2 - 2 - 18 - 18 */
        {   U32 const lhc = hType + (3 << 2) + ((U32)srcSize<<4) + ((U32)cLitSize<<22);
            MEM_writeLE32(ostart, lhc);
            ostart[4] = (BYTE)(cLitSize >> 10);
            break;
        }
    default:  /* not possible : lhSize is {3,4,5} */
        assert(0);
    }
    return lhSize+cLitSize;
}

void ZSTD_seqToCodes(const seqStore_t* seqStorePtr)
{
    const seqDef* const sequences = seqStorePtr->sequencesStart;
    BYTE* const llCodeTable = seqStorePtr->llCode;
    BYTE* const ofCodeTable = seqStorePtr->ofCode;
    BYTE* const mlCodeTable = seqStorePtr->mlCode;
    U32 const nbSeq = (U32)(seqStorePtr->sequences - seqStorePtr->sequencesStart);
    U32 u;
    for (u=0; u<nbSeq; u++) {
        U32 const llv = sequences[u].litLength;
        U32 const mlv = sequences[u].matchLength;
        llCodeTable[u] = (BYTE)ZSTD_LLcode(llv);
        ofCodeTable[u] = (BYTE)ZSTD_highbit32(sequences[u].offset);
        mlCodeTable[u] = (BYTE)ZSTD_MLcode(mlv);
    }
    if (seqStorePtr->longLengthID==1)
        llCodeTable[seqStorePtr->longLengthPos] = MaxLL;
    if (seqStorePtr->longLengthID==2)
        mlCodeTable[seqStorePtr->longLengthPos] = MaxML;
}

/* HIST_countFast_wksp() :
 * Same as HIST_countFast(), but using an externally provided scratch buffer.
 * `workSpace` size must be table of >= HIST_WKSP_SIZE_U32 unsigned */
size_t HIST_countFast_wksp(unsigned* count, unsigned* maxSymbolValuePtr,
                          const void* source, size_t sourceSize,
                          unsigned* workSpace)
{
    if (sourceSize < 1500) /* heuristic threshold */
        return HIST_count_simple(count, maxSymbolValuePtr, source, sourceSize);
    return HIST_count_parallel_wksp(count, maxSymbolValuePtr, source, sourceSize, 0, workSpace);
}

unsigned HIST_count_simple(unsigned* count, unsigned* maxSymbolValuePtr,
                           const void* src, size_t srcSize)
{
    const BYTE* ip = (const BYTE*)src;
    const BYTE* const end = ip + srcSize;
    unsigned maxSymbolValue = *maxSymbolValuePtr;
    unsigned largestCount=0;

    memset(count, 0, (maxSymbolValue+1) * sizeof(*count));
    if (srcSize==0) { *maxSymbolValuePtr = 0; return 0; }

    while (ip<end) {
        assert(*ip <= maxSymbolValue);
        count[*ip++]++;
    }

    while (!count[maxSymbolValue]) maxSymbolValue--;
    *maxSymbolValuePtr = maxSymbolValue;

    {   U32 s;
        for (s=0; s<=maxSymbolValue; s++)
            if (count[s] > largestCount) largestCount = count[s];
    }

    return largestCount;
}

/* HIST_count_parallel_wksp() :
 * store histogram into 4 intermediate tables, recombined at the end.
 * this design makes better use of OoO cpus,
 * and is noticeably faster when some values are heavily repeated.
 * But it needs some additional workspace for intermediate tables.
 * `workSpace` size must be a table of size >= HIST_WKSP_SIZE_U32.
 * @return : largest histogram frequency,
 *           or an error code (notably when histogram would be larger than *maxSymbolValuePtr). */
static size_t HIST_count_parallel_wksp(
                                unsigned* count, unsigned* maxSymbolValuePtr,
                                const void* source, size_t sourceSize,
                                unsigned checkMax,
                                unsigned* const workSpace)
{
    const BYTE* ip = (const BYTE*)source;
    const BYTE* const iend = ip+sourceSize;
    unsigned maxSymbolValue = *maxSymbolValuePtr;
    unsigned max=0;
    U32* const Counting1 = workSpace;
    U32* const Counting2 = Counting1 + 256;
    U32* const Counting3 = Counting2 + 256;
    U32* const Counting4 = Counting3 + 256;

    memset(workSpace, 0, 4*256*sizeof(unsigned));

    /* safety checks */
    if (!sourceSize) {
        memset(count, 0, maxSymbolValue + 1);
        *maxSymbolValuePtr = 0;
        return 0;
    }
    if (!maxSymbolValue) maxSymbolValue = 255;            /* 0 == default */

    /* by stripes of 16 bytes */
    {   U32 cached = MEM_read32(ip); ip += 4;
        while (ip < iend-15) {
            U32 c = cached; cached = MEM_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = MEM_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = MEM_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
            c = cached; cached = MEM_read32(ip); ip += 4;
            Counting1[(BYTE) c     ]++;
            Counting2[(BYTE)(c>>8) ]++;
            Counting3[(BYTE)(c>>16)]++;
            Counting4[       c>>24 ]++;
        }
        ip-=4;
    }

    /* finish last symbols */
    while (ip<iend) Counting1[*ip++]++;

    if (checkMax) {   /* verify stats will fit into destination table */
        U32 s; for (s=255; s>maxSymbolValue; s--) {
            Counting1[s] += Counting2[s] + Counting3[s] + Counting4[s];
            if (Counting1[s]) return ERROR(maxSymbolValue_tooSmall);
    }   }

    {   U32 s;
        if (maxSymbolValue > 255) maxSymbolValue = 255;
        for (s=0; s<=maxSymbolValue; s++) {
            count[s] = Counting1[s] + Counting2[s] + Counting3[s] + Counting4[s];
            if (count[s] > max) max = count[s];
    }   }

    while (!count[maxSymbolValue]) maxSymbolValue--;
    *maxSymbolValuePtr = maxSymbolValue;
    return (size_t)max;
}

MEM_STATIC symbolEncodingType_e
ZSTD_selectEncodingType(
        FSE_repeat* repeatMode, unsigned const* count, unsigned const max,
        size_t const mostFrequent, size_t nbSeq, unsigned const FSELog,
        FSE_CTable const* prevCTable,
        short const* defaultNorm, U32 defaultNormLog,
        ZSTD_defaultPolicy_e const isDefaultAllowed,
        ZSTD_strategy const strategy)
{
    ZSTD_STATIC_ASSERT(ZSTD_defaultDisallowed == 0 && ZSTD_defaultAllowed != 0);
    if (mostFrequent == nbSeq) {
        *repeatMode = FSE_repeat_none;
        if (isDefaultAllowed && nbSeq <= 2) {
            /* Prefer set_basic over set_rle when there are 2 or less symbols,
             * since RLE uses 1 byte, but set_basic uses 5-6 bits per symbol.
             * If basic encoding isn't possible, always choose RLE.
             */
            DEBUGLOG(5, "Selected set_basic");
            return set_basic;
        }
        DEBUGLOG(5, "Selected set_rle");
        return set_rle;
    }
    if (strategy < ZSTD_lazy) {
        if (isDefaultAllowed) {
            size_t const staticFse_nbSeq_max = 1000;
            size_t const mult = 10 - strategy;
            size_t const baseLog = 3;
            size_t const dynamicFse_nbSeq_min = (((size_t)1 << defaultNormLog) * mult) >> baseLog;  /* 28-36 for offset, 56-72 for lengths */
            assert(defaultNormLog >= 5 && defaultNormLog <= 6);  /* xx_DEFAULTNORMLOG */
            assert(mult <= 9 && mult >= 7);
            if ( (*repeatMode == FSE_repeat_valid)
              && (nbSeq < staticFse_nbSeq_max) ) {
                DEBUGLOG(5, "Selected set_repeat");
                return set_repeat;
            }
            if ( (nbSeq < dynamicFse_nbSeq_min)
              || (mostFrequent < (nbSeq >> (defaultNormLog-1))) ) {
                DEBUGLOG(5, "Selected set_basic");
                /* The format allows default tables to be repeated, but it isn't useful.
                 * When using simple heuristics to select encoding type, we don't want
                 * to confuse these tables with dictionaries. When running more careful
                 * analysis, we don't need to waste time checking both repeating tables
                 * and default tables.
                 */
                *repeatMode = FSE_repeat_none;
                return set_basic;
            }
        }
    } else {
        size_t const basicCost = isDefaultAllowed ? ZSTD_crossEntropyCost(defaultNorm, defaultNormLog, count, max) : ERROR(GENERIC);
        size_t const repeatCost = *repeatMode != FSE_repeat_none ? ZSTD_fseBitCost(prevCTable, count, max) : ERROR(GENERIC);
        size_t const NCountCost = ZSTD_NCountCost(count, max, nbSeq, FSELog);
        size_t const compressedCost = (NCountCost << 3) + ZSTD_entropyCost(count, max, nbSeq);

        if (isDefaultAllowed) {
            assert(!ZSTD_isError(basicCost));
            assert(!(*repeatMode == FSE_repeat_valid && ZSTD_isError(repeatCost)));
        }
        assert(!ZSTD_isError(NCountCost));
        assert(compressedCost < ERROR(maxCode));
        DEBUGLOG(5, "Estimated bit costs: basic=%u\trepeat=%u\tcompressed=%u",
                    (U32)basicCost, (U32)repeatCost, (U32)compressedCost);
        if (basicCost <= repeatCost && basicCost <= compressedCost) {
            DEBUGLOG(5, "Selected set_basic");
            assert(isDefaultAllowed);
            *repeatMode = FSE_repeat_none;
            return set_basic;
        }
        if (repeatCost <= compressedCost) {
            DEBUGLOG(5, "Selected set_repeat");
            assert(!ZSTD_isError(repeatCost));
            return set_repeat;
        }
        assert(compressedCost < basicCost && compressedCost < repeatCost);
    }
    DEBUGLOG(5, "Selected set_compressed");
    *repeatMode = FSE_repeat_check;
    return set_compressed;
}

MEM_STATIC size_t
ZSTD_buildCTable(void* dst, size_t dstCapacity,
                FSE_CTable* nextCTable, U32 FSELog, symbolEncodingType_e type,
                U32* count, U32 max,
                const BYTE* codeTable, size_t nbSeq,
                const S16* defaultNorm, U32 defaultNormLog, U32 defaultMax,
                const FSE_CTable* prevCTable, size_t prevCTableSize,
                void* workspace, size_t workspaceSize)
{
    BYTE* op = (BYTE*)dst;
    const BYTE* const oend = op + dstCapacity;

    switch (type) {
    case set_rle:
        *op = codeTable[0];
        CHECK_F(FSE_buildCTable_rle(nextCTable, (BYTE)max));
        return 1;
    case set_repeat:
        memcpy(nextCTable, prevCTable, prevCTableSize);
        return 0;
    case set_basic:
        CHECK_F(FSE_buildCTable_wksp(nextCTable, defaultNorm, defaultMax, defaultNormLog, workspace, workspaceSize));  /* note : could be pre-calculated */
        return 0;
    case set_compressed: {
        S16 norm[MaxSeq + 1];
        size_t nbSeq_1 = nbSeq;
        const U32 tableLog = FSE_optimalTableLog(FSELog, nbSeq, max);
        if (count[codeTable[nbSeq-1]] > 1) {
            count[codeTable[nbSeq-1]]--;
            nbSeq_1--;
        }
        assert(nbSeq_1 > 1);
        CHECK_F(FSE_normalizeCount(norm, tableLog, count, nbSeq_1, max));
        {   size_t const NCountSize = FSE_writeNCount(op, oend - op, norm, max, tableLog);   /* overflow protected */
            if (FSE_isError(NCountSize)) return NCountSize;
            CHECK_F(FSE_buildCTable_wksp(nextCTable, norm, max, tableLog, workspace, workspaceSize));
            return NCountSize;
        }
    }
    default: return assert(0), ERROR(GENERIC);
    }
}

size_t ZSTD_encodeSequences(
            void* dst, size_t dstCapacity,
            FSE_CTable const* CTable_MatchLength, BYTE const* mlCodeTable,
            FSE_CTable const* CTable_OffsetBits, BYTE const* ofCodeTable,
            FSE_CTable const* CTable_LitLength, BYTE const* llCodeTable,
            seqDef const* sequences, size_t nbSeq, int longOffsets, int bmi2)
{
#if DYNAMIC_BMI2
    if (bmi2) {
        return ZSTD_encodeSequences_bmi2(dst, dstCapacity,
                                         CTable_MatchLength, mlCodeTable,
                                         CTable_OffsetBits, ofCodeTable,
                                         CTable_LitLength, llCodeTable,
                                         sequences, nbSeq, longOffsets);
    }
#endif
    (void)bmi2;
    return ZSTD_encodeSequences_default(dst, dstCapacity,
                                        CTable_MatchLength, mlCodeTable,
                                        CTable_OffsetBits, ofCodeTable,
                                        CTable_LitLength, llCodeTable,
                                        sequences, nbSeq, longOffsets);
}

static size_t
ZSTD_encodeSequences_default(
            void* dst, size_t dstCapacity,
            FSE_CTable const* CTable_MatchLength, BYTE const* mlCodeTable,
            FSE_CTable const* CTable_OffsetBits, BYTE const* ofCodeTable,
            FSE_CTable const* CTable_LitLength, BYTE const* llCodeTable,
            seqDef const* sequences, size_t nbSeq, int longOffsets)
{
    return ZSTD_encodeSequences_body(dst, dstCapacity,
                                    CTable_MatchLength, mlCodeTable,
                                    CTable_OffsetBits, ofCodeTable,
                                    CTable_LitLength, llCodeTable,
                                    sequences, nbSeq, longOffsets);
}

FORCE_INLINE_TEMPLATE size_t
ZSTD_encodeSequences_body(
            void* dst, size_t dstCapacity,
            FSE_CTable const* CTable_MatchLength, BYTE const* mlCodeTable,
            FSE_CTable const* CTable_OffsetBits, BYTE const* ofCodeTable,
            FSE_CTable const* CTable_LitLength, BYTE const* llCodeTable,
            seqDef const* sequences, size_t nbSeq, int longOffsets)
{
    BIT_CStream_t blockStream;
    FSE_CState_t  stateMatchLength;
    FSE_CState_t  stateOffsetBits;
    FSE_CState_t  stateLitLength;

    CHECK_E(BIT_initCStream(&blockStream, dst, dstCapacity), dstSize_tooSmall); /* not enough space remaining */

    /* first symbols */
    FSE_initCState2(&stateMatchLength, CTable_MatchLength, mlCodeTable[nbSeq-1]);
    FSE_initCState2(&stateOffsetBits,  CTable_OffsetBits,  ofCodeTable[nbSeq-1]);
    FSE_initCState2(&stateLitLength,   CTable_LitLength,   llCodeTable[nbSeq-1]);
    BIT_addBits(&blockStream, sequences[nbSeq-1].litLength, LL_bits[llCodeTable[nbSeq-1]]);
    if (MEM_32bits()) BIT_flushBits(&blockStream);
    BIT_addBits(&blockStream, sequences[nbSeq-1].matchLength, ML_bits[mlCodeTable[nbSeq-1]]);
    if (MEM_32bits()) BIT_flushBits(&blockStream);
    if (longOffsets) {
        U32 const ofBits = ofCodeTable[nbSeq-1];
        int const extraBits = ofBits - MIN(ofBits, STREAM_ACCUMULATOR_MIN-1);
        if (extraBits) {
            BIT_addBits(&blockStream, sequences[nbSeq-1].offset, extraBits);
            BIT_flushBits(&blockStream);
        }
        BIT_addBits(&blockStream, sequences[nbSeq-1].offset >> extraBits,
                    ofBits - extraBits);
    } else {
        BIT_addBits(&blockStream, sequences[nbSeq-1].offset, ofCodeTable[nbSeq-1]);
    }
    BIT_flushBits(&blockStream);

    {   size_t n;
        for (n=nbSeq-2 ; n<nbSeq ; n--) {      /* intentional underflow */
            BYTE const llCode = llCodeTable[n];
            BYTE const ofCode = ofCodeTable[n];
            BYTE const mlCode = mlCodeTable[n];
            U32  const llBits = LL_bits[llCode];
            U32  const ofBits = ofCode;
            U32  const mlBits = ML_bits[mlCode];
            DEBUGLOG(6, "encoding: litlen:%2u - matchlen:%2u - offCode:%7u",
                        sequences[n].litLength,
                        sequences[n].matchLength + MINMATCH,
                        sequences[n].offset);
                                                                            /* 32b*/  /* 64b*/
                                                                            /* (7)*/  /* (7)*/
            FSE_encodeSymbol(&blockStream, &stateOffsetBits, ofCode);       /* 15 */  /* 15 */
            FSE_encodeSymbol(&blockStream, &stateMatchLength, mlCode);      /* 24 */  /* 24 */
            if (MEM_32bits()) BIT_flushBits(&blockStream);                  /* (7)*/
            FSE_encodeSymbol(&blockStream, &stateLitLength, llCode);        /* 16 */  /* 33 */
            if (MEM_32bits() || (ofBits+mlBits+llBits >= 64-7-(LLFSELog+MLFSELog+OffFSELog)))
                BIT_flushBits(&blockStream);                                /* (7)*/
            BIT_addBits(&blockStream, sequences[n].litLength, llBits);
            if (MEM_32bits() && ((llBits+mlBits)>24)) BIT_flushBits(&blockStream);
            BIT_addBits(&blockStream, sequences[n].matchLength, mlBits);
            if (MEM_32bits() || (ofBits+mlBits+llBits > 56)) BIT_flushBits(&blockStream);
            if (longOffsets) {
                int const extraBits = ofBits - MIN(ofBits, STREAM_ACCUMULATOR_MIN-1);
                if (extraBits) {
                    BIT_addBits(&blockStream, sequences[n].offset, extraBits);
                    BIT_flushBits(&blockStream);                            /* (7)*/
                }
                BIT_addBits(&blockStream, sequences[n].offset >> extraBits,
                            ofBits - extraBits);                            /* 31 */
            } else {
                BIT_addBits(&blockStream, sequences[n].offset, ofBits);     /* 31 */
            }
            BIT_flushBits(&blockStream);                                    /* (7)*/
    }   }

    DEBUGLOG(6, "ZSTD_encodeSequences: flushing ML state with %u bits", stateMatchLength.stateLog);
    FSE_flushCState(&blockStream, &stateMatchLength);
    DEBUGLOG(6, "ZSTD_encodeSequences: flushing Off state with %u bits", stateOffsetBits.stateLog);
    FSE_flushCState(&blockStream, &stateOffsetBits);
    DEBUGLOG(6, "ZSTD_encodeSequences: flushing LL state with %u bits", stateLitLength.stateLog);
    FSE_flushCState(&blockStream, &stateLitLength);

    {   size_t const streamSize = BIT_closeCStream(&blockStream);
        if (streamSize==0) return ERROR(dstSize_tooSmall);   /* not enough space */
        return streamSize;
    }
}

static size_t ZSTD_noCompressLiterals (void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;
    U32   const flSize = 1 + (srcSize>31) + (srcSize>4095);

    if (srcSize + flSize > dstCapacity) return ERROR(dstSize_tooSmall);

    switch(flSize)
    {
        case 1: /* 2 - 1 - 5 */
            ostart[0] = (BYTE)((U32)set_basic + (srcSize<<3));
            break;
        case 2: /* 2 - 2 - 12 */
            MEM_writeLE16(ostart, (U16)((U32)set_basic + (1<<2) + (srcSize<<4)));
            break;
        case 3: /* 2 - 2 - 20 */
            MEM_writeLE32(ostart, (U32)((U32)set_basic + (3<<2) + (srcSize<<4)));
            break;
        default:   /* not necessary : flSize is {1,2,3} */
            assert(0);
    }

    memcpy(ostart + flSize, src, srcSize);
    return srcSize + flSize;
}

size_t HUF_compress1X_repeat (void* dst, size_t dstSize,
                      const void* src, size_t srcSize,
                      unsigned maxSymbolValue, unsigned huffLog,
                      void* workSpace, size_t wkspSize,
                      HUF_CElt* hufTable, HUF_repeat* repeat, int preferRepeat, int bmi2)
{
    return HUF_compress_internal(dst, dstSize, src, srcSize,
                                 maxSymbolValue, huffLog, 1 /*single stream*/,
                                 workSpace, wkspSize, hufTable,
                                 repeat, preferRepeat, bmi2);
}

/* HUF_compress4X_repeat():
 * compress input using 4 streams.
 * re-use an existing huffman compression table */
size_t HUF_compress4X_repeat (void* dst, size_t dstSize,
                      const void* src, size_t srcSize,
                      unsigned maxSymbolValue, unsigned huffLog,
                      void* workSpace, size_t wkspSize,
                      HUF_CElt* hufTable, HUF_repeat* repeat, int preferRepeat, int bmi2)
{
    return HUF_compress_internal(dst, dstSize, src, srcSize,
                                 maxSymbolValue, huffLog, 0 /* 4 streams */,
                                 workSpace, wkspSize,
                                 hufTable, repeat, preferRepeat, bmi2);
}

/* HUF_compress_internal() :
 * `workSpace` must a table of at least HUF_WORKSPACE_SIZE_U32 unsigned */
static size_t HUF_compress_internal (
                void* dst, size_t dstSize,
                const void* src, size_t srcSize,
                unsigned maxSymbolValue, unsigned huffLog,
                unsigned singleStream,
                void* workSpace, size_t wkspSize,
                HUF_CElt* oldHufTable, HUF_repeat* repeat, int preferRepeat,
                const int bmi2)
{
    HUF_compress_tables_t* const table = (HUF_compress_tables_t*)workSpace;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* const oend = ostart + dstSize;
    BYTE* op = ostart;

    /* checks & inits */
    if (((size_t)workSpace & 3) != 0) return ERROR(GENERIC);  /* must be aligned on 4-bytes boundaries */
    if (wkspSize < sizeof(*table)) return ERROR(workSpace_tooSmall);
    if (!srcSize) return 0;  /* Uncompressed */
    if (!dstSize) return 0;  /* cannot fit anything within dst budget */
    if (srcSize > HUF_BLOCKSIZE_MAX) return ERROR(srcSize_wrong);   /* current block size limit */
    if (huffLog > HUF_TABLELOG_MAX) return ERROR(tableLog_tooLarge);
    if (maxSymbolValue > HUF_SYMBOLVALUE_MAX) return ERROR(maxSymbolValue_tooLarge);
    if (!maxSymbolValue) maxSymbolValue = HUF_SYMBOLVALUE_MAX;
    if (!huffLog) huffLog = HUF_TABLELOG_DEFAULT;

    /* Heuristic : If old table is valid, use it for small inputs */
    if (preferRepeat && repeat && *repeat == HUF_repeat_valid) {
        return HUF_compressCTable_internal(ostart, op, oend,
                                           src, srcSize,
                                           singleStream, oldHufTable, bmi2);
    }

    /* Scan input and build symbol stats */
    {   CHECK_V_F(largest, HIST_count_wksp (table->count, &maxSymbolValue, (const BYTE*)src, srcSize, table->count) );
        if (largest == srcSize) { *ostart = ((const BYTE*)src)[0]; return 1; }   /* single symbol, rle */
        if (largest <= (srcSize >> 7)+4) return 0;   /* heuristic : probably not compressible enough */
    }

    /* Check validity of previous table */
    if ( repeat
      && *repeat == HUF_repeat_check
      && !HUF_validateCTable(oldHufTable, table->count, maxSymbolValue)) {
        *repeat = HUF_repeat_none;
    }
    /* Heuristic : use existing table for small inputs */
    if (preferRepeat && repeat && *repeat != HUF_repeat_none) {
        return HUF_compressCTable_internal(ostart, op, oend,
                                           src, srcSize,
                                           singleStream, oldHufTable, bmi2);
    }

    /* Build Huffman Tree */
    huffLog = HUF_optimalTableLog(huffLog, srcSize, maxSymbolValue);
    {   CHECK_V_F(maxBits, HUF_buildCTable_wksp(table->CTable, table->count,
                                                maxSymbolValue, huffLog,
                                                table->nodeTable, sizeof(table->nodeTable)) );
        huffLog = (U32)maxBits;
        /* Zero unused symbols in CTable, so we can check it for validity */
        memset(table->CTable + (maxSymbolValue + 1), 0,
               sizeof(table->CTable) - ((maxSymbolValue + 1) * sizeof(HUF_CElt)));
    }
    /* Write table description header */
    {   CHECK_V_F(hSize, HUF_writeCTable (op, dstSize, table->CTable, maxSymbolValue, huffLog) );
        /* Check if using previous huffman table is beneficial */
        if (repeat && *repeat != HUF_repeat_none) {
            size_t const oldSize = HUF_estimateCompressedSize(oldHufTable, table->count, maxSymbolValue);
            size_t const newSize = HUF_estimateCompressedSize(table->CTable, table->count, maxSymbolValue);
            if (oldSize <= hSize + newSize || hSize + 12 >= srcSize) {
                return HUF_compressCTable_internal(ostart, op, oend,
                                                   src, srcSize,
                                                   singleStream, oldHufTable, bmi2);
        }   }

        /* Use the new huffman table */
        if (hSize + 12ul >= srcSize) { return 0; }
        op += hSize;
        if (repeat) { *repeat = HUF_repeat_none; }
        if (oldHufTable)
            memcpy(oldHufTable, table->CTable, sizeof(table->CTable));  /* Save new table */
    }
    return HUF_compressCTable_internal(ostart, op, oend,
                                       src, srcSize,
                                       singleStream, table->CTable, bmi2);
}

static size_t ZSTD_compressRleLiteralsBlock (void* dst, size_t dstCapacity, const void* src, size_t srcSize)
{
    BYTE* const ostart = (BYTE* const)dst;
    U32   const flSize = 1 + (srcSize>31) + (srcSize>4095);

    (void)dstCapacity;  /* dstCapacity already guaranteed to be >=4, hence large enough */

    switch(flSize)
    {
        case 1: /* 2 - 1 - 5 */
            ostart[0] = (BYTE)((U32)set_rle + (srcSize<<3));
            break;
        case 2: /* 2 - 2 - 12 */
            MEM_writeLE16(ostart, (U16)((U32)set_rle + (1<<2) + (srcSize<<4)));
            break;
        case 3: /* 2 - 2 - 20 */
            MEM_writeLE32(ostart, (U32)((U32)set_rle + (3<<2) + (srcSize<<4)));
            break;
        default:   /* not necessary : flSize is {1,2,3} */
            assert(0);
    }

    ostart[flSize] = *(const BYTE*)src;
    return flSize+1;
}

/**
 * Returns the cost in bits of encoding the distribution in count using the
 * table described by norm. The max symbol support by norm is assumed >= max.
 * norm must be valid for every symbol with non-zero probability in count.
 */
static size_t ZSTD_crossEntropyCost(short const* norm, unsigned accuracyLog,
                                    unsigned const* count, unsigned const max)
{
    unsigned const shift = 8 - accuracyLog;
    size_t cost = 0;
    unsigned s;
    assert(accuracyLog <= 8);
    for (s = 0; s <= max; ++s) {
        unsigned const normAcc = norm[s] != -1 ? norm[s] : 1;
        unsigned const norm256 = normAcc << shift;
        assert(norm256 > 0);
        assert(norm256 < 256);
        cost += count[s] * kInverseProbabiltyLog256[norm256];
    }
    return cost >> 8;
}

/**
 * Returns the cost in bits of encoding the distribution in count using ctable.
 * Returns an error if ctable cannot represent all the symbols in count.
 */
static size_t ZSTD_fseBitCost(
    FSE_CTable const* ctable,
    unsigned const* count,
    unsigned const max)
{
    unsigned const kAccuracyLog = 8;
    size_t cost = 0;
    unsigned s;
    FSE_CState_t cstate;
    FSE_initCState(&cstate, ctable);
    if (ZSTD_getFSEMaxSymbolValue(ctable) < max) {
        DEBUGLOG(5, "Repeat FSE_CTable has maxSymbolValue %u < %u",
                    ZSTD_getFSEMaxSymbolValue(ctable), max);
        return ERROR(GENERIC);
    }
    for (s = 0; s <= max; ++s) {
        unsigned const tableLog = cstate.stateLog;
        unsigned const badCost = (tableLog + 1) << kAccuracyLog;
        unsigned const bitCost = FSE_bitCost(cstate.symbolTT, tableLog, s, kAccuracyLog);
        if (count[s] == 0)
            continue;
        if (bitCost >= badCost) {
            DEBUGLOG(5, "Repeat FSE_CTable has Prob[%u] == 0", s);
            return ERROR(GENERIC);
        }
        cost += count[s] * bitCost;
    }
    return cost >> kAccuracyLog;
}

/**
 * Returns the cost in bytes of encoding the normalized count header.
 * Returns an error if any of the helper functions return an error.
 */
static size_t ZSTD_NCountCost(unsigned const* count, unsigned const max,
                              size_t const nbSeq, unsigned const FSELog)
{
    BYTE wksp[FSE_NCOUNTBOUND];
    S16 norm[MaxSeq + 1];
    const U32 tableLog = FSE_optimalTableLog(FSELog, nbSeq, max);
    CHECK_F(FSE_normalizeCount(norm, tableLog, count, nbSeq, max));
    return FSE_writeNCount(wksp, sizeof(wksp), norm, max, tableLog);
}

/* fake FSE_CTable, for rle input (always same symbol) */
size_t FSE_buildCTable_rle (FSE_CTable* ct, BYTE symbolValue)
{
    void* ptr = ct;
    U16* tableU16 = ( (U16*) ptr) + 2;
    void* FSCTptr = (U32*)ptr + 2;
    FSE_symbolCompressionTransform* symbolTT = (FSE_symbolCompressionTransform*) FSCTptr;

    /* header */
    tableU16[-2] = (U16) 0;
    tableU16[-1] = (U16) symbolValue;

    /* Build table */
    tableU16[0] = 0;
    tableU16[1] = 0;   /* just in case */

    /* Build Symbol Transformation Table */
    symbolTT[symbolValue].deltaNbBits = 0;
    symbolTT[symbolValue].deltaFindState = 0;

    return 0;
}

unsigned FSE_optimalTableLog(unsigned maxTableLog, size_t srcSize, unsigned maxSymbolValue)
{
    return FSE_optimalTableLog_internal(maxTableLog, srcSize, maxSymbolValue, 2);
}

unsigned FSE_optimalTableLog_internal(unsigned maxTableLog, size_t srcSize, unsigned maxSymbolValue, unsigned minus)
{
    U32 maxBitsSrc = BIT_highbit32((U32)(srcSize - 1)) - minus;
    U32 tableLog = maxTableLog;
    U32 minBits = FSE_minTableLog(srcSize, maxSymbolValue);
    assert(srcSize > 1); /* Not supported, RLE should be used instead */
    if (tableLog==0) tableLog = FSE_DEFAULT_TABLELOG;
    if (maxBitsSrc < tableLog) tableLog = maxBitsSrc;   /* Accuracy can be reduced */
    if (minBits > tableLog) tableLog = minBits;   /* Need a minimum to safely represent all symbol values */
    if (tableLog < FSE_MIN_TABLELOG) tableLog = FSE_MIN_TABLELOG;
    if (tableLog > FSE_MAX_TABLELOG) tableLog = FSE_MAX_TABLELOG;
    return tableLog;
}

size_t FSE_normalizeCount (short* normalizedCounter, unsigned tableLog,
                           const unsigned* count, size_t total,
                           unsigned maxSymbolValue)
{
    /* Sanity checks */
    if (tableLog==0) tableLog = FSE_DEFAULT_TABLELOG;
    if (tableLog < FSE_MIN_TABLELOG) return ERROR(GENERIC);   /* Unsupported size */
    if (tableLog > FSE_MAX_TABLELOG) return ERROR(tableLog_tooLarge);   /* Unsupported size */
    if (tableLog < FSE_minTableLog(total, maxSymbolValue)) return ERROR(GENERIC);   /* Too small tableLog, compression potentially impossible */

    {   static U32 const rtbTable[] = {     0, 473195, 504333, 520860, 550000, 700000, 750000, 830000 };
        U64 const scale = 62 - tableLog;
        U64 const step = ((U64)1<<62) / total;   /* <== here, one division ! */
        U64 const vStep = 1ULL<<(scale-20);
        int stillToDistribute = 1<<tableLog;
        unsigned s;
        unsigned largest=0;
        short largestP=0;
        U32 lowThreshold = (U32)(total >> tableLog);

        for (s=0; s<=maxSymbolValue; s++) {
            if (count[s] == total) return 0;   /* rle special case */
            if (count[s] == 0) { normalizedCounter[s]=0; continue; }
            if (count[s] <= lowThreshold) {
                normalizedCounter[s] = -1;
                stillToDistribute--;
            } else {
                short proba = (short)((count[s]*step) >> scale);
                if (proba<8) {
                    U64 restToBeat = vStep * rtbTable[proba];
                    proba += (count[s]*step) - ((U64)proba<<scale) > restToBeat;
                }
                if (proba > largestP) { largestP=proba; largest=s; }
                normalizedCounter[s] = proba;
                stillToDistribute -= proba;
        }   }
        if (-stillToDistribute >= (normalizedCounter[largest] >> 1)) {
            /* corner case, need another normalization method */
            size_t const errorCode = FSE_normalizeM2(normalizedCounter, tableLog, count, total, maxSymbolValue);
            if (FSE_isError(errorCode)) return errorCode;
        }
        else normalizedCounter[largest] += (short)stillToDistribute;
    }

#if 0
    {   /* Print Table (debug) */
        U32 s;
        U32 nTotal = 0;
        for (s=0; s<=maxSymbolValue; s++)
            RAWLOG(2, "%3i: %4i \n", s, normalizedCounter[s]);
        for (s=0; s<=maxSymbolValue; s++)
            nTotal += abs(normalizedCounter[s]);
        if (nTotal != (1U<<tableLog))
            RAWLOG(2, "Warning !!! Total == %u != %u !!!", nTotal, 1U<<tableLog);
        getchar();
    }
#endif

    return tableLog;
}

size_t FSE_writeNCount (void* buffer, size_t bufferSize, const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog)
{
    if (tableLog > FSE_MAX_TABLELOG) return ERROR(tableLog_tooLarge);   /* Unsupported */
    if (tableLog < FSE_MIN_TABLELOG) return ERROR(GENERIC);   /* Unsupported */

    if (bufferSize < FSE_NCountWriteBound(maxSymbolValue, tableLog))
        return FSE_writeNCount_generic(buffer, bufferSize, normalizedCounter, maxSymbolValue, tableLog, 0);

    return FSE_writeNCount_generic(buffer, bufferSize, normalizedCounter, maxSymbolValue, tableLog, 1);
}

/*! BIT_initCStream() :
 *  `dstCapacity` must be > sizeof(size_t)
 *  @return : 0 if success,
 *            otherwise an error code (can be tested using ERR_isError()) */
MEM_STATIC size_t BIT_initCStream(BIT_CStream_t* bitC,
                                  void* startPtr, size_t dstCapacity)
{
    bitC->bitContainer = 0;
    bitC->bitPos = 0;
    bitC->startPtr = (char*)startPtr;
    bitC->ptr = bitC->startPtr;
    bitC->endPtr = bitC->startPtr + dstCapacity - sizeof(bitC->bitContainer);
    if (dstCapacity <= sizeof(bitC->bitContainer)) return ERROR(dstSize_tooSmall);
    return 0;
}

/*! FSE_initCState2() :
*   Same as FSE_initCState(), but the first symbol to include (which will be the last to be read)
*   uses the smallest state value possible, saving the cost of this symbol */
MEM_STATIC void FSE_initCState2(FSE_CState_t* statePtr, const FSE_CTable* ct, U32 symbol)
{
    FSE_initCState(statePtr, ct);
    {   const FSE_symbolCompressionTransform symbolTT = ((const FSE_symbolCompressionTransform*)(statePtr->symbolTT))[symbol];
        const U16* stateTable = (const U16*)(statePtr->stateTable);
        U32 nbBitsOut  = (U32)((symbolTT.deltaNbBits + (1<<15)) >> 16);
        statePtr->value = (nbBitsOut << 16) - symbolTT.deltaNbBits;
        statePtr->value = stateTable[(statePtr->value >> nbBitsOut) + symbolTT.deltaFindState];
    }
}

/*! BIT_addBits() :
 *  can add up to 31 bits into `bitC`.
 *  Note : does not check for register overflow ! */
MEM_STATIC void BIT_addBits(BIT_CStream_t* bitC,
                            size_t value, unsigned nbBits)
{
    MEM_STATIC_ASSERT(BIT_MASK_SIZE == 32);
    assert(nbBits < BIT_MASK_SIZE);
    assert(nbBits + bitC->bitPos < sizeof(bitC->bitContainer) * 8);
    bitC->bitContainer |= (value & BIT_mask[nbBits]) << bitC->bitPos;
    bitC->bitPos += nbBits;
}

/*! BIT_flushBits() :
 *  assumption : bitContainer has not overflowed
 *  safe version; check for buffer overflow, and prevents it.
 *  note : does not signal buffer overflow.
 *  overflow will be revealed later on using BIT_closeCStream() */
MEM_STATIC void BIT_flushBits(BIT_CStream_t* bitC)
{
    size_t const nbBytes = bitC->bitPos >> 3;
    assert(bitC->bitPos < sizeof(bitC->bitContainer) * 8);
    MEM_writeLEST(bitC->ptr, bitC->bitContainer);
    bitC->ptr += nbBytes;
    if (bitC->ptr > bitC->endPtr) bitC->ptr = bitC->endPtr;
    bitC->bitPos &= 7;
    bitC->bitContainer >>= nbBytes*8;
}

MEM_STATIC void FSE_encodeSymbol(BIT_CStream_t* bitC, FSE_CState_t* statePtr, U32 symbol)
{
    FSE_symbolCompressionTransform const symbolTT = ((const FSE_symbolCompressionTransform*)(statePtr->symbolTT))[symbol];
    const U16* const stateTable = (const U16*)(statePtr->stateTable);
    U32 const nbBitsOut  = (U32)((statePtr->value + symbolTT.deltaNbBits) >> 16);
    BIT_addBits(bitC, statePtr->value, nbBitsOut);
    statePtr->value = stateTable[ (statePtr->value >> nbBitsOut) + symbolTT.deltaFindState];
}

/*! BIT_closeCStream() :
 *  @return : size of CStream, in bytes,
 *            or 0 if it could not fit into dstBuffer */
MEM_STATIC size_t BIT_closeCStream(BIT_CStream_t* bitC)
{
    BIT_addBitsFast(bitC, 1, 1);   /* endMark */
    BIT_flushBits(bitC);
    if (bitC->ptr >= bitC->endPtr) return 0; /* overflow detected */
    return (bitC->ptr - bitC->startPtr) + (bitC->bitPos > 0);
}

static size_t HUF_compressCTable_internal(
                BYTE* const ostart, BYTE* op, BYTE* const oend,
                const void* src, size_t srcSize,
                unsigned singleStream, const HUF_CElt* CTable, const int bmi2)
{
    size_t const cSize = singleStream ?
                         HUF_compress1X_usingCTable_internal(op, oend - op, src, srcSize, CTable, bmi2) :
                         HUF_compress4X_usingCTable_internal(op, oend - op, src, srcSize, CTable, bmi2);
    if (HUF_isError(cSize)) { return cSize; }
    if (cSize==0) { return 0; }   /* uncompressible */
    op += cSize;
    /* check compressibility */
    if ((size_t)(op-ostart) >= srcSize-1) { return 0; }
    return op-ostart;
}

/* HIST_count_wksp() :
 * Same as HIST_count(), but using an externally provided scratch buffer.
 * `workSpace` size must be table of >= HIST_WKSP_SIZE_U32 unsigned */
size_t HIST_count_wksp(unsigned* count, unsigned* maxSymbolValuePtr,
                 const void* source, size_t sourceSize, unsigned* workSpace)
{
    if (*maxSymbolValuePtr < 255)
        return HIST_count_parallel_wksp(count, maxSymbolValuePtr, source, sourceSize, 1, workSpace);
    *maxSymbolValuePtr = 255;
    return HIST_countFast_wksp(count, maxSymbolValuePtr, source, sourceSize, workSpace);
}

static int HUF_validateCTable(const HUF_CElt* CTable, const unsigned* count, unsigned maxSymbolValue) {
  int bad = 0;
  int s;
  for (s = 0; s <= (int)maxSymbolValue; ++s) {
    bad |= (count[s] != 0) & (CTable[s].nbBits == 0);
  }
  return !bad;
}

unsigned HUF_optimalTableLog(unsigned maxTableLog, size_t srcSize, unsigned maxSymbolValue)
{
    return FSE_optimalTableLog_internal(maxTableLog, srcSize, maxSymbolValue, 1);
}

/** HUF_buildCTable_wksp() :
 *  Same as HUF_buildCTable(), but using externally allocated scratch buffer.
 *  `workSpace` must be aligned on 4-bytes boundaries, and be at least as large as a table of HUF_CTABLE_WORKSPACE_SIZE_U32 unsigned.
 */
#define STARTNODE (HUF_SYMBOLVALUE_MAX+1)
typedef nodeElt huffNodeTable[HUF_CTABLE_WORKSPACE_SIZE_U32];
size_t HUF_buildCTable_wksp (HUF_CElt* tree, const U32* count, U32 maxSymbolValue, U32 maxNbBits, void* workSpace, size_t wkspSize)
{
    nodeElt* const huffNode0 = (nodeElt*)workSpace;
    nodeElt* const huffNode = huffNode0+1;
    U32 n, nonNullRank;
    int lowS, lowN;
    U16 nodeNb = STARTNODE;
    U32 nodeRoot;

    /* safety checks */
    if (((size_t)workSpace & 3) != 0) return ERROR(GENERIC);  /* must be aligned on 4-bytes boundaries */
    if (wkspSize < sizeof(huffNodeTable)) return ERROR(workSpace_tooSmall);
    if (maxNbBits == 0) maxNbBits = HUF_TABLELOG_DEFAULT;
    if (maxSymbolValue > HUF_SYMBOLVALUE_MAX) return ERROR(maxSymbolValue_tooLarge);
    memset(huffNode0, 0, sizeof(huffNodeTable));

    /* sort, decreasing order */
    HUF_sort(huffNode, count, maxSymbolValue);

    /* init for parents */
    nonNullRank = maxSymbolValue;
    while(huffNode[nonNullRank].count == 0) nonNullRank--;
    lowS = nonNullRank; nodeRoot = nodeNb + lowS - 1; lowN = nodeNb;
    huffNode[nodeNb].count = huffNode[lowS].count + huffNode[lowS-1].count;
    huffNode[lowS].parent = huffNode[lowS-1].parent = nodeNb;
    nodeNb++; lowS-=2;
    for (n=nodeNb; n<=nodeRoot; n++) huffNode[n].count = (U32)(1U<<30);
    huffNode0[0].count = (U32)(1U<<31);  /* fake entry, strong barrier */

    /* create parents */
    while (nodeNb <= nodeRoot) {
        U32 n1 = (huffNode[lowS].count < huffNode[lowN].count) ? lowS-- : lowN++;
        U32 n2 = (huffNode[lowS].count < huffNode[lowN].count) ? lowS-- : lowN++;
        huffNode[nodeNb].count = huffNode[n1].count + huffNode[n2].count;
        huffNode[n1].parent = huffNode[n2].parent = nodeNb;
        nodeNb++;
    }

    /* distribute weights (unlimited tree height) */
    huffNode[nodeRoot].nbBits = 0;
    for (n=nodeRoot-1; n>=STARTNODE; n--)
        huffNode[n].nbBits = huffNode[ huffNode[n].parent ].nbBits + 1;
    for (n=0; n<=nonNullRank; n++)
        huffNode[n].nbBits = huffNode[ huffNode[n].parent ].nbBits + 1;

    /* enforce maxTableLog */
    maxNbBits = HUF_setMaxHeight(huffNode, nonNullRank, maxNbBits);

    /* fill result into tree (val, nbBits) */
    {   U16 nbPerRank[HUF_TABLELOG_MAX+1] = {0};
        U16 valPerRank[HUF_TABLELOG_MAX+1] = {0};
        if (maxNbBits > HUF_TABLELOG_MAX) return ERROR(GENERIC);   /* check fit into table */
        for (n=0; n<=nonNullRank; n++)
            nbPerRank[huffNode[n].nbBits]++;
        /* determine stating value per rank */
        {   U16 min = 0;
            for (n=maxNbBits; n>0; n--) {
                valPerRank[n] = min;      /* get starting value within each rank */
                min += nbPerRank[n];
                min >>= 1;
        }   }
        for (n=0; n<=maxSymbolValue; n++)
            tree[huffNode[n].byte].nbBits = huffNode[n].nbBits;   /* push nbBits per symbol, symbol order */
        for (n=0; n<=maxSymbolValue; n++)
            tree[n].val = valPerRank[tree[n].nbBits]++;   /* assign value within rank, symbol order */
    }

    return maxNbBits;
}

/*! HUF_writeCTable() :
    `CTable` : Huffman tree to save, using huf representation.
    @return : size of saved CTable */
size_t HUF_writeCTable (void* dst, size_t maxDstSize,
                        const HUF_CElt* CTable, U32 maxSymbolValue, U32 huffLog)
{
    BYTE bitsToWeight[HUF_TABLELOG_MAX + 1];   /* precomputed conversion table */
    BYTE huffWeight[HUF_SYMBOLVALUE_MAX];
    BYTE* op = (BYTE*)dst;
    U32 n;

     /* check conditions */
    if (maxSymbolValue > HUF_SYMBOLVALUE_MAX) return ERROR(maxSymbolValue_tooLarge);

    /* convert to weight */
    bitsToWeight[0] = 0;
    for (n=1; n<huffLog+1; n++)
        bitsToWeight[n] = (BYTE)(huffLog + 1 - n);
    for (n=0; n<maxSymbolValue; n++)
        huffWeight[n] = bitsToWeight[CTable[n].nbBits];

    /* attempt weights compression by FSE */
    {   CHECK_V_F(hSize, HUF_compressWeights(op+1, maxDstSize-1, huffWeight, maxSymbolValue) );
        if ((hSize>1) & (hSize < maxSymbolValue/2)) {   /* FSE compressed */
            op[0] = (BYTE)hSize;
            return hSize+1;
    }   }

    /* write raw values as 4-bits (max : 15) */
    if (maxSymbolValue > (256-128)) return ERROR(GENERIC);   /* should not happen : likely means source cannot be compressed */
    if (((maxSymbolValue+1)/2) + 1 > maxDstSize) return ERROR(dstSize_tooSmall);   /* not enough space within dst buffer */
    op[0] = (BYTE)(128 /*special case*/ + (maxSymbolValue-1));
    huffWeight[maxSymbolValue] = 0;   /* to be sure it doesn't cause msan issue in final combination */
    for (n=0; n<maxSymbolValue; n+=2)
        op[(n/2)+1] = (BYTE)((huffWeight[n] << 4) + huffWeight[n+1]);
    return ((maxSymbolValue+1)/2) + 1;
}

static size_t HUF_estimateCompressedSize(HUF_CElt* CTable, const unsigned* count, unsigned maxSymbolValue)
{
    size_t nbBits = 0;
    int s;
    for (s = 0; s <= (int)maxSymbolValue; ++s) {
        nbBits += CTable[s].nbBits * count[s];
    }
    return nbBits >> 3;
}

MEM_STATIC void FSE_initCState(FSE_CState_t* statePtr, const FSE_CTable* ct)
{
    const void* ptr = ct;
    const U16* u16ptr = (const U16*) ptr;
    const U32 tableLog = MEM_read16(ptr);
    statePtr->value = (ptrdiff_t)1<<tableLog;
    statePtr->stateTable = u16ptr+2;
    statePtr->symbolTT = ((const U32*)ct + 1 + (tableLog ? (1<<(tableLog-1)) : 1));
    statePtr->stateLog = tableLog;
}

static unsigned ZSTD_getFSEMaxSymbolValue(FSE_CTable const* ctable) {
  void const* ptr = ctable;
  U16 const* u16ptr = (U16 const*)ptr;
  U32 const maxSymbolValue = MEM_read16(u16ptr + 1);
  return maxSymbolValue;
}

/* FSE_bitCost() :
 * Approximate symbol cost, as fractional value, using fixed-point format (accuracyLog fractional bits)
 * note 1 : assume symbolValue is valid (<= maxSymbolValue)
 * note 2 : if freq[symbolValue]==0, @return a fake cost of tableLog+1 bits */
MEM_STATIC U32 FSE_bitCost(const void* symbolTTPtr, U32 tableLog, U32 symbolValue, U32 accuracyLog)
{
    const FSE_symbolCompressionTransform* symbolTT = (const FSE_symbolCompressionTransform*) symbolTTPtr;
    U32 const minNbBits = symbolTT[symbolValue].deltaNbBits >> 16;
    U32 const threshold = (minNbBits+1) << 16;
    assert(tableLog < 16);
    assert(accuracyLog < 31-tableLog);  /* ensure enough room for renormalization double shift */
    {   U32 const tableSize = 1 << tableLog;
        U32 const deltaFromThreshold = threshold - (symbolTT[symbolValue].deltaNbBits + tableSize);
        U32 const normalizedDeltaFromThreshold = (deltaFromThreshold << accuracyLog) >> tableLog;   /* linear interpolation (very approximate) */
        U32 const bitMultiplier = 1 << accuracyLog;
        assert(symbolTT[symbolValue].deltaNbBits + tableSize <= threshold);
        assert(normalizedDeltaFromThreshold <= bitMultiplier);
        return (minNbBits+1)*bitMultiplier - normalizedDeltaFromThreshold;
    }
}

/* provides the minimum logSize to safely represent a distribution */
static unsigned FSE_minTableLog(size_t srcSize, unsigned maxSymbolValue)
{
    U32 minBitsSrc = BIT_highbit32((U32)(srcSize - 1)) + 1;
    U32 minBitsSymbols = BIT_highbit32(maxSymbolValue) + 2;
    U32 minBits = minBitsSrc < minBitsSymbols ? minBitsSrc : minBitsSymbols;
    assert(srcSize > 1); /* Not supported, RLE should be used instead */
    return minBits;
}

static size_t FSE_normalizeM2(short* norm, U32 tableLog, const unsigned* count, size_t total, U32 maxSymbolValue)
{
    short const NOT_YET_ASSIGNED = -2;
    U32 s;
    U32 distributed = 0;
    U32 ToDistribute;

    /* Init */
    U32 const lowThreshold = (U32)(total >> tableLog);
    U32 lowOne = (U32)((total * 3) >> (tableLog + 1));

    for (s=0; s<=maxSymbolValue; s++) {
        if (count[s] == 0) {
            norm[s]=0;
            continue;
        }
        if (count[s] <= lowThreshold) {
            norm[s] = -1;
            distributed++;
            total -= count[s];
            continue;
        }
        if (count[s] <= lowOne) {
            norm[s] = 1;
            distributed++;
            total -= count[s];
            continue;
        }

        norm[s]=NOT_YET_ASSIGNED;
    }
    ToDistribute = (1 << tableLog) - distributed;

    if ((total / ToDistribute) > lowOne) {
        /* risk of rounding to zero */
        lowOne = (U32)((total * 3) / (ToDistribute * 2));
        for (s=0; s<=maxSymbolValue; s++) {
            if ((norm[s] == NOT_YET_ASSIGNED) && (count[s] <= lowOne)) {
                norm[s] = 1;
                distributed++;
                total -= count[s];
                continue;
        }   }
        ToDistribute = (1 << tableLog) - distributed;
    }

    if (distributed == maxSymbolValue+1) {
        /* all values are pretty poor;
           probably incompressible data (should have already been detected);
           find max, then give all remaining points to max */
        U32 maxV = 0, maxC = 0;
        for (s=0; s<=maxSymbolValue; s++)
            if (count[s] > maxC) { maxV=s; maxC=count[s]; }
        norm[maxV] += (short)ToDistribute;
        return 0;
    }

    if (total == 0) {
        /* all of the symbols were low enough for the lowOne or lowThreshold */
        for (s=0; ToDistribute > 0; s = (s+1)%(maxSymbolValue+1))
            if (norm[s] > 0) { ToDistribute--; norm[s]++; }
        return 0;
    }

    {   U64 const vStepLog = 62 - tableLog;
        U64 const mid = (1ULL << (vStepLog-1)) - 1;
        U64 const rStep = ((((U64)1<<vStepLog) * ToDistribute) + mid) / total;   /* scale on remaining */
        U64 tmpTotal = mid;
        for (s=0; s<=maxSymbolValue; s++) {
            if (norm[s]==NOT_YET_ASSIGNED) {
                U64 const end = tmpTotal + (count[s] * rStep);
                U32 const sStart = (U32)(tmpTotal >> vStepLog);
                U32 const sEnd = (U32)(end >> vStepLog);
                U32 const weight = sEnd - sStart;
                if (weight < 1)
                    return ERROR(GENERIC);
                norm[s] = (short)weight;
                tmpTotal = end;
    }   }   }

    return 0;
}

size_t FSE_NCountWriteBound(unsigned maxSymbolValue, unsigned tableLog)
{
    size_t const maxHeaderSize = (((maxSymbolValue+1) * tableLog) >> 3) + 3;
    return maxSymbolValue ? maxHeaderSize : FSE_NCOUNTBOUND;  /* maxSymbolValue==0 ? use default */
}

static size_t FSE_writeNCount_generic (void* header, size_t headerBufferSize,
                                       const short* normalizedCounter, unsigned maxSymbolValue, unsigned tableLog,
                                       unsigned writeIsSafe)
{
    BYTE* const ostart = (BYTE*) header;
    BYTE* out = ostart;
    BYTE* const oend = ostart + headerBufferSize;
    int nbBits;
    const int tableSize = 1 << tableLog;
    int remaining;
    int threshold;
    U32 bitStream;
    int bitCount;
    unsigned charnum = 0;
    int previous0 = 0;

    bitStream = 0;
    bitCount  = 0;
    /* Table Size */
    bitStream += (tableLog-FSE_MIN_TABLELOG) << bitCount;
    bitCount  += 4;

    /* Init */
    remaining = tableSize+1;   /* +1 for extra accuracy */
    threshold = tableSize;
    nbBits = tableLog+1;

    while (remaining>1) {  /* stops at 1 */
        if (previous0) {
            unsigned start = charnum;
            while (!normalizedCounter[charnum]) charnum++;
            while (charnum >= start+24) {
                start+=24;
                bitStream += 0xFFFFU << bitCount;
                if ((!writeIsSafe) && (out > oend-2)) return ERROR(dstSize_tooSmall);   /* Buffer overflow */
                out[0] = (BYTE) bitStream;
                out[1] = (BYTE)(bitStream>>8);
                out+=2;
                bitStream>>=16;
            }
            while (charnum >= start+3) {
                start+=3;
                bitStream += 3 << bitCount;
                bitCount += 2;
            }
            bitStream += (charnum-start) << bitCount;
            bitCount += 2;
            if (bitCount>16) {
                if ((!writeIsSafe) && (out > oend - 2)) return ERROR(dstSize_tooSmall);   /* Buffer overflow */
                out[0] = (BYTE)bitStream;
                out[1] = (BYTE)(bitStream>>8);
                out += 2;
                bitStream >>= 16;
                bitCount -= 16;
        }   }
        {   int count = normalizedCounter[charnum++];
            int const max = (2*threshold-1)-remaining;
            remaining -= count < 0 ? -count : count;
            count++;   /* +1 for extra accuracy */
            if (count>=threshold) count += max;   /* [0..max[ [max..threshold[ (...) [threshold+max 2*threshold[ */
            bitStream += count << bitCount;
            bitCount  += nbBits;
            bitCount  -= (count<max);
            previous0  = (count==1);
            if (remaining<1) return ERROR(GENERIC);
            while (remaining<threshold) { nbBits--; threshold>>=1; }
        }
        if (bitCount>16) {
            if ((!writeIsSafe) && (out > oend - 2)) return ERROR(dstSize_tooSmall);   /* Buffer overflow */
            out[0] = (BYTE)bitStream;
            out[1] = (BYTE)(bitStream>>8);
            out += 2;
            bitStream >>= 16;
            bitCount -= 16;
    }   }

    /* flush remaining bitStream */
    if ((!writeIsSafe) && (out > oend - 2)) return ERROR(dstSize_tooSmall);   /* Buffer overflow */
    out[0] = (BYTE)bitStream;
    out[1] = (BYTE)(bitStream>>8);
    out+= (bitCount+7) /8;

    if (charnum > maxSymbolValue + 1) return ERROR(GENERIC);

    return (out-ostart);
}

MEM_STATIC void MEM_writeLEST(void* memPtr, size_t val)
{
    if (MEM_32bits())
        MEM_writeLE32(memPtr, (U32)val);
    else
        MEM_writeLE64(memPtr, (U64)val);
}

/*! BIT_addBitsFast() :
 *  works only if `value` is _clean_,
 *  meaning all high bits above nbBits are 0 */
MEM_STATIC void BIT_addBitsFast(BIT_CStream_t* bitC,
                                size_t value, unsigned nbBits)
{
    assert((value>>nbBits) == 0);
    assert(nbBits + bitC->bitPos < sizeof(bitC->bitContainer) * 8);
    bitC->bitContainer |= value << bitC->bitPos;
    bitC->bitPos += nbBits;
}

FORCE_INLINE_TEMPLATE size_t
HUF_compress1X_usingCTable_internal_body(void* dst, size_t dstSize,
                                   const void* src, size_t srcSize,
                                   const HUF_CElt* CTable)
{
    const BYTE* ip = (const BYTE*) src;
    BYTE* const ostart = (BYTE*)dst;
    BYTE* const oend = ostart + dstSize;
    BYTE* op = ostart;
    size_t n;
    BIT_CStream_t bitC;

    /* init */
    if (dstSize < 8) return 0;   /* not enough space to compress */
    { size_t const initErr = BIT_initCStream(&bitC, op, oend-op);
      if (HUF_isError(initErr)) return 0; }

    n = srcSize & ~3;  /* join to mod 4 */
    switch (srcSize & 3)
    {
        case 3 : HUF_encodeSymbol(&bitC, ip[n+ 2], CTable);
                 HUF_FLUSHBITS_2(&bitC);
                 /* fall-through */
        case 2 : HUF_encodeSymbol(&bitC, ip[n+ 1], CTable);
                 HUF_FLUSHBITS_1(&bitC);
                 /* fall-through */
        case 1 : HUF_encodeSymbol(&bitC, ip[n+ 0], CTable);
                 HUF_FLUSHBITS(&bitC);
                 /* fall-through */
        case 0 : /* fall-through */
        default: break;
    }

    for (; n>0; n-=4) {  /* note : n&3==0 at this stage */
        HUF_encodeSymbol(&bitC, ip[n- 1], CTable);
        HUF_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 2], CTable);
        HUF_FLUSHBITS_2(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 3], CTable);
        HUF_FLUSHBITS_1(&bitC);
        HUF_encodeSymbol(&bitC, ip[n- 4], CTable);
        HUF_FLUSHBITS(&bitC);
    }

    return BIT_closeCStream(&bitC);
}

static size_t
HUF_compress4X_usingCTable_internal(void* dst, size_t dstSize,
                              const void* src, size_t srcSize,
                              const HUF_CElt* CTable, int bmi2)
{
    size_t const segmentSize = (srcSize+3)/4;   /* first 3 segments */
    const BYTE* ip = (const BYTE*) src;
    const BYTE* const iend = ip + srcSize;
    BYTE* const ostart = (BYTE*) dst;
    BYTE* const oend = ostart + dstSize;
    BYTE* op = ostart;

    if (dstSize < 6 + 1 + 1 + 1 + 8) return 0;   /* minimum space to compress successfully */
    if (srcSize < 12) return 0;   /* no saving possible : too small input */
    op += 6;   /* jumpTable */

    {   CHECK_V_F(cSize, HUF_compress1X_usingCTable_internal(op, oend-op, ip, segmentSize, CTable, bmi2) );
        if (cSize==0) return 0;
        assert(cSize <= 65535);
        MEM_writeLE16(ostart, (U16)cSize);
        op += cSize;
    }

    ip += segmentSize;
    {   CHECK_V_F(cSize, HUF_compress1X_usingCTable_internal(op, oend-op, ip, segmentSize, CTable, bmi2) );
        if (cSize==0) return 0;
        assert(cSize <= 65535);
        MEM_writeLE16(ostart+2, (U16)cSize);
        op += cSize;
    }

    ip += segmentSize;
    {   CHECK_V_F(cSize, HUF_compress1X_usingCTable_internal(op, oend-op, ip, segmentSize, CTable, bmi2) );
        if (cSize==0) return 0;
        assert(cSize <= 65535);
        MEM_writeLE16(ostart+4, (U16)cSize);
        op += cSize;
    }

    ip += segmentSize;
    {   CHECK_V_F(cSize, HUF_compress1X_usingCTable_internal(op, oend-op, ip, iend-ip, CTable, bmi2) );
        if (cSize==0) return 0;
        op += cSize;
    }

    return op-ostart;
}

static void HUF_sort(nodeElt* huffNode, const U32* count, U32 maxSymbolValue)
{
    rankPos rank[32];
    U32 n;

    memset(rank, 0, sizeof(rank));
    for (n=0; n<=maxSymbolValue; n++) {
        U32 r = BIT_highbit32(count[n] + 1);
        rank[r].base ++;
    }
    for (n=30; n>0; n--) rank[n-1].base += rank[n].base;
    for (n=0; n<32; n++) rank[n].current = rank[n].base;
    for (n=0; n<=maxSymbolValue; n++) {
        U32 const c = count[n];
        U32 const r = BIT_highbit32(c+1) + 1;
        U32 pos = rank[r].current++;
        while ((pos > rank[r].base) && (c > huffNode[pos-1].count)) {
            huffNode[pos] = huffNode[pos-1];
            pos--;
        }
        huffNode[pos].count = c;
        huffNode[pos].byte  = (BYTE)n;
    }
}

static U32 HUF_setMaxHeight(nodeElt* huffNode, U32 lastNonNull, U32 maxNbBits)
{
    const U32 largestBits = huffNode[lastNonNull].nbBits;
    if (largestBits <= maxNbBits) return largestBits;   /* early exit : no elt > maxNbBits */

    /* there are several too large elements (at least >= 2) */
    {   int totalCost = 0;
        const U32 baseCost = 1 << (largestBits - maxNbBits);
        U32 n = lastNonNull;

        while (huffNode[n].nbBits > maxNbBits) {
            totalCost += baseCost - (1 << (largestBits - huffNode[n].nbBits));
            huffNode[n].nbBits = (BYTE)maxNbBits;
            n --;
        }  /* n stops at huffNode[n].nbBits <= maxNbBits */
        while (huffNode[n].nbBits == maxNbBits) n--;   /* n end at index of smallest symbol using < maxNbBits */

        /* renorm totalCost */
        totalCost >>= (largestBits - maxNbBits);  /* note : totalCost is necessarily a multiple of baseCost */

        /* repay normalized cost */
        {   U32 const noSymbol = 0xF0F0F0F0;
            U32 rankLast[HUF_TABLELOG_MAX+2];
            int pos;

            /* Get pos of last (smallest) symbol per rank */
            memset(rankLast, 0xF0, sizeof(rankLast));
            {   U32 currentNbBits = maxNbBits;
                for (pos=n ; pos >= 0; pos--) {
                    if (huffNode[pos].nbBits >= currentNbBits) continue;
                    currentNbBits = huffNode[pos].nbBits;   /* < maxNbBits */
                    rankLast[maxNbBits-currentNbBits] = pos;
            }   }

            while (totalCost > 0) {
                U32 nBitsToDecrease = BIT_highbit32(totalCost) + 1;
                for ( ; nBitsToDecrease > 1; nBitsToDecrease--) {
                    U32 highPos = rankLast[nBitsToDecrease];
                    U32 lowPos = rankLast[nBitsToDecrease-1];
                    if (highPos == noSymbol) continue;
                    if (lowPos == noSymbol) break;
                    {   U32 const highTotal = huffNode[highPos].count;
                        U32 const lowTotal = 2 * huffNode[lowPos].count;
                        if (highTotal <= lowTotal) break;
                }   }
                /* only triggered when no more rank 1 symbol left => find closest one (note : there is necessarily at least one !) */
                /* HUF_MAX_TABLELOG test just to please gcc 5+; but it should not be necessary */
                while ((nBitsToDecrease<=HUF_TABLELOG_MAX) && (rankLast[nBitsToDecrease] == noSymbol))
                    nBitsToDecrease ++;
                totalCost -= 1 << (nBitsToDecrease-1);
                if (rankLast[nBitsToDecrease-1] == noSymbol)
                    rankLast[nBitsToDecrease-1] = rankLast[nBitsToDecrease];   /* this rank is no longer empty */
                huffNode[rankLast[nBitsToDecrease]].nbBits ++;
                if (rankLast[nBitsToDecrease] == 0)    /* special case, reached largest symbol */
                    rankLast[nBitsToDecrease] = noSymbol;
                else {
                    rankLast[nBitsToDecrease]--;
                    if (huffNode[rankLast[nBitsToDecrease]].nbBits != maxNbBits-nBitsToDecrease)
                        rankLast[nBitsToDecrease] = noSymbol;   /* this rank is now empty */
            }   }   /* while (totalCost > 0) */

            while (totalCost < 0) {  /* Sometimes, cost correction overshoot */
                if (rankLast[1] == noSymbol) {  /* special case : no rank 1 symbol (using maxNbBits-1); let's create one from largest rank 0 (using maxNbBits) */
                    while (huffNode[n].nbBits == maxNbBits) n--;
                    huffNode[n+1].nbBits--;
                    rankLast[1] = n+1;
                    totalCost++;
                    continue;
                }
                huffNode[ rankLast[1] + 1 ].nbBits--;
                rankLast[1]++;
                totalCost ++;
    }   }   }   /* there are several too large elements (at least >= 2) */

    return maxNbBits;
}


/* HUF_compressWeights() :
 * Same as FSE_compress(), but dedicated to huff0's weights compression.
 * The use case needs much less stack memory.
 * Note : all elements within weightTable are supposed to be <= HUF_TABLELOG_MAX.
 */
#define MAX_FSE_TABLELOG_FOR_HUFF_HEADER 6
size_t HUF_compressWeights (void* dst, size_t dstSize, const void* weightTable, size_t wtSize)
{
    BYTE* const ostart = (BYTE*) dst;
    BYTE* op = ostart;
    BYTE* const oend = ostart + dstSize;

    U32 maxSymbolValue = HUF_TABLELOG_MAX;
    U32 tableLog = MAX_FSE_TABLELOG_FOR_HUFF_HEADER;

    FSE_CTable CTable[FSE_CTABLE_SIZE_U32(MAX_FSE_TABLELOG_FOR_HUFF_HEADER, HUF_TABLELOG_MAX)];
    BYTE scratchBuffer[1<<MAX_FSE_TABLELOG_FOR_HUFF_HEADER];

    U32 count[HUF_TABLELOG_MAX+1];
    S16 norm[HUF_TABLELOG_MAX+1];

    /* init conditions */
    if (wtSize <= 1) return 0;  /* Not compressible */

    /* Scan input and build symbol stats */
    {   unsigned const maxCount = HIST_count_simple(count, &maxSymbolValue, weightTable, wtSize);   /* never fails */
        if (maxCount == wtSize) return 1;   /* only a single symbol in src : rle */
        if (maxCount == 1) return 0;        /* each symbol present maximum once => not compressible */
    }

    tableLog = FSE_optimalTableLog(tableLog, wtSize, maxSymbolValue);
    CHECK_F( FSE_normalizeCount(norm, tableLog, count, wtSize, maxSymbolValue) );

    /* Write table description header */
    {   CHECK_V_F(hSize, FSE_writeNCount(op, oend-op, norm, maxSymbolValue, tableLog) );
        op += hSize;
    }

    /* Compress */
    CHECK_F( FSE_buildCTable_wksp(CTable, norm, maxSymbolValue, tableLog, scratchBuffer, sizeof(scratchBuffer)) );
    {   CHECK_V_F(cSize, FSE_compress_usingCTable(op, oend - op, weightTable, wtSize, CTable) );
        if (cSize == 0) return 0;   /* not enough space for compressed data */
        op += cSize;
    }

    return op-ostart;
}

FORCE_INLINE_TEMPLATE void
HUF_encodeSymbol(BIT_CStream_t* bitCPtr, U32 symbol, const HUF_CElt* CTable)
{
    BIT_addBitsFast(bitCPtr, CTable[symbol].val, CTable[symbol].nbBits);
}

static size_t FSE_compress_usingCTable_generic (void* dst, size_t dstSize,
                           const void* src, size_t srcSize,
                           const FSE_CTable* ct, const unsigned fast)
{
    const BYTE* const istart = (const BYTE*) src;
    const BYTE* const iend = istart + srcSize;
    const BYTE* ip=iend;

    BIT_CStream_t bitC;
    FSE_CState_t CState1, CState2;

    /* init */
    if (srcSize <= 2) return 0;
    { size_t const initError = BIT_initCStream(&bitC, dst, dstSize);
      if (FSE_isError(initError)) return 0; /* not enough space available to write a bitstream */ }

#define FSE_FLUSHBITS(s)  (fast ? BIT_flushBitsFast(s) : BIT_flushBits(s))

    if (srcSize & 1) {
        FSE_initCState2(&CState1, ct, *--ip);
        FSE_initCState2(&CState2, ct, *--ip);
        FSE_encodeSymbol(&bitC, &CState1, *--ip);
        FSE_FLUSHBITS(&bitC);
    } else {
        FSE_initCState2(&CState2, ct, *--ip);
        FSE_initCState2(&CState1, ct, *--ip);
    }

    /* join to mod 4 */
    srcSize -= 2;
    if ((sizeof(bitC.bitContainer)*8 > FSE_MAX_TABLELOG*4+7 ) && (srcSize & 2)) {  /* test bit 2 */
        FSE_encodeSymbol(&bitC, &CState2, *--ip);
        FSE_encodeSymbol(&bitC, &CState1, *--ip);
        FSE_FLUSHBITS(&bitC);
    }

    /* 2 or 4 encoding per loop */
    while ( ip>istart ) {

        FSE_encodeSymbol(&bitC, &CState2, *--ip);

        if (sizeof(bitC.bitContainer)*8 < FSE_MAX_TABLELOG*2+7 )   /* this test must be static */
            FSE_FLUSHBITS(&bitC);

        FSE_encodeSymbol(&bitC, &CState1, *--ip);

        if (sizeof(bitC.bitContainer)*8 > FSE_MAX_TABLELOG*4+7 ) {  /* this test must be static */
            FSE_encodeSymbol(&bitC, &CState2, *--ip);
            FSE_encodeSymbol(&bitC, &CState1, *--ip);
        }

        FSE_FLUSHBITS(&bitC);
    }

    FSE_flushCState(&bitC, &CState2);
    FSE_flushCState(&bitC, &CState1);
    return BIT_closeCStream(&bitC);
}

MEM_STATIC void FSE_flushCState(BIT_CStream_t* bitC, const FSE_CState_t* statePtr)
{
    BIT_addBits(bitC, statePtr->value, statePtr->stateLog);
    BIT_flushBits(bitC);
}

/**
 * ZSTD_matchState_dictMode():
 * Inspects the provided matchState and figures out what dictMode should be
 * passed to the compressor.
 */
MEM_STATIC ZSTD_dictMode_e ZSTD_matchState_dictMode(const ZSTD_matchState_t *ms)
{
    return ZSTD_window_hasExtDict(ms->window) ?
        ZSTD_extDict :
        ms->dictMatchState != NULL ?
            ZSTD_dictMatchState :
            ZSTD_noDict;
}

size_t ZSTD_compressBlock_doubleFast( ZSTD_matchState_t* ms, seqStore_t* seqStore, U32 rep[ZSTD_REP_NUM], ZSTD_compressionParameters const* cParams, void const* src, size_t srcSize)
{
    const U32 mls = cParams->searchLength;
    switch(mls)
    {
    default: /* includes case 3 */
    case 4 :
        return ZSTD_compressBlock_doubleFast_generic(ms, seqStore, rep, cParams, src, srcSize, 4, ZSTD_noDict);
    case 5 :
        return ZSTD_compressBlock_doubleFast_generic(ms, seqStore, rep, cParams, src, srcSize, 5, ZSTD_noDict);
    case 6 :
        return ZSTD_compressBlock_doubleFast_generic(ms, seqStore, rep, cParams, src, srcSize, 6, ZSTD_noDict);
    case 7 :
        return ZSTD_compressBlock_doubleFast_generic(ms, seqStore, rep, cParams, src, srcSize, 7, ZSTD_noDict);
    }
}

FORCE_INLINE_TEMPLATE
size_t ZSTD_compressBlock_doubleFast_generic( ZSTD_matchState_t* ms, seqStore_t* seqStore, U32 rep[ZSTD_REP_NUM], ZSTD_compressionParameters const* cParams, void const* src, size_t srcSize, U32 const mls /* template */, ZSTD_dictMode_e const dictMode)
{
    U32* const hashLong = ms->hashTable;
    const U32 hBitsL = cParams->hashLog;
    U32* const hashSmall = ms->chainTable;
    const U32 hBitsS = cParams->chainLog;
    const BYTE* const base = ms->window.base;
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* anchor = istart;
    const U32 prefixLowestIndex = ms->window.dictLimit;
    const BYTE* const prefixLowest = base + prefixLowestIndex;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - HASH_READ_SIZE;
    U32 offset_1=rep[0], offset_2=rep[1];
    U32 offsetSaved = 0;

    const U32 dictAndPrefixLength  = (U32)(ip - prefixLowest + NULL - NULL);

    /* init */
    ip += (dictAndPrefixLength == 0);
    U32 const maxRep = (U32)(ip - prefixLowest);
    if (offset_2 > maxRep) offsetSaved = offset_2, offset_2 = 0;
    if (offset_1 > maxRep) offsetSaved = offset_1, offset_1 = 0;

    /* Main Search Loop */
    while (ip < ilimit) {   /* < instead of <=, because repcode check at (ip+1) */
        size_t mLength;
        U32 offset;
        size_t const h2 = ZSTD_hashPtr(ip, hBitsL, 8);
        size_t const h = ZSTD_hashPtr(ip, hBitsS, mls);
        U32 const current = (U32)(ip-base);
        U32 const matchIndexL = hashLong[h2];
        U32 matchIndexS = hashSmall[h];
        const BYTE* matchLong = base + matchIndexL;
        const BYTE* match = base + matchIndexS;
        const U32 repIndex = current + 1 - offset_1;
        const BYTE* repMatch = base + repIndex;
        hashLong[h2] = hashSmall[h] = current;   /* update hash tables */

        /* check noDict repcode */
        if (((offset_1 > 0) & (MEM_read32(ip+1-offset_1) == MEM_read32(ip+1)))) {
            mLength = ZSTD_count(ip+1+4, ip+1+4-offset_1, iend) + 4;
            ip++;
            ZSTD_storeSeq(seqStore, ip-anchor, anchor, 0, mLength-MINMATCH);
            goto _match_stored;
        }

        /* check prefix long match */
        if ( (matchIndexL > prefixLowestIndex) && (MEM_read64(matchLong) == MEM_read64(ip)) ) {
            mLength = ZSTD_count(ip+8, matchLong+8, iend) + 8;
            offset = (U32)(ip-matchLong);
            while (((ip>anchor) & (matchLong>prefixLowest)) && (ip[-1] == matchLong[-1])) { ip--; matchLong--; mLength++; } /* catch up */
            goto _match_found;
        }

        /* check prefix short match */
        if ( (matchIndexS > prefixLowestIndex) && (MEM_read32(match) == MEM_read32(ip)) ) {
            goto _search_next_long;
        }

        ip += ((ip-anchor) >> kSearchStrength) + 1;
        continue;

_search_next_long:

        {
            size_t const hl3 = ZSTD_hashPtr(ip+1, hBitsL, 8);
            U32 const matchIndexL3 = hashLong[hl3];
            const BYTE* matchL3 = base + matchIndexL3;
            hashLong[hl3] = current + 1;

            /* check prefix long +1 match */
            if ( (matchIndexL3 > prefixLowestIndex) && (MEM_read64(matchL3) == MEM_read64(ip+1)) ) {
                mLength = ZSTD_count(ip+9, matchL3+8, iend) + 8;
                ip++;
                offset = (U32)(ip-matchL3);
                while (((ip>anchor) & (matchL3>prefixLowest)) && (ip[-1] == matchL3[-1])) { ip--; matchL3--; mLength++; } /* catch up */
                goto _match_found;
            }
        }

        /* if no long +1 match, explore the short match we found */
        mLength = ZSTD_count(ip+4, match+4, iend) + 4;
        offset = (U32)(ip - match);
        while (((ip>anchor) & (match>prefixLowest)) && (ip[-1] == match[-1])) { ip--; match--; mLength++; } /* catch up */

        /* fall-through */


_match_found:
        offset_2 = offset_1;
        offset_1 = offset;

        ZSTD_storeSeq(seqStore, ip-anchor, anchor, offset + ZSTD_REP_MOVE, mLength-MINMATCH);

_match_stored:
        /* match found */
        ip += mLength;
        anchor = ip;

        if (ip <= ilimit) {
            /* Fill Table */
            hashLong[ZSTD_hashPtr(base+current+2, hBitsL, 8)] = hashSmall[ZSTD_hashPtr(base+current+2, hBitsS, mls)] = current+2;  /* here because current+2 could be > iend-8 */
            hashLong[ZSTD_hashPtr(ip-2, hBitsL, 8)] = hashSmall[ZSTD_hashPtr(ip-2, hBitsS, mls)] = (U32)(ip-2-base);

            /* check immediate repcode */

            while ( (ip <= ilimit) && ( (offset_2>0) & (MEM_read32(ip) == MEM_read32(ip - offset_2)) )) {
                /* store sequence */
                size_t const rLength = ZSTD_count(ip+4, ip+4-offset_2, iend) + 4;
                U32 const tmpOff = offset_2; offset_2 = offset_1; offset_1 = tmpOff;  /* swap offset_2 <=> offset_1 */
                hashSmall[ZSTD_hashPtr(ip, hBitsS, mls)] = (U32)(ip-base);
                hashLong[ZSTD_hashPtr(ip, hBitsL, 8)] = (U32)(ip-base);
                ZSTD_storeSeq(seqStore, 0, anchor, 0, rLength-MINMATCH);
                ip += rLength;
                anchor = ip;
                continue;   /* faster when present ... (?) */
            }
        }
    }

    /* save reps for next block */
    rep[0] = offset_1 ? offset_1 : offsetSaved;
    rep[1] = offset_2 ? offset_2 : offsetSaved;

    /* Return the last literals size */
    return iend - anchor;
}


MEM_STATIC U32 ZSTD_LLcode(U32 litLength)
{
    static const BYTE LL_Code[64] = {  0,  1,  2,  3,  4,  5,  6,  7,
                                       8,  9, 10, 11, 12, 13, 14, 15,
                                      16, 16, 17, 17, 18, 18, 19, 19,
                                      20, 20, 20, 20, 21, 21, 21, 21,
                                      22, 22, 22, 22, 22, 22, 22, 22,
                                      23, 23, 23, 23, 23, 23, 23, 23,
                                      24, 24, 24, 24, 24, 24, 24, 24,
                                      24, 24, 24, 24, 24, 24, 24, 24 };
    static const U32 LL_deltaCode = 19;
    return (litLength > 63) ? ZSTD_highbit32(litLength) + LL_deltaCode : LL_Code[litLength];
}

/* ZSTD_MLcode() :
 * note : mlBase = matchLength - MINMATCH;
 *        because it's the format it's stored in seqStore->sequences */
MEM_STATIC U32 ZSTD_MLcode(U32 mlBase)
{
    static const BYTE ML_Code[128] = { 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
                                      16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                                      32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
                                      38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39,
                                      40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
                                      41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
                                      42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                                      42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42 };
    static const U32 ML_deltaCode = 36;
    return (mlBase > 127) ? ZSTD_highbit32(mlBase) + ML_deltaCode : ML_Code[mlBase];
}

MEM_STATIC void FSE_initCState(FSE_CState_t* statePtr, const FSE_CTable* ct)
{
    const void* ptr = ct;
    const U16* u16ptr = (const U16*) ptr;
    const U32 tableLog = MEM_read16(ptr);
    statePtr->value = (ptrdiff_t)1<<tableLog;
    statePtr->stateTable = u16ptr+2;
    statePtr->symbolTT = ((const U32*)ct + 1 + (tableLog ? (1<<(tableLog-1)) : 1));
    statePtr->stateLog = tableLog;
}

/*! FSE_initCState2() :
*   Same as FSE_initCState(), but the first symbol to include (which will be the last to be read)
*   uses the smallest state value possible, saving the cost of this symbol */
MEM_STATIC void FSE_initCState2(FSE_CState_t* statePtr, const FSE_CTable* ct, U32 symbol)
{
    FSE_initCState(statePtr, ct);
    {   const FSE_symbolCompressionTransform symbolTT = ((const FSE_symbolCompressionTransform*)(statePtr->symbolTT))[symbol];
        const U16* stateTable = (const U16*)(statePtr->stateTable);
        U32 nbBitsOut  = (U32)((symbolTT.deltaNbBits + (1<<15)) >> 16);
        statePtr->value = (nbBitsOut << 16) - symbolTT.deltaNbBits;
        statePtr->value = stateTable[(statePtr->value >> nbBitsOut) + symbolTT.deltaFindState];
    }
}

/*! BIT_addBits() :
 *  can add up to 31 bits into `bitC`.
 *  Note : does not check for register overflow ! */
MEM_STATIC void BIT_addBits(BIT_CStream_t* bitC,
                            size_t value, unsigned nbBits)
{
    MEM_STATIC_ASSERT(BIT_MASK_SIZE == 32);
    assert(nbBits < BIT_MASK_SIZE);
    assert(nbBits + bitC->bitPos < sizeof(bitC->bitContainer) * 8);
    bitC->bitContainer |= (value & BIT_mask[nbBits]) << bitC->bitPos;
    bitC->bitPos += nbBits;
}

/*! BIT_addBitsFast() :
 *  works only if `value` is _clean_,
 *  meaning all high bits above nbBits are 0 */
MEM_STATIC void BIT_addBitsFast(BIT_CStream_t* bitC,
                                size_t value, unsigned nbBits)
{
    assert((value>>nbBits) == 0);
    assert(nbBits + bitC->bitPos < sizeof(bitC->bitContainer) * 8);
    bitC->bitContainer |= value << bitC->bitPos;
    bitC->bitPos += nbBits;
}

/*! BIT_flushBitsFast() :
 *  assumption : bitContainer has not overflowed
 *  unsafe version; does not check buffer overflow */
MEM_STATIC void BIT_flushBitsFast(BIT_CStream_t* bitC)
{
    size_t const nbBytes = bitC->bitPos >> 3;
    assert(bitC->bitPos < sizeof(bitC->bitContainer) * 8);
    MEM_writeLEST(bitC->ptr, bitC->bitContainer);
    bitC->ptr += nbBytes;
    assert(bitC->ptr <= bitC->endPtr);
    bitC->bitPos &= 7;
    bitC->bitContainer >>= nbBytes*8;
}

/*! BIT_flushBits() :
 *  assumption : bitContainer has not overflowed
 *  safe version; check for buffer overflow, and prevents it.
 *  note : does not signal buffer overflow.
 *  overflow will be revealed later on using BIT_closeCStream() */
MEM_STATIC void BIT_flushBits(BIT_CStream_t* bitC)
{
    size_t const nbBytes = bitC->bitPos >> 3;
    assert(bitC->bitPos < sizeof(bitC->bitContainer) * 8);
    MEM_writeLEST(bitC->ptr, bitC->bitContainer);
    bitC->ptr += nbBytes;
    if (bitC->ptr > bitC->endPtr) bitC->ptr = bitC->endPtr;
    bitC->bitPos &= 7;
    bitC->bitContainer >>= nbBytes*8;
}

MEM_STATIC void FSE_encodeSymbol(BIT_CStream_t* bitC, FSE_CState_t* statePtr, U32 symbol)
{
    FSE_symbolCompressionTransform const symbolTT = ((const FSE_symbolCompressionTransform*)(statePtr->symbolTT))[symbol];
    const U16* const stateTable = (const U16*)(statePtr->stateTable);
    U32 const nbBitsOut  = (U32)((statePtr->value + symbolTT.deltaNbBits) >> 16);
    BIT_addBits(bitC, statePtr->value, nbBitsOut);
    statePtr->value = stateTable[ (statePtr->value >> nbBitsOut) + symbolTT.deltaFindState];
}

MEM_STATIC void FSE_flushCState(BIT_CStream_t* bitC, const FSE_CState_t* statePtr)
{
    BIT_addBits(bitC, statePtr->value, statePtr->stateLog);
    BIT_flushBits(bitC);
}

size_t FSE_compress_usingCTable (void* dst, size_t dstSize,
                           const void* src, size_t srcSize,
                           const FSE_CTable* ct)
{
    unsigned const fast = (dstSize >= FSE_BLOCKBOUND(srcSize));

    if (fast)
        return FSE_compress_usingCTable_generic(dst, dstSize, src, srcSize, ct, 1);
    else
        return FSE_compress_usingCTable_generic(dst, dstSize, src, srcSize, ct, 0);
}
