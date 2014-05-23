package org.apache.lucene.codecs.idversion;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.NoOutputs;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.PackedInts;

/*
  TODO:
  
    - Currently there is a one-to-one mapping of indexed
      term to term block, but we could decouple the two, ie,
      put more terms into the index than there are blocks.
      The index would take up more RAM but then it'd be able
      to avoid seeking more often and could make PK/FuzzyQ
      faster if the additional indexed terms could store
      the offset into the terms block.

    - The blocks are not written in true depth-first
      order, meaning if you just next() the file pointer will
      sometimes jump backwards.  For example, block foo* will
      be written before block f* because it finished before.
      This could possibly hurt performance if the terms dict is
      not hot, since OSs anticipate sequential file access.  We
      could fix the writer to re-order the blocks as a 2nd
      pass.

    - Each block encodes the term suffixes packed
      sequentially using a separate vInt per term, which is
      1) wasteful and 2) slow (must linear scan to find a
      particular suffix).  We should instead 1) make
      random-access array so we can directly access the Nth
      suffix, and 2) bulk-encode this array using bulk int[]
      codecs; then at search time we can binary search when
      we seek a particular term.
*/

/**
 * This is just like {@link BlockTreeTermsWriter}, except it also stores a version per term, and adds a method to its TermsEnum
 * implementation to seekExact only if the version is >= the specified version.  The version is added to the terms index to avoid seeking if
 * no term in the block has a high enough version.  The term blocks file is .tiv and the terms index extension is .tipv.
 *
 * @lucene.experimental
 */

final class VersionBlockTreeTermsWriter extends FieldsConsumer {

  // private static boolean DEBUG = IDVersionSegmentTermsEnum.DEBUG;

  static final PairOutputs<BytesRef,Long> FST_OUTPUTS = new PairOutputs<>(ByteSequenceOutputs.getSingleton(),
                                                                          PositiveIntOutputs.getSingleton());

  static final Pair<BytesRef,Long> NO_OUTPUT = FST_OUTPUTS.getNoOutput();

  /** Suggested default value for the {@code
   *  minItemsInBlock} parameter to {@link
   *  #VersionBlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}. */
  public final static int DEFAULT_MIN_BLOCK_SIZE = 25;

  /** Suggested default value for the {@code
   *  maxItemsInBlock} parameter to {@link
   *  #VersionBlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}. */
  public final static int DEFAULT_MAX_BLOCK_SIZE = 48;

  //public final static boolean DEBUG = false;
  //private final static boolean SAVE_DOT_FILES = false;

  static final int OUTPUT_FLAGS_NUM_BITS = 2;
  static final int OUTPUT_FLAGS_MASK = 0x3;
  static final int OUTPUT_FLAG_IS_FLOOR = 0x1;
  static final int OUTPUT_FLAG_HAS_TERMS = 0x2;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tiv";
  final static String TERMS_CODEC_NAME = "VERSION_BLOCK_TREE_TERMS_DICT";

  /** Initial terms format. */
  public static final int VERSION_START = 0;

  /** Current terms format. */
  public static final int VERSION_CURRENT = VERSION_START;

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tipv";
  final static String TERMS_INDEX_CODEC_NAME = "VERSION_BLOCK_TREE_TERMS_INDEX";

  private final IndexOutput out;
  private final IndexOutput indexOut;
  final int maxDoc;
  final int minItemsInBlock;
  final int maxItemsInBlock;

  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  FieldInfo currentField;

  private static class FieldMetaData {
    public final FieldInfo fieldInfo;
    public final Pair<BytesRef,Long> rootCode;
    public final long numTerms;
    public final long indexStartFP;
    private final int longsSize;
    public final BytesRef minTerm;
    public final BytesRef maxTerm;

    public FieldMetaData(FieldInfo fieldInfo, Pair<BytesRef,Long> rootCode, long numTerms, long indexStartFP, int longsSize,
                         BytesRef minTerm, BytesRef maxTerm) {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      assert rootCode != null: "field=" + fieldInfo.name + " numTerms=" + numTerms;
      this.rootCode = rootCode;
      this.indexStartFP = indexStartFP;
      this.numTerms = numTerms;
      this.longsSize = longsSize;
      this.minTerm = minTerm;
      this.maxTerm = maxTerm;
    }
  }

  private final List<FieldMetaData> fields = new ArrayList<>();
  // private final String segment;

  /** Create a new writer.  The number of items (terms or
   *  sub-blocks) per block will aim to be between
   *  minItemsPerBlock and maxItemsPerBlock, though in some
   *  cases the blocks may be smaller than the min. */
  public VersionBlockTreeTermsWriter(
                                     SegmentWriteState state,
                                     PostingsWriterBase postingsWriter,
                                     int minItemsInBlock,
                                     int maxItemsInBlock)
    throws IOException
  {
    if (minItemsInBlock <= 1) {
      throw new IllegalArgumentException("minItemsInBlock must be >= 2; got " + minItemsInBlock);
    }
    if (maxItemsInBlock <= 0) {
      throw new IllegalArgumentException("maxItemsInBlock must be >= 1; got " + maxItemsInBlock);
    }
    if (minItemsInBlock > maxItemsInBlock) {
      throw new IllegalArgumentException("maxItemsInBlock must be >= minItemsInBlock; got maxItemsInBlock=" + maxItemsInBlock + " minItemsInBlock=" + minItemsInBlock);
    }
    if (2*(minItemsInBlock-1) > maxItemsInBlock) {
      throw new IllegalArgumentException("maxItemsInBlock must be at least 2*(minItemsInBlock-1); got maxItemsInBlock=" + maxItemsInBlock + " minItemsInBlock=" + minItemsInBlock);
    }

    maxDoc = state.segmentInfo.getDocCount();

    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_EXTENSION);
    out = state.directory.createOutput(termsFileName, state.context);
    boolean success = false;
    IndexOutput indexOut = null;
    try {
      fieldInfos = state.fieldInfos;
      this.minItemsInBlock = minItemsInBlock;
      this.maxItemsInBlock = maxItemsInBlock;
      CodecUtil.writeHeader(out, TERMS_CODEC_NAME, VERSION_CURRENT);   

      //DEBUG = state.segmentName.equals("_4a");

      final String termsIndexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_INDEX_EXTENSION);
      indexOut = state.directory.createOutput(termsIndexFileName, state.context);
      CodecUtil.writeHeader(indexOut, TERMS_INDEX_CODEC_NAME, VERSION_CURRENT); 

      currentField = null;
      this.postingsWriter = postingsWriter;
      // segment = state.segmentInfo.name;

      // System.out.println("BTW.init seg=" + state.segmentName);

      postingsWriter.init(out);                          // have consumer write its format/header
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out, indexOut);
      }
    }
    this.indexOut = indexOut;
  }

  /** Writes the terms file trailer. */
  private void writeTrailer(IndexOutput out, long dirStart) throws IOException {
    out.writeLong(dirStart);    
  }

  /** Writes the index file trailer. */
  private void writeIndexTrailer(IndexOutput indexOut, long dirStart) throws IOException {
    indexOut.writeLong(dirStart);    
  }

  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    //DEBUG = field.name.equals("id");
    //if (DEBUG) System.out.println("\nBTTW.addField seg=" + segment + " field=" + field.name);
    assert currentField == null || currentField.name.compareTo(field.name) < 0;
    currentField = field;
    return new TermsWriter(field);
  }

  static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
    assert fp < (1L << 62);
    return (fp << 2) | (hasTerms ? OUTPUT_FLAG_HAS_TERMS : 0) | (isFloor ? OUTPUT_FLAG_IS_FLOOR : 0);
  }

  private static class PendingEntry {
    public final boolean isTerm;

    protected PendingEntry(boolean isTerm) {
      this.isTerm = isTerm;
    }
  }

  private static final class PendingTerm extends PendingEntry {
    public final BytesRef term;
    // stats + metadata
    public final BlockTermState state;

    public PendingTerm(BytesRef term, BlockTermState state) {
      super(true);
      this.term = term;
      this.state = state;
    }

    @Override
    public String toString() {
      return term.utf8ToString();
    }
  }

  private static final class PendingBlock extends PendingEntry {
    public final BytesRef prefix;
    public final long fp;
    public FST<Pair<BytesRef,Long>> index;
    public List<FST<Pair<BytesRef,Long>>> subIndices;
    public final boolean hasTerms;
    public final boolean isFloor;
    public final int floorLeadByte;
    private final IntsRef scratchIntsRef = new IntsRef();
    /** Max version for all terms in this block. */
    private final long maxVersion;

    public PendingBlock(BytesRef prefix, long maxVersion, long fp, boolean hasTerms, boolean isFloor, int floorLeadByte, List<FST<Pair<BytesRef,Long>>> subIndices) {
      super(false);
      this.prefix = prefix;
      this.maxVersion = maxVersion;
      this.fp = fp;
      this.hasTerms = hasTerms;
      this.isFloor = isFloor;
      this.floorLeadByte = floorLeadByte;
      this.subIndices = subIndices;
    }

    @Override
    public String toString() {
      return "BLOCK: " + prefix.utf8ToString();
    }

    public void compileIndex(List<PendingBlock> floorBlocks, RAMOutputStream scratchBytes) throws IOException {

      assert (isFloor && floorBlocks != null && floorBlocks.size() != 0) || (!isFloor && floorBlocks == null): "isFloor=" + isFloor + " floorBlocks=" + floorBlocks;

      assert scratchBytes.getFilePointer() == 0;

      long maxVersionIndex = maxVersion;

      // TODO: try writing the leading vLong in MSB order
      // (opposite of what Lucene does today), for better
      // outputs sharing in the FST
      scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));
      if (isFloor) {
        scratchBytes.writeVInt(floorBlocks.size());
        for (PendingBlock sub : floorBlocks) {
          assert sub.floorLeadByte != -1;
          maxVersionIndex = Math.max(maxVersionIndex, sub.maxVersion);
          //if (DEBUG) {
          //  System.out.println("    write floorLeadByte=" + Integer.toHexString(sub.floorLeadByte&0xff));
          //}
          scratchBytes.writeByte((byte) sub.floorLeadByte);
          assert sub.fp > fp;
          scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
        }
      }

      final Builder<Pair<BytesRef,Long>> indexBuilder = new Builder<>(FST.INPUT_TYPE.BYTE1,
                                                                      0, 0, true, false, Integer.MAX_VALUE,
                                                                      FST_OUTPUTS, null, false,
                                                                      PackedInts.COMPACT, true, 15);
      //if (DEBUG) {
      //  System.out.println("  compile index for prefix=" + prefix);
      //}
      //indexBuilder.DEBUG = false;
      final byte[] bytes = new byte[(int) scratchBytes.getFilePointer()];
      assert bytes.length > 0;
      scratchBytes.writeTo(bytes, 0);
      indexBuilder.add(Util.toIntsRef(prefix, scratchIntsRef), FST_OUTPUTS.newPair(new BytesRef(bytes, 0, bytes.length), Long.MAX_VALUE - maxVersionIndex));
      scratchBytes.reset();

      // Copy over index for all sub-blocks

      if (subIndices != null) {
        for(FST<Pair<BytesRef,Long>> subIndex : subIndices) {
          append(indexBuilder, subIndex);
        }
      }

      if (floorBlocks != null) {
        for (PendingBlock sub : floorBlocks) {
          if (sub.subIndices != null) {
            for(FST<Pair<BytesRef,Long>> subIndex : sub.subIndices) {
              append(indexBuilder, subIndex);
            }
          }
          sub.subIndices = null;
        }
      }

      index = indexBuilder.finish();
      subIndices = null;

      /*
      Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
      Util.toDot(index, w, false, false);
      System.out.println("SAVED to out.dot");
      w.close();
      */
    }

    // TODO: maybe we could add bulk-add method to
    // Builder?  Takes FST and unions it w/ current
    // FST.
    private void append(Builder<Pair<BytesRef,Long>> builder, FST<Pair<BytesRef,Long>> subIndex) throws IOException {
      final BytesRefFSTEnum<Pair<BytesRef,Long>> subIndexEnum = new BytesRefFSTEnum<>(subIndex);
      BytesRefFSTEnum.InputOutput<Pair<BytesRef,Long>> indexEnt;
      while((indexEnt = subIndexEnum.next()) != null) {
        //if (DEBUG) {
        //  System.out.println("      add sub=" + indexEnt.input + " " + indexEnt.input + " output=" + indexEnt.output);
        //}
        builder.add(Util.toIntsRef(indexEnt.input, scratchIntsRef), indexEnt.output);
      }
    }
  }

  final RAMOutputStream scratchBytes = new RAMOutputStream();

  class TermsWriter extends TermsConsumer {
    private final FieldInfo fieldInfo;
    private final int longsSize;
    private long numTerms;
    final FixedBitSet docsSeen;
    long indexStartFP;

    // Used only to partition terms into the block tree; we
    // don't pull an FST from this builder:
    private final NoOutputs noOutputs;
    private final Builder<Object> blockBuilder;

    // PendingTerm or PendingBlock:
    private final List<PendingEntry> pending = new ArrayList<>();

    // Index into pending of most recently written block
    private int lastBlockIndex = -1;

    // Re-used when segmenting a too-large block into floor
    // blocks:
    private int[] subBytes = new int[10];
    private int[] subTermCounts = new int[10];
    private int[] subTermCountSums = new int[10];
    private int[] subSubCounts = new int[10];

    private BytesRef minTerm;
    private BytesRef maxTerm = new BytesRef();

    // This class assigns terms to blocks "naturally", ie,
    // according to the number of terms under a given prefix
    // that we encounter:
    private class FindBlocks extends Builder.FreezeTail<Object> {

      @Override
      public void freeze(final Builder.UnCompiledNode<Object>[] frontier, int prefixLenPlus1, final IntsRef lastInput) throws IOException {

        //if (DEBUG) System.out.println("  freeze prefixLenPlus1=" + prefixLenPlus1);

        for(int idx=lastInput.length; idx >= prefixLenPlus1; idx--) {
          final Builder.UnCompiledNode<Object> node = frontier[idx];

          long totCount = 0;

          if (node.isFinal) {
            totCount++;
          }

          for(int arcIdx=0;arcIdx<node.numArcs;arcIdx++) {
            @SuppressWarnings("unchecked") final Builder.UnCompiledNode<Object> target = (Builder.UnCompiledNode<Object>) node.arcs[arcIdx].target;
            totCount += target.inputCount;
            target.clear();
            node.arcs[arcIdx].target = null;
          }
          node.numArcs = 0;

          if (totCount >= minItemsInBlock || idx == 0) {
            // We are on a prefix node that has enough
            // entries (terms or sub-blocks) under it to let
            // us write a new block or multiple blocks (main
            // block + follow on floor blocks):
            //if (DEBUG) {
            //  if (totCount < minItemsInBlock && idx != 0) {
            //    System.out.println("  force block has terms");
            //  }
            //}
            writeBlocks(lastInput, idx, (int) totCount);
            node.inputCount = 1;
          } else {
            // stragglers!  carry count upwards
            node.inputCount = totCount;
          }
          frontier[idx] = new Builder.UnCompiledNode<>(blockBuilder, idx);
        }
      }
    }

    // Write the top count entries on the pending stack as
    // one or more blocks.  Returns how many blocks were
    // written.  If the entry count is <= maxItemsPerBlock
    // we just write a single block; else we break into
    // primary (initial) block and then one or more
    // following floor blocks:

    void writeBlocks(IntsRef prevTerm, int prefixLength, int count) throws IOException {
      if (count <= maxItemsInBlock) {
        // Easy case: not floor block.  Eg, prefix is "foo",
        // and we found 30 terms/sub-blocks starting w/ that
        // prefix, and minItemsInBlock <= 30 <=
        // maxItemsInBlock.
        final PendingBlock nonFloorBlock = writeBlock(prevTerm, prefixLength, prefixLength, count, count, 0, false, -1, true);
        nonFloorBlock.compileIndex(null, scratchBytes);
        pending.add(nonFloorBlock);
      } else {
        // Floor block case.  Eg, prefix is "foo" but we
        // have 100 terms/sub-blocks starting w/ that
        // prefix.  We segment the entries into a primary
        // block and following floor blocks using the first
        // label in the suffix to assign to floor blocks.

        // TODO: we could store min & max suffix start byte
        // in each block, to make floor blocks authoritative

        /*
        if (DEBUG) {
          final BytesRef prefix = new BytesRef(prefixLength);
          for(int m=0;m<prefixLength;m++) {
            prefix.bytes[m] = (byte) prevTerm.ints[m];
          }
          prefix.length = prefixLength;
          //System.out.println("\nWBS count=" + count + " prefix=" + prefix.utf8ToString() + " " + prefix);
          System.out.println("writeBlocks: prefix=" + toString(prefix) + " " + prefix + " count=" + count + " pending.size()=" + pending.size());
        }
        */
        //System.out.println("\nwbs count=" + count);

        final int savLabel = prevTerm.ints[prevTerm.offset + prefixLength];

        // Count up how many items fall under
        // each unique label after the prefix.
        
        // TODO: this is wasteful since the builder had
        // already done this (partitioned these sub-terms
        // according to their leading prefix byte)
        
        final List<PendingEntry> slice = pending.subList(pending.size()-count, pending.size());
        int lastSuffixLeadLabel = -1;
        int termCount = 0;
        int subCount = 0;
        int numSubs = 0;

        for(PendingEntry ent : slice) {

          // First byte in the suffix of this term
          final int suffixLeadLabel;
          if (ent.isTerm) {
            PendingTerm term = (PendingTerm) ent;
            if (term.term.length == prefixLength) {
              // Suffix is 0, ie prefix 'foo' and term is
              // 'foo' so the term has empty string suffix
              // in this block
              assert lastSuffixLeadLabel == -1;
              assert numSubs == 0;
              suffixLeadLabel = -1;
            } else {
              suffixLeadLabel = term.term.bytes[term.term.offset + prefixLength] & 0xff;
            }
          } else {
            PendingBlock block = (PendingBlock) ent;
            assert block.prefix.length > prefixLength;
            suffixLeadLabel = block.prefix.bytes[block.prefix.offset + prefixLength] & 0xff;
          }

          if (suffixLeadLabel != lastSuffixLeadLabel && (termCount + subCount) != 0) {
            if (subBytes.length == numSubs) {
              subBytes = ArrayUtil.grow(subBytes);
              subTermCounts = ArrayUtil.grow(subTermCounts);
              subSubCounts = ArrayUtil.grow(subSubCounts);
            }
            subBytes[numSubs] = lastSuffixLeadLabel;
            lastSuffixLeadLabel = suffixLeadLabel;
            subTermCounts[numSubs] = termCount;
            subSubCounts[numSubs] = subCount;
            /*
            if (suffixLeadLabel == -1) {
              System.out.println("  sub " + -1 + " termCount=" + termCount + " subCount=" + subCount);
            } else {
              System.out.println("  sub " + Integer.toHexString(suffixLeadLabel) + " termCount=" + termCount + " subCount=" + subCount);
            }
            */
            termCount = subCount = 0;
            numSubs++;
          }

          if (ent.isTerm) {
            termCount++;
          } else {
            subCount++;
          }
        }

        if (subBytes.length == numSubs) {
          subBytes = ArrayUtil.grow(subBytes);
          subTermCounts = ArrayUtil.grow(subTermCounts);
          subSubCounts = ArrayUtil.grow(subSubCounts);
        }

        subBytes[numSubs] = lastSuffixLeadLabel;
        subTermCounts[numSubs] = termCount;
        subSubCounts[numSubs] = subCount;
        numSubs++;
        /*
        if (lastSuffixLeadLabel == -1) {
          System.out.println("  sub " + -1 + " termCount=" + termCount + " subCount=" + subCount);
        } else {
          System.out.println("  sub " + Integer.toHexString(lastSuffixLeadLabel) + " termCount=" + termCount + " subCount=" + subCount);
        }
        */

        if (subTermCountSums.length < numSubs) {
          subTermCountSums = ArrayUtil.grow(subTermCountSums, numSubs);
        }

        // Roll up (backwards) the termCounts; postings impl
        // needs this to know where to pull the term slice
        // from its pending terms stack:
        int sum = 0;
        for(int idx=numSubs-1;idx>=0;idx--) {
          sum += subTermCounts[idx];
          subTermCountSums[idx] = sum;
        }

        // TODO: make a better segmenter?  It'd have to
        // absorb the too-small end blocks backwards into
        // the previous blocks

        // Naive greedy segmentation; this is not always
        // best (it can produce a too-small block as the
        // last block):
        int pendingCount = 0;
        int startLabel = subBytes[0];
        int curStart = count;
        subCount = 0;

        final List<PendingBlock> floorBlocks = new ArrayList<>();
        PendingBlock firstBlock = null;

        for(int sub=0;sub<numSubs;sub++) {
          pendingCount += subTermCounts[sub] + subSubCounts[sub];
          //System.out.println("  " + (subTermCounts[sub] + subSubCounts[sub]));
          subCount++;

          // Greedily make a floor block as soon as we've
          // crossed the min count
          if (pendingCount >= minItemsInBlock) {
            final int curPrefixLength;
            if (startLabel == -1) {
              curPrefixLength = prefixLength;
            } else {
              curPrefixLength = 1+prefixLength;
              // floor term:
              prevTerm.ints[prevTerm.offset + prefixLength] = startLabel;
            }
            //System.out.println("  " + subCount + " subs");
            final PendingBlock floorBlock = writeBlock(prevTerm, prefixLength, curPrefixLength, curStart, pendingCount, subTermCountSums[1+sub], true, startLabel, curStart == pendingCount);
            if (firstBlock == null) {
              firstBlock = floorBlock;
            } else {
              floorBlocks.add(floorBlock);
            }
            curStart -= pendingCount;
            //System.out.println("    = " + pendingCount);
            pendingCount = 0;

            assert minItemsInBlock == 1 || subCount > 1: "minItemsInBlock=" + minItemsInBlock + " subCount=" + subCount + " sub=" + sub + " of " + numSubs + " subTermCount=" + subTermCountSums[sub] + " subSubCount=" + subSubCounts[sub] + " depth=" + prefixLength;
            subCount = 0;
            startLabel = subBytes[sub+1];

            if (curStart == 0) {
              break;
            }

            if (curStart <= maxItemsInBlock) {
              // remainder is small enough to fit into a
              // block.  NOTE that this may be too small (<
              // minItemsInBlock); need a true segmenter
              // here
              assert startLabel != -1;
              assert firstBlock != null;
              prevTerm.ints[prevTerm.offset + prefixLength] = startLabel;
              //System.out.println("  final " + (numSubs-sub-1) + " subs");
              /*
              for(sub++;sub < numSubs;sub++) {
                System.out.println("  " + (subTermCounts[sub] + subSubCounts[sub]));
              }
              System.out.println("    = " + curStart);
              if (curStart < minItemsInBlock) {
                System.out.println("      **");
              }
              */
              floorBlocks.add(writeBlock(prevTerm, prefixLength, prefixLength+1, curStart, curStart, 0, true, startLabel, true));
              break;
            }
          }
        }

        prevTerm.ints[prevTerm.offset + prefixLength] = savLabel;

        assert firstBlock != null;
        firstBlock.compileIndex(floorBlocks, scratchBytes);

        pending.add(firstBlock);
        //if (DEBUG) System.out.println("  done pending.size()=" + pending.size());
      }
      lastBlockIndex = pending.size()-1;
    }

    // for debugging
    @SuppressWarnings("unused")
    private String toString(BytesRef b) {
      try {
        return b.utf8ToString() + " " + b;
      } catch (Throwable t) {
        // If BytesRef isn't actually UTF8, or it's eg a
        // prefix of UTF8 that ends mid-unicode-char, we
        // fallback to hex:
        return b.toString();
      }
    }

    // Writes all entries in the pending slice as a single
    // block: 
    private PendingBlock writeBlock(IntsRef prevTerm, int prefixLength, int indexPrefixLength, int startBackwards, int length,
                                    int futureTermCount, boolean isFloor, int floorLeadByte, boolean isLastInFloor) throws IOException {

      assert length > 0;

      final int start = pending.size()-startBackwards;

      assert start >= 0: "pending.size()=" + pending.size() + " startBackwards=" + startBackwards + " length=" + length;

      final List<PendingEntry> slice = pending.subList(start, start + length);

      final long startFP = out.getFilePointer();

      final BytesRef prefix = new BytesRef(indexPrefixLength);
      for(int m=0;m<indexPrefixLength;m++) {
        prefix.bytes[m] = (byte) prevTerm.ints[m];
      }
      prefix.length = indexPrefixLength;

      // Write block header:
      out.writeVInt((length<<1)|(isLastInFloor ? 1:0));

      // if (DEBUG) {
      //  System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + " pending.size()=" + pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" + toString(prefix) + " entCount=" + length + " startFP=" + startFP + " futureTermCount=" + futureTermCount + (isFloor ? (" floorLeadByte=" + Integer.toHexString(floorLeadByte&0xff)) : "") + " isLastInFloor=" + isLastInFloor);
      // }

      // 1st pass: pack term suffix bytes into byte[] blob
      // TODO: cutover to bulk int codec... simple64?

      final boolean isLeafBlock;
      if (lastBlockIndex < start) {
        // This block definitely does not contain sub-blocks:
        isLeafBlock = true;
        //System.out.println("no scan true isFloor=" + isFloor);
      } else if (!isFloor) {
        // This block definitely does contain at least one sub-block:
        isLeafBlock = false;
        //System.out.println("no scan false " + lastBlockIndex + " vs start=" + start + " len=" + length);
      } else {
        // Must scan up-front to see if there is a sub-block
        boolean v = true;
        //System.out.println("scan " + lastBlockIndex + " vs start=" + start + " len=" + length);
        for (PendingEntry ent : slice) {
          if (!ent.isTerm) {
            v = false;
            break;
          }
        }
        isLeafBlock = v;
      }

      final List<FST<Pair<BytesRef,Long>>> subIndices;

      int termCount;

      long[] longs = new long[longsSize];
      boolean absolute = true;
      long maxVersionInBlock = -1;

      // int countx = 0;
      if (isLeafBlock) {
        subIndices = null;
        for (PendingEntry ent : slice) {
          assert ent.isTerm;
          PendingTerm term = (PendingTerm) ent;
          BlockTermState state = term.state;
          maxVersionInBlock = Math.max(maxVersionInBlock, ((IDVersionTermState) state).idVersion);
          final int suffix = term.term.length - prefixLength;
          // if (DEBUG) {
          //    BytesRef suffixBytes = new BytesRef(suffix);
          //    System.arraycopy(term.term.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
          //    suffixBytes.length = suffix;
          //    System.out.println("    " + (countx++) + ": write term suffix=" + toString(suffixBytes));
          // }
          // For leaf block we write suffix straight
          suffixWriter.writeVInt(suffix);
          suffixWriter.writeBytes(term.term.bytes, prefixLength, suffix);

          // Write term meta data
          postingsWriter.encodeTerm(longs, bytesWriter, fieldInfo, state, absolute);
          for (int pos = 0; pos < longsSize; pos++) {
            assert longs[pos] >= 0;
            metaWriter.writeVLong(longs[pos]);
          }
          bytesWriter.writeTo(metaWriter);
          bytesWriter.reset();
          absolute = false;
        }
        termCount = length;
      } else {
        subIndices = new ArrayList<>();
        termCount = 0;
        for (PendingEntry ent : slice) {
          if (ent.isTerm) {
            PendingTerm term = (PendingTerm) ent;
            BlockTermState state = term.state;
            maxVersionInBlock = Math.max(maxVersionInBlock, ((IDVersionTermState) state).idVersion);
            final int suffix = term.term.length - prefixLength;
            // if (DEBUG) {
            //    BytesRef suffixBytes = new BytesRef(suffix);
            //    System.arraycopy(term.term.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //    suffixBytes.length = suffix;
            //    System.out.println("    " + (countx++) + ": write term suffix=" + toString(suffixBytes));
            // }
            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block
            suffixWriter.writeVInt(suffix<<1);
            suffixWriter.writeBytes(term.term.bytes, prefixLength, suffix);

            // TODO: now that terms dict "sees" these longs,
            // we can explore better column-stride encodings
            // to encode all long[0]s for this block at
            // once, all long[1]s, etc., e.g. using
            // Simple64.  Alternatively, we could interleave
            // stats + meta ... no reason to have them
            // separate anymore:

            // Write term meta data
            postingsWriter.encodeTerm(longs, bytesWriter, fieldInfo, state, absolute);
            for (int pos = 0; pos < longsSize; pos++) {
              assert longs[pos] >= 0;
              metaWriter.writeVLong(longs[pos]);
            }
            bytesWriter.writeTo(metaWriter);
            bytesWriter.reset();
            absolute = false;

            termCount++;
          } else {
            PendingBlock block = (PendingBlock) ent;
            maxVersionInBlock = Math.max(maxVersionInBlock, block.maxVersion);
            final int suffix = block.prefix.length - prefixLength;

            assert suffix > 0;

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block
            suffixWriter.writeVInt((suffix<<1)|1);
            suffixWriter.writeBytes(block.prefix.bytes, prefixLength, suffix);
            assert block.fp < startFP;

            // if (DEBUG) {
            //    BytesRef suffixBytes = new BytesRef(suffix);
            //    System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //    suffixBytes.length = suffix;
            //    System.out.println("    " + (countx++) + ": write sub-block suffix=" + toString(suffixBytes) + " subFP=" + block.fp + " subCode=" + (startFP-block.fp) + " floor=" + block.isFloor);
            // }

            suffixWriter.writeVLong(startFP - block.fp);
            subIndices.add(block.index);
          }
        }

        assert subIndices.size() != 0;
      }

      // TODO: we could block-write the term suffix pointers;
      // this would take more space but would enable binary
      // search on lookup

      // Write suffixes byte[] blob to terms dict output:
      out.writeVInt((int) (suffixWriter.getFilePointer() << 1) | (isLeafBlock ? 1:0));
      suffixWriter.writeTo(out);
      suffixWriter.reset();

      // Write term meta data byte[] blob
      out.writeVInt((int) metaWriter.getFilePointer());
      metaWriter.writeTo(out);
      metaWriter.reset();

      // Remove slice replaced by block:
      slice.clear();

      if (lastBlockIndex >= start) {
        if (lastBlockIndex < start+length) {
          lastBlockIndex = start;
        } else {
          lastBlockIndex -= length;
        }
      }

      // if (DEBUG) {
      //   System.out.println("      fpEnd=" + out.getFilePointer());
      // }

      return new PendingBlock(prefix, maxVersionInBlock, startFP, termCount != 0, isFloor, floorLeadByte, subIndices);
    }

    TermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      docsSeen = new FixedBitSet(maxDoc);

      noOutputs = NoOutputs.getSingleton();

      // This Builder is just used transiently to fragment
      // terms into "good" blocks; we don't save the
      // resulting FST:
      blockBuilder = new Builder<>(FST.INPUT_TYPE.BYTE1,
                                   0, 0, true,
                                   true, Integer.MAX_VALUE,
                                   noOutputs,
                                   new FindBlocks(), false,
                                   PackedInts.COMPACT,
                                   true, 15);

      this.longsSize = postingsWriter.setField(fieldInfo);
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    private final IntsRef scratchIntsRef = new IntsRef();

    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      //if (DEBUG) System.out.println("\nBTTW.startTerm term=" + fieldInfo.name + ":" + toString(text) + " seg=" + segment);
      postingsWriter.startTerm();
      /*
      if (fieldInfo.name.equals("id")) {
        postingsWriter.termID = Integer.parseInt(text.utf8ToString());
      } else {
        postingsWriter.termID = -1;
      }
      */
      return postingsWriter;
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {

      assert stats.docFreq > 0;
      //if (DEBUG) System.out.println("BTTW.finishTerm term=" + fieldInfo.name + ":" + toString(text) + " seg=" + segment + " df=" + stats.docFreq);
      if (((IDVersionPostingsWriter) postingsWriter).lastDocID != -1) {
        blockBuilder.add(Util.toIntsRef(text, scratchIntsRef), noOutputs.getNoOutput());
        BlockTermState state = postingsWriter.newTermState();
        state.docFreq = stats.docFreq;
        state.totalTermFreq = stats.totalTermFreq;
        postingsWriter.finishTerm(state);

        PendingTerm term = new PendingTerm(BytesRef.deepCopyOf(text), state);
        pending.add(term);
        numTerms++;

        if (minTerm == null) {
          minTerm = BytesRef.deepCopyOf(text);
        }
        maxTerm.copyBytes(text);
      }
    }

    // Finishes all terms in this field
    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      if (numTerms > 0) {
        blockBuilder.finish();

        // We better have one final "root" block:
        assert pending.size() == 1 && !pending.get(0).isTerm: "pending.size()=" + pending.size() + " pending=" + pending;
        final PendingBlock root = (PendingBlock) pending.get(0);
        assert root.prefix.length == 0;
        assert root.index.getEmptyOutput() != null;

        // Write FST to index
        indexStartFP = indexOut.getFilePointer();
        root.index.save(indexOut);
        //System.out.println("  write FST " + indexStartFP + " field=" + fieldInfo.name);

        // if (SAVE_DOT_FILES || DEBUG) {
        //   final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
        //   Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
        //   Util.toDot(root.index, w, false, false);
        //   System.out.println("SAVED to " + dotFileName);
        //   w.close();
        // }

        fields.add(new FieldMetaData(fieldInfo,
                                     ((PendingBlock) pending.get(0)).index.getEmptyOutput(),
                                     numTerms,
                                     indexStartFP,
                                     longsSize,
                                     minTerm, maxTerm));
      } else {
        // cannot assert this: we skip deleted docIDs in the postings:
        // assert docsSeen.cardinality() == 0;
      }
    }

    private final RAMOutputStream suffixWriter = new RAMOutputStream();
    private final RAMOutputStream metaWriter = new RAMOutputStream();
    private final RAMOutputStream bytesWriter = new RAMOutputStream();
  }

  @Override
  public void close() throws IOException {

    boolean success = false;
    try {
      
      final long dirStart = out.getFilePointer();
      final long indexDirStart = indexOut.getFilePointer();

      out.writeVInt(fields.size());
      
      for(FieldMetaData field : fields) {
        //System.out.println("  field " + field.fieldInfo.name + " " + field.numTerms + " terms");
        out.writeVInt(field.fieldInfo.number);
        assert field.numTerms > 0;
        out.writeVLong(field.numTerms);
        out.writeVInt(field.rootCode.output1.length);
        out.writeBytes(field.rootCode.output1.bytes, field.rootCode.output1.offset, field.rootCode.output1.length);
        out.writeVLong(field.rootCode.output2);
        out.writeVInt(field.longsSize);
        indexOut.writeVLong(field.indexStartFP);
        writeBytesRef(out, field.minTerm);
        writeBytesRef(out, field.maxTerm);
      }
      writeTrailer(out, dirStart);
      CodecUtil.writeFooter(out);
      writeIndexTrailer(indexOut, indexDirStart);
      CodecUtil.writeFooter(indexOut);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(out, indexOut, postingsWriter);
      } else {
        IOUtils.closeWhileHandlingException(out, indexOut, postingsWriter);
      }
    }
  }

  private static void writeBytesRef(IndexOutput out, BytesRef bytes) throws IOException {
    out.writeVInt(bytes.length);
    out.writeBytes(bytes.bytes, bytes.offset, bytes.length);
  }
}
