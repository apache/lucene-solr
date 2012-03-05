package org.apache.lucene.codecs;

/**
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

import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.NoOutputs;
import org.apache.lucene.util.fst.Util;

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

/**
 * Writes terms dict and index, block-encoding (column
 * stride) each term's metadata for each set of terms
 * between two index terms.
 *
 * @lucene.experimental
 */

/** See {@link BlockTreeTermsReader}.
 *
 * @lucene.experimental
*/

public class BlockTreeTermsWriter extends FieldsConsumer {

  public final static int DEFAULT_MIN_BLOCK_SIZE = 25;
  public final static int DEFAULT_MAX_BLOCK_SIZE = 48;

  //public final static boolean DEBUG = false;
  public final static boolean SAVE_DOT_FILES = false;

  static final int OUTPUT_FLAGS_NUM_BITS = 2;
  static final int OUTPUT_FLAGS_MASK = 0x3;
  static final int OUTPUT_FLAG_IS_FLOOR = 0x1;
  static final int OUTPUT_FLAG_HAS_TERMS = 0x2;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tim";
  final static String TERMS_CODEC_NAME = "BLOCK_TREE_TERMS_DICT";
  // Initial format
  public static final int TERMS_VERSION_START = 0;
  public static final int TERMS_VERSION_CURRENT = TERMS_VERSION_START;

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tip";
  final static String TERMS_INDEX_CODEC_NAME = "BLOCK_TREE_TERMS_INDEX";
  // Initial format
  public static final int TERMS_INDEX_VERSION_START = 0;
  public static final int TERMS_INDEX_VERSION_CURRENT = TERMS_INDEX_VERSION_START;

  private final IndexOutput out;
  private final IndexOutput indexOut;
  final int minItemsInBlock;
  final int maxItemsInBlock;

  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  FieldInfo currentField;
  private final List<TermsWriter> fields = new ArrayList<TermsWriter>();
  // private final String segment;

  /** Create a new writer.  The number of items (terms or
   *  sub-blocks) per block will aim to be between
   *  minItemsPerBlock and maxItemsPerBlock, though in some
   *  cases the blocks may be smaller than the min. */
  public BlockTreeTermsWriter(
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

    final String termsFileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, TERMS_EXTENSION);
    out = state.directory.createOutput(termsFileName, state.context);
    boolean success = false;
    IndexOutput indexOut = null;
    try {
      fieldInfos = state.fieldInfos;
      this.minItemsInBlock = minItemsInBlock;
      this.maxItemsInBlock = maxItemsInBlock;
      writeHeader(out);

      //DEBUG = state.segmentName.equals("_4a");

      final String termsIndexFileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, TERMS_INDEX_EXTENSION);
      indexOut = state.directory.createOutput(termsIndexFileName, state.context);
      writeIndexHeader(indexOut);

      currentField = null;
      this.postingsWriter = postingsWriter;
      // segment = state.segmentName;

      // System.out.println("BTW.init seg=" + state.segmentName);

      postingsWriter.start(out);                          // have consumer write its format/header
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out, indexOut);
      }
    }
    this.indexOut = indexOut;
  }
  
  protected void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, TERMS_CODEC_NAME, TERMS_VERSION_CURRENT); 
    out.writeLong(0);                             // leave space for end index pointer    
  }

  protected void writeIndexHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, TERMS_INDEX_CODEC_NAME, TERMS_INDEX_VERSION_CURRENT); 
    out.writeLong(0);                             // leave space for end index pointer    
  }

  protected void writeTrailer(IndexOutput out, long dirStart) throws IOException {
    out.seek(CodecUtil.headerLength(TERMS_CODEC_NAME));
    out.writeLong(dirStart);    
  }

  protected void writeIndexTrailer(IndexOutput indexOut, long dirStart) throws IOException {
    indexOut.seek(CodecUtil.headerLength(TERMS_INDEX_CODEC_NAME));
    indexOut.writeLong(dirStart);    
  }
  
  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    //DEBUG = field.name.equals("id");
    //if (DEBUG) System.out.println("\nBTTW.addField seg=" + segment + " field=" + field.name);
    assert currentField == null || currentField.name.compareTo(field.name) < 0;
    currentField = field;
    final TermsWriter terms = new TermsWriter(field);
    fields.add(terms);
    return terms;
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
    public final TermStats stats;

    public PendingTerm(BytesRef term, TermStats stats) {
      super(true);
      this.term = term;
      this.stats = stats;
    }

    @Override
    public String toString() {
      return term.utf8ToString();
    }
  }

  private static final class PendingBlock extends PendingEntry {
    public final BytesRef prefix;
    public final long fp;
    public FST<BytesRef> index;
    public List<FST<BytesRef>> subIndices;
    public final boolean hasTerms;
    public final boolean isFloor;
    public final int floorLeadByte;
    private final IntsRef scratchIntsRef = new IntsRef();

    public PendingBlock(BytesRef prefix, long fp, boolean hasTerms, boolean isFloor, int floorLeadByte, List<FST<BytesRef>> subIndices) {
      super(false);
      this.prefix = prefix;
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

      // TODO: try writing the leading vLong in MSB order
      // (opposite of what Lucene does today), for better
      // outputs sharing in the FST
      scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));
      if (isFloor) {
        scratchBytes.writeVInt(floorBlocks.size());
        for (PendingBlock sub : floorBlocks) {
          assert sub.floorLeadByte != -1;
          //if (DEBUG) {
          //  System.out.println("    write floorLeadByte=" + Integer.toHexString(sub.floorLeadByte&0xff));
          //}
          scratchBytes.writeByte((byte) sub.floorLeadByte);
          assert sub.fp > fp;
          scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
        }
      }

      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final Builder<BytesRef> indexBuilder = new Builder<BytesRef>(FST.INPUT_TYPE.BYTE1,
                                                                   0, 0, true, false, Integer.MAX_VALUE,
                                                                   outputs, null, false);
      //if (DEBUG) {
      //  System.out.println("  compile index for prefix=" + prefix);
      //}
      //indexBuilder.DEBUG = false;
      final byte[] bytes = new byte[(int) scratchBytes.getFilePointer()];
      assert bytes.length > 0;
      scratchBytes.writeTo(bytes, 0);
      indexBuilder.add(Util.toIntsRef(prefix, scratchIntsRef), new BytesRef(bytes, 0, bytes.length));
      scratchBytes.reset();

      // Copy over index for all sub-blocks

      if (subIndices != null) {
        for(FST<BytesRef> subIndex : subIndices) {
          append(indexBuilder, subIndex);
        }
      }

      if (floorBlocks != null) {
        for (PendingBlock sub : floorBlocks) {
          if (sub.subIndices != null) {
            for(FST<BytesRef> subIndex : sub.subIndices) {
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
    private void append(Builder<BytesRef> builder, FST<BytesRef> subIndex) throws IOException {
      final BytesRefFSTEnum<BytesRef> subIndexEnum = new BytesRefFSTEnum<BytesRef>(subIndex);
      BytesRefFSTEnum.InputOutput<BytesRef> indexEnt;
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
    private long numTerms;
    long sumTotalTermFreq;
    long sumDocFreq;
    int docCount;
    long indexStartFP;

    // Used only to partition terms into the block tree; we
    // don't pull an FST from this builder:
    private final NoOutputs noOutputs;
    private final Builder<Object> blockBuilder;

    // PendingTerm or PendingBlock:
    private final List<PendingEntry> pending = new ArrayList<PendingEntry>();

    // Index into pending of most recently written block
    private int lastBlockIndex = -1;

    // Re-used when segmenting a too-large block into floor
    // blocks:
    private int[] subBytes = new int[10];
    private int[] subTermCounts = new int[10];
    private int[] subTermCountSums = new int[10];
    private int[] subSubCounts = new int[10];

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
          frontier[idx] = new Builder.UnCompiledNode<Object>(blockBuilder, idx);
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
      if (prefixLength == 0 || count <= maxItemsInBlock) {
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

        //if (DEBUG) {
        //  final BytesRef prefix = new BytesRef(prefixLength);
        //  for(int m=0;m<prefixLength;m++) {
        //    prefix.bytes[m] = (byte) prevTerm.ints[m];
        //  }
        //  prefix.length = prefixLength;
        //  //System.out.println("\nWBS count=" + count + " prefix=" + prefix.utf8ToString() + " " + prefix);
        //  System.out.println("writeBlocks: prefix=" + prefix + " " + prefix + " count=" + count + " pending.size()=" + pending.size());
        //}
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

        final List<PendingBlock> floorBlocks = new ArrayList<PendingBlock>();
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
      //   System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + " pending.size()=" + pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" + toString(prefix) + " entCount=" + length + " startFP=" + startFP + " futureTermCount=" + futureTermCount + (isFloor ? (" floorLeadByte=" + Integer.toHexString(floorLeadByte&0xff)) : "") + " isLastInFloor=" + isLastInFloor);
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

      final List<FST<BytesRef>> subIndices;

      int termCount;
      if (isLeafBlock) {
        subIndices = null;
        for (PendingEntry ent : slice) {
          assert ent.isTerm;
          PendingTerm term = (PendingTerm) ent;
          final int suffix = term.term.length - prefixLength;
          // if (DEBUG) {
          //   BytesRef suffixBytes = new BytesRef(suffix);
          //   System.arraycopy(term.term.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
          //   suffixBytes.length = suffix;
          //   System.out.println("    write term suffix=" + suffixBytes);
          // }
          // For leaf block we write suffix straight
          bytesWriter.writeVInt(suffix);
          bytesWriter.writeBytes(term.term.bytes, prefixLength, suffix);

          // Write term stats, to separate byte[] blob:
          bytesWriter2.writeVInt(term.stats.docFreq);
          if (fieldInfo.indexOptions != IndexOptions.DOCS_ONLY) {
            assert term.stats.totalTermFreq >= term.stats.docFreq;
            bytesWriter2.writeVLong(term.stats.totalTermFreq - term.stats.docFreq);
          }
        }
        termCount = length;
      } else {
        subIndices = new ArrayList<FST<BytesRef>>();
        termCount = 0;
        for (PendingEntry ent : slice) {
          if (ent.isTerm) {
            PendingTerm term = (PendingTerm) ent;
            final int suffix = term.term.length - prefixLength;
            // if (DEBUG) {
            //   BytesRef suffixBytes = new BytesRef(suffix);
            //   System.arraycopy(term.term.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //   suffixBytes.length = suffix;
            //   System.out.println("    write term suffix=" + suffixBytes);
            // }
            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block
            bytesWriter.writeVInt(suffix<<1);
            bytesWriter.writeBytes(term.term.bytes, prefixLength, suffix);

            // Write term stats, to separate byte[] blob:
            bytesWriter2.writeVInt(term.stats.docFreq);
            if (fieldInfo.indexOptions != IndexOptions.DOCS_ONLY) {
              assert term.stats.totalTermFreq >= term.stats.docFreq;
              bytesWriter2.writeVLong(term.stats.totalTermFreq - term.stats.docFreq);
            }

            termCount++;
          } else {
            PendingBlock block = (PendingBlock) ent;
            final int suffix = block.prefix.length - prefixLength;

            assert suffix > 0;

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block
            bytesWriter.writeVInt((suffix<<1)|1);
            bytesWriter.writeBytes(block.prefix.bytes, prefixLength, suffix);
            assert block.fp < startFP;

            // if (DEBUG) {
            //   BytesRef suffixBytes = new BytesRef(suffix);
            //   System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //   suffixBytes.length = suffix;
            //   System.out.println("    write sub-block suffix=" + toString(suffixBytes) + " subFP=" + block.fp + " subCode=" + (startFP-block.fp) + " floor=" + block.isFloor);
            // }

            bytesWriter.writeVLong(startFP - block.fp);
            subIndices.add(block.index);
          }
        }

        assert subIndices.size() != 0;
      }

      // TODO: we could block-write the term suffix pointers;
      // this would take more space but would enable binary
      // search on lookup

      // Write suffixes byte[] blob to terms dict output:
      out.writeVInt((int) (bytesWriter.getFilePointer() << 1) | (isLeafBlock ? 1:0));
      bytesWriter.writeTo(out);
      bytesWriter.reset();

      // Write term stats byte[] blob
      out.writeVInt((int) bytesWriter2.getFilePointer());
      bytesWriter2.writeTo(out);
      bytesWriter2.reset();

      // Have postings writer write block
      postingsWriter.flushTermsBlock(futureTermCount+termCount, termCount);

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

      return new PendingBlock(prefix, startFP, termCount != 0, isFloor, floorLeadByte, subIndices);
    }

    TermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;

      noOutputs = NoOutputs.getSingleton();

      // This Builder is just used transiently to fragment
      // terms into "good" blocks; we don't save the
      // resulting FST:
      blockBuilder = new Builder<Object>(FST.INPUT_TYPE.BYTE1,
                                         0, 0, true,
                                         true, Integer.MAX_VALUE,
                                         noOutputs,
                                         new FindBlocks(), false);

      postingsWriter.setField(fieldInfo);
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

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

    private final IntsRef scratchIntsRef = new IntsRef();

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {

      assert stats.docFreq > 0;
      //if (DEBUG) System.out.println("BTTW.finishTerm term=" + fieldInfo.name + ":" + toString(text) + " seg=" + segment + " df=" + stats.docFreq);

      blockBuilder.add(Util.toIntsRef(text, scratchIntsRef), noOutputs.getNoOutput());
      pending.add(new PendingTerm(BytesRef.deepCopyOf(text), stats));
      postingsWriter.finishTerm(stats);
      numTerms++;
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

        this.sumTotalTermFreq = sumTotalTermFreq;
        this.sumDocFreq = sumDocFreq;
        this.docCount = docCount;

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
      }
    }

    private final RAMOutputStream bytesWriter = new RAMOutputStream();
    private final RAMOutputStream bytesWriter2 = new RAMOutputStream();
  }

  @Override
  public void close() throws IOException {

    IOException ioe = null;
    try {
      
      int nonZeroCount = 0;
      for(TermsWriter field : fields) {
        if (field.numTerms > 0) {
          nonZeroCount++;
        }
      }

      final long dirStart = out.getFilePointer();
      final long indexDirStart = indexOut.getFilePointer();

      out.writeVInt(nonZeroCount);
      
      for(TermsWriter field : fields) {
        if (field.numTerms > 0) {
          //System.out.println("  field " + field.fieldInfo.name + " " + field.numTerms + " terms");
          out.writeVInt(field.fieldInfo.number);
          out.writeVLong(field.numTerms);
          final BytesRef rootCode = ((PendingBlock) field.pending.get(0)).index.getEmptyOutput();
          assert rootCode != null: "field=" + field.fieldInfo.name + " numTerms=" + field.numTerms;
          out.writeVInt(rootCode.length);
          out.writeBytes(rootCode.bytes, rootCode.offset, rootCode.length);
          if (field.fieldInfo.indexOptions != IndexOptions.DOCS_ONLY) {
            out.writeVLong(field.sumTotalTermFreq);
          }
          out.writeVLong(field.sumDocFreq);
          out.writeVInt(field.docCount);
          indexOut.writeVLong(field.indexStartFP);
        }
      }
      writeTrailer(out, dirStart);
      writeIndexTrailer(indexOut, indexDirStart);
    } catch (IOException ioe2) {
      ioe = ioe2;
    } finally {
      IOUtils.closeWhileHandlingException(ioe, out, indexOut, postingsWriter);
    }
  }
}
