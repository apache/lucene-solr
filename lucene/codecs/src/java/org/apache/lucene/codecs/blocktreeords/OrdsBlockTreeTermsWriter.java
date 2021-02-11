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
package org.apache.lucene.codecs.blocktreeords;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blocktreeords.FSTOrdsOutputs.Output;
import org.apache.lucene.codecs.lucene90.blocktree.BlockTreeTermsWriter; // javadocs
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
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
*/

/**
 * This is just like {@link BlockTreeTermsWriter}, except it also stores a version per term, and
 * adds a method to its TermsEnum implementation to seekExact only if the version is &gt;= the
 * specified version. The version is added to the terms index to avoid seeking if no term in the
 * block has a high enough version. The term blocks file is .tiv and the terms index extension is
 * .tipv.
 *
 * @lucene.experimental
 */
public final class OrdsBlockTreeTermsWriter extends FieldsConsumer {

  static final FSTOrdsOutputs FST_OUTPUTS = new FSTOrdsOutputs();

  static final Output NO_OUTPUT = FST_OUTPUTS.getNoOutput();

  /**
   * Suggested default value for the {@code minItemsInBlock} parameter to {@link
   * #OrdsBlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}.
   */
  public static final int DEFAULT_MIN_BLOCK_SIZE = 25;

  /**
   * Suggested default value for the {@code maxItemsInBlock} parameter to {@link
   * #OrdsBlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}.
   */
  public static final int DEFAULT_MAX_BLOCK_SIZE = 48;

  // public final static boolean DEBUG = false;
  // private final static boolean SAVE_DOT_FILES = false;

  static final int OUTPUT_FLAGS_NUM_BITS = 2;
  static final int OUTPUT_FLAGS_MASK = 0x3;
  static final int OUTPUT_FLAG_IS_FLOOR = 0x1;
  static final int OUTPUT_FLAG_HAS_TERMS = 0x2;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tio";

  static final String TERMS_CODEC_NAME = "OrdsBlockTreeTerms";

  /** Initial terms format. */
  public static final int VERSION_START = 1;

  /** Current terms format. */
  public static final int VERSION_CURRENT = VERSION_START;

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tipo";

  static final String TERMS_INDEX_CODEC_NAME = "OrdsBlockTreeIndex";

  private final IndexOutput out;
  private final IndexOutput indexOut;
  final int maxDoc;
  final int minItemsInBlock;
  final int maxItemsInBlock;

  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;

  private static class FieldMetaData {
    public final FieldInfo fieldInfo;
    public final Output rootCode;
    public final long numTerms;
    public final long indexStartFP;
    public final long sumTotalTermFreq;
    public final long sumDocFreq;
    public final int docCount;
    public final BytesRef minTerm;
    public final BytesRef maxTerm;

    public FieldMetaData(
        FieldInfo fieldInfo,
        Output rootCode,
        long numTerms,
        long indexStartFP,
        long sumTotalTermFreq,
        long sumDocFreq,
        int docCount,
        BytesRef minTerm,
        BytesRef maxTerm) {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      assert rootCode != null : "field=" + fieldInfo.name + " numTerms=" + numTerms;
      this.rootCode = rootCode;
      this.indexStartFP = indexStartFP;
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
      this.minTerm = minTerm;
      this.maxTerm = maxTerm;
    }
  }

  private final List<FieldMetaData> fields = new ArrayList<>();

  // private final String segment;

  /**
   * Create a new writer. The number of items (terms or sub-blocks) per block will aim to be between
   * minItemsPerBlock and maxItemsPerBlock, though in some cases the blocks may be smaller than the
   * min.
   */
  public OrdsBlockTreeTermsWriter(
      SegmentWriteState state,
      PostingsWriterBase postingsWriter,
      int minItemsInBlock,
      int maxItemsInBlock)
      throws IOException {
    BlockTreeTermsWriter.validateSettings(minItemsInBlock, maxItemsInBlock);

    maxDoc = state.segmentInfo.maxDoc();

    final String termsFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, TERMS_EXTENSION);
    out = state.directory.createOutput(termsFileName, state.context);
    boolean success = false;
    IndexOutput indexOut = null;
    try {
      fieldInfos = state.fieldInfos;
      this.minItemsInBlock = minItemsInBlock;
      this.maxItemsInBlock = maxItemsInBlock;
      CodecUtil.writeIndexHeader(
          out, TERMS_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

      final String termsIndexFileName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, TERMS_INDEX_EXTENSION);
      indexOut = state.directory.createOutput(termsIndexFileName, state.context);
      CodecUtil.writeIndexHeader(
          indexOut,
          TERMS_INDEX_CODEC_NAME,
          VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);

      this.postingsWriter = postingsWriter;
      // segment = state.segmentInfo.name;

      // System.out.println("BTW.init seg=" + state.segmentName);

      postingsWriter.init(out, state); // have consumer write its format/header
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out, indexOut);
      }
    }
    this.indexOut = indexOut;
  }

  @Override
  public void write(Fields fields, NormsProducer norms) throws IOException {

    String lastField = null;
    for (String field : fields) {
      assert lastField == null || lastField.compareTo(field) < 0;
      lastField = field;

      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }

      TermsEnum termsEnum = terms.iterator();

      TermsWriter termsWriter = new TermsWriter(fieldInfos.fieldInfo(field));
      while (true) {
        BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }
        termsWriter.write(term, termsEnum, norms);
      }

      termsWriter.finish();
    }
  }

  static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
    assert fp < (1L << 62);
    return (fp << 2)
        | (hasTerms ? OUTPUT_FLAG_HAS_TERMS : 0)
        | (isFloor ? OUTPUT_FLAG_IS_FLOOR : 0);
  }

  private static class PendingEntry {
    public final boolean isTerm;

    protected PendingEntry(boolean isTerm) {
      this.isTerm = isTerm;
    }
  }

  private static final class PendingTerm extends PendingEntry {
    public final byte[] termBytes;
    // stats + metadata
    public final BlockTermState state;

    public PendingTerm(BytesRef term, BlockTermState state) {
      super(true);
      this.termBytes = new byte[term.length];
      System.arraycopy(term.bytes, term.offset, termBytes, 0, term.length);
      this.state = state;
    }

    @Override
    public String toString() {
      return brToString(termBytes);
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      // If BytesRef isn't actually UTF8, or it's eg a
      // prefix of UTF8 that ends mid-unicode-char, we
      // fallback to hex:
      return b.toString();
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(byte[] b) {
    return brToString(new BytesRef(b));
  }

  private static final class SubIndex {
    public final FST<Output> index;
    public final long termOrdStart;

    public SubIndex(FST<Output> index, long termOrdStart) {
      this.index = index;
      this.termOrdStart = termOrdStart;
    }
  }

  private static final class PendingBlock extends PendingEntry {
    public final BytesRef prefix;
    public final long fp;
    public FST<Output> index;
    public List<SubIndex> subIndices;
    public final boolean hasTerms;
    public final boolean isFloor;
    public final int floorLeadByte;
    public long totFloorTermCount;
    private final long totalTermCount;

    public PendingBlock(
        BytesRef prefix,
        long fp,
        boolean hasTerms,
        long totalTermCount,
        boolean isFloor,
        int floorLeadByte,
        List<SubIndex> subIndices) {
      super(false);
      this.prefix = prefix;
      this.fp = fp;
      this.hasTerms = hasTerms;
      this.totalTermCount = totalTermCount;
      assert totalTermCount > 0;
      this.isFloor = isFloor;
      this.floorLeadByte = floorLeadByte;
      this.subIndices = subIndices;
    }

    @Override
    public String toString() {
      return "BLOCK: " + brToString(prefix);
    }

    public void compileIndex(
        List<PendingBlock> blocks,
        ByteBuffersDataOutput scratchBytes,
        IntsRefBuilder scratchIntsRef)
        throws IOException {

      assert (isFloor && blocks.size() > 1) || (isFloor == false && blocks.size() == 1)
          : "isFloor=" + isFloor + " blocks=" + blocks;
      assert this == blocks.get(0);

      assert scratchBytes.size() == 0;

      // TODO: try writing the leading vLong in MSB order
      // (opposite of what Lucene does today), for better
      // outputs sharing in the FST
      long lastSumTotalTermCount = 0;
      long sumTotalTermCount = totalTermCount;
      scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));
      if (isFloor) {
        scratchBytes.writeVInt(blocks.size() - 1);
        for (int i = 1; i < blocks.size(); i++) {
          PendingBlock sub = blocks.get(i);
          assert sub.floorLeadByte != -1;
          // if (DEBUG) {
          //  System.out.println("    write floorLeadByte=" +
          // Integer.toHexString(sub.floorLeadByte&0xff));
          // }
          scratchBytes.writeByte((byte) sub.floorLeadByte);
          // System.out.println("  write floor byte=" + (byte) sub.floorLeadByte + " ordShift=" +
          // sumTotalTermCount);
          scratchBytes.writeVLong(sumTotalTermCount - lastSumTotalTermCount);
          lastSumTotalTermCount = sumTotalTermCount;
          sumTotalTermCount += sub.totalTermCount;
          assert sub.fp > fp;
          scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
        }
      }

      final FSTCompiler<Output> fstCompiler =
          new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, FST_OUTPUTS)
              .shouldShareNonSingletonNodes(false)
              .build();
      // if (DEBUG) {
      //  System.out.println("  compile index for prefix=" + prefix);
      // }
      // indexBuilder.DEBUG = false;
      final byte[] bytes = scratchBytes.toArrayCopy();
      assert bytes.length > 0;
      fstCompiler.add(
          Util.toIntsRef(prefix, scratchIntsRef),
          FST_OUTPUTS.newOutput(
              new BytesRef(bytes, 0, bytes.length), 0, Long.MAX_VALUE - (sumTotalTermCount - 1)));
      scratchBytes.reset();

      // Copy over index for all sub-blocks

      long termOrdOffset = 0;
      for (PendingBlock block : blocks) {
        if (block.subIndices != null) {
          for (SubIndex subIndex : block.subIndices) {
            append(
                fstCompiler, subIndex.index, termOrdOffset + subIndex.termOrdStart, scratchIntsRef);
          }
          block.subIndices = null;
        }
        termOrdOffset += block.totalTermCount;
      }
      totFloorTermCount = termOrdOffset;

      assert sumTotalTermCount == totFloorTermCount;

      index = fstCompiler.compile();
      assert subIndices == null;

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
    private void append(
        FSTCompiler<Output> fstCompiler,
        FST<Output> subIndex,
        long termOrdOffset,
        IntsRefBuilder scratchIntsRef)
        throws IOException {
      final BytesRefFSTEnum<Output> subIndexEnum = new BytesRefFSTEnum<>(subIndex);
      BytesRefFSTEnum.InputOutput<Output> indexEnt;
      while ((indexEnt = subIndexEnum.next()) != null) {
        // if (DEBUG) {
        //  System.out.println("      add sub=" + indexEnt.input + " " + indexEnt.input + " output="
        // + indexEnt.output);
        // }
        Output output = indexEnt.output;
        // long blockTermCount = output.endOrd - output.startOrd + 1;
        Output newOutput =
            FST_OUTPUTS.newOutput(
                output.bytes, termOrdOffset + output.startOrd, output.endOrd - termOrdOffset);
        // System.out.println("  append sub=" + indexEnt.input + " output=" + indexEnt.output + "
        // termOrdOffset=" + termOrdOffset + " blockTermCount=" + blockTermCount  + " newOutput=" +
        // newOutput  + " endOrd=" + (termOrdOffset+Long.MAX_VALUE-output.endOrd));
        fstCompiler.add(Util.toIntsRef(indexEnt.input, scratchIntsRef), newOutput);
      }
    }
  }

  private final ByteBuffersDataOutput scratchBytes = ByteBuffersDataOutput.newResettableInstance();
  private final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();

  class TermsWriter {
    private final FieldInfo fieldInfo;
    private long numTerms;
    final FixedBitSet docsSeen;
    long sumTotalTermFreq;
    long sumDocFreq;
    long indexStartFP;

    // Records index into pending where the current prefix at that
    // length "started"; for example, if current term starts with 't',
    // startsByPrefix[0] is the index into pending for the first
    // term/sub-block starting with 't'.  We use this to figure out when
    // to write a new block:
    private final BytesRefBuilder lastTerm = new BytesRefBuilder();
    private int[] prefixStarts = new int[8];

    // Pending stack of terms and blocks.  As terms arrive (in sorted order)
    // we append to this stack, and once the top of the stack has enough
    // terms starting with a common prefix, we write a new block with
    // those terms and replace those terms in the stack with a new block:
    private final List<PendingEntry> pending = new ArrayList<>();

    // Reused in writeBlocks:
    private final List<PendingBlock> newBlocks = new ArrayList<>();

    private PendingTerm firstPendingTerm;
    private PendingTerm lastPendingTerm;

    /** Writes the top count entries in pending, using prevTerm to compute the prefix. */
    void writeBlocks(int prefixLength, int count) throws IOException {

      assert count > 0;

      /*
      if (DEBUG) {
        BytesRef br = new BytesRef(lastTerm.bytes);
        br.offset = lastTerm.offset;
        br.length = prefixLength;
        System.out.println("writeBlocks: " + br.utf8ToString() + " count=" + count);
      }
      */

      // Root block better write all remaining pending entries:
      assert prefixLength > 0 || count == pending.size();

      int lastSuffixLeadLabel = -1;

      // True if we saw at least one term in this block (we record if a block
      // only points to sub-blocks in the terms index so we can avoid seeking
      // to it when we are looking for a term):
      boolean hasTerms = false;
      boolean hasSubBlocks = false;

      int start = pending.size() - count;
      int end = pending.size();
      int nextBlockStart = start;
      int nextFloorLeadLabel = -1;

      for (int i = start; i < end; i++) {

        PendingEntry ent = pending.get(i);

        int suffixLeadLabel;

        if (ent.isTerm) {
          PendingTerm term = (PendingTerm) ent;
          if (term.termBytes.length == prefixLength) {
            // Suffix is 0, i.e. prefix 'foo' and term is
            // 'foo' so the term has empty string suffix
            // in this block
            assert lastSuffixLeadLabel == -1;
            suffixLeadLabel = -1;
          } else {
            suffixLeadLabel = term.termBytes[prefixLength] & 0xff;
          }
        } else {
          PendingBlock block = (PendingBlock) ent;
          assert block.prefix.length > prefixLength;
          suffixLeadLabel = block.prefix.bytes[block.prefix.offset + prefixLength] & 0xff;
        }
        // if (DEBUG) System.out.println("  i=" + i + " ent=" + ent + " suffixLeadLabel=" +
        // suffixLeadLabel);

        if (suffixLeadLabel != lastSuffixLeadLabel) {
          int itemsInBlock = i - nextBlockStart;
          if (itemsInBlock >= minItemsInBlock && end - nextBlockStart > maxItemsInBlock) {
            // The count is too large for one block, so we must break it into "floor" blocks, where
            // we record
            // the leading label of the suffix of the first term in each floor block, so at search
            // time we can
            // jump to the right floor block.  We just use a naive greedy segmenter here: make a new
            // floor
            // block as soon as we have at least minItemsInBlock.  This is not always best: it often
            // produces
            // a too-small block as the final block:
            boolean isFloor = itemsInBlock < count;
            newBlocks.add(
                writeBlock(
                    prefixLength,
                    isFloor,
                    nextFloorLeadLabel,
                    nextBlockStart,
                    i,
                    hasTerms,
                    hasSubBlocks));

            hasTerms = false;
            hasSubBlocks = false;
            nextFloorLeadLabel = suffixLeadLabel;
            nextBlockStart = i;
          }

          lastSuffixLeadLabel = suffixLeadLabel;
        }

        if (ent.isTerm) {
          hasTerms = true;
        } else {
          hasSubBlocks = true;
        }
      }

      // Write last block, if any:
      if (nextBlockStart < end) {
        int itemsInBlock = end - nextBlockStart;
        boolean isFloor = itemsInBlock < count;
        newBlocks.add(
            writeBlock(
                prefixLength,
                isFloor,
                nextFloorLeadLabel,
                nextBlockStart,
                end,
                hasTerms,
                hasSubBlocks));
      }

      assert newBlocks.isEmpty() == false;

      PendingBlock firstBlock = newBlocks.get(0);

      assert firstBlock.isFloor || newBlocks.size() == 1;

      firstBlock.compileIndex(newBlocks, scratchBytes, scratchIntsRef);

      // Remove slice from the top of the pending stack, that we just wrote:
      pending.subList(pending.size() - count, pending.size()).clear();

      // Append new block
      pending.add(firstBlock);

      newBlocks.clear();
    }

    /**
     * Writes the specified slice (start is inclusive, end is exclusive) from pending stack as a new
     * block. If isFloor is true, there were too many (more than maxItemsInBlock) entries sharing
     * the same prefix, and so we broke it into multiple floor blocks where we record the starting
     * label of the suffix of each floor block.
     */
    private PendingBlock writeBlock(
        int prefixLength,
        boolean isFloor,
        int floorLeadLabel,
        int start,
        int end,
        boolean hasTerms,
        boolean hasSubBlocks)
        throws IOException {

      assert end > start;

      long startFP = out.getFilePointer();

      // if (DEBUG) System.out.println("    writeBlock fp=" + startFP + " isFloor=" + isFloor + "
      // floorLeadLabel=" + floorLeadLabel + " start=" + start + " end=" + end + " hasTerms=" +
      // hasTerms + " hasSubBlocks=" + hasSubBlocks);

      boolean hasFloorLeadLabel = isFloor && floorLeadLabel != -1;

      final BytesRef prefix = new BytesRef(prefixLength + (hasFloorLeadLabel ? 1 : 0));
      System.arraycopy(lastTerm.bytes(), 0, prefix.bytes, 0, prefixLength);
      prefix.length = prefixLength;

      // Write block header:
      int numEntries = end - start;
      int code = numEntries << 1;
      if (end == pending.size()) {
        // Last block:
        code |= 1;
      }
      out.writeVInt(code);

      // if (DEBUG) {
      //   System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + "
      // pending.size()=" + pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" +
      // brToString(prefix) + " entCount=" + length + " startFP=" + startFP + (isFloor ? ("
      // floorLeadByte=" + Integer.toHexString(floorLeadByte&0xff)) : "") + " isLastInFloor=" +
      // isLastInFloor);
      // }

      final List<SubIndex> subIndices;

      // We optimize the leaf block case (block has only terms), writing a more
      // compact format in this case:
      boolean isLeafBlock = hasSubBlocks == false;

      // Number of terms in this block and all sub-blocks (recursively)
      long totalTermCount;

      boolean absolute = true;

      if (isLeafBlock) {
        // Only terms:
        subIndices = null;
        for (int i = start; i < end; i++) {
          PendingEntry ent = pending.get(i);
          assert ent.isTerm : "i=" + i;

          PendingTerm term = (PendingTerm) ent;
          assert StringHelper.startsWith(term.termBytes, prefix) : term + " prefix=" + prefix;
          BlockTermState state = term.state;
          final int suffix = term.termBytes.length - prefixLength;
          /*
          if (DEBUG) {
            BytesRef suffixBytes = new BytesRef(suffix);
            System.arraycopy(term.term.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            suffixBytes.length = suffix;
            System.out.println("    write term suffix=" + suffixBytes);
          }
          */
          // For leaf block we write suffix straight
          suffixWriter.writeVInt(suffix);
          suffixWriter.writeBytes(term.termBytes, prefixLength, suffix);
          assert floorLeadLabel == -1 || (term.termBytes[prefixLength] & 0xff) >= floorLeadLabel;

          // Write term stats, to separate byte[] blob:
          statsWriter.writeVInt(state.docFreq);
          if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
            assert state.totalTermFreq >= state.docFreq
                : state.totalTermFreq + " vs " + state.docFreq;
            statsWriter.writeVLong(state.totalTermFreq - state.docFreq);
          }

          // Write term meta data
          postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
          absolute = false;
        }
        totalTermCount = end - start;
      } else {
        // Mixed terms and sub-blocks:
        subIndices = new ArrayList<>();
        totalTermCount = 0;
        for (int i = start; i < end; i++) {
          PendingEntry ent = pending.get(i);
          if (ent.isTerm) {
            PendingTerm term = (PendingTerm) ent;
            assert StringHelper.startsWith(term.termBytes, prefix) : term + " prefix=" + prefix;
            BlockTermState state = term.state;
            final int suffix = term.termBytes.length - prefixLength;
            /*
            if (DEBUG) {
              BytesRef suffixBytes = new BytesRef(suffix);
              System.arraycopy(term.term.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
              suffixBytes.length = suffix;
              System.out.println("    write term suffix=" + suffixBytes);
            }
            */
            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block
            suffixWriter.writeVInt(suffix << 1);
            suffixWriter.writeBytes(term.termBytes, prefixLength, suffix);
            assert floorLeadLabel == -1 || (term.termBytes[prefixLength] & 0xff) >= floorLeadLabel;

            // Write term stats, to separate byte[] blob:
            statsWriter.writeVInt(state.docFreq);
            if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
              assert state.totalTermFreq >= state.docFreq;
              statsWriter.writeVLong(state.totalTermFreq - state.docFreq);
            }

            // TODO: now that terms dict "sees" these longs,
            // we can explore better column-stride encodings
            // to encode all long[0]s for this block at
            // once, all long[1]s, etc., e.g. using
            // Simple64.  Alternatively, we could interleave
            // stats + meta ... no reason to have them
            // separate anymore:

            // Write term meta data
            postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
            absolute = false;

            totalTermCount++;
          } else {
            PendingBlock block = (PendingBlock) ent;
            assert StringHelper.startsWith(block.prefix, prefix);
            final int suffix = block.prefix.length - prefixLength;

            assert suffix > 0;

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block
            suffixWriter.writeVInt((suffix << 1) | 1);
            suffixWriter.writeBytes(block.prefix.bytes, prefixLength, suffix);

            assert floorLeadLabel == -1
                || (block.prefix.bytes[prefixLength] & 0xff) >= floorLeadLabel;

            assert block.fp < startFP;

            /*
            if (DEBUG) {
              BytesRef suffixBytes = new BytesRef(suffix);
              System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
              suffixBytes.length = suffix;
              System.out.println("    write sub-block suffix=" + brToString(suffixBytes) + " subFP=" + block.fp + " subCode=" + (startFP-block.fp) + " floor=" + block.isFloor);
            }
            */

            suffixWriter.writeVLong(startFP - block.fp);
            suffixWriter.writeVLong(block.totFloorTermCount);
            subIndices.add(new SubIndex(block.index, totalTermCount));
            totalTermCount += block.totFloorTermCount;
          }
        }

        assert subIndices.size() != 0;
      }

      // TODO: we could block-write the term suffix pointers;
      // this would take more space but would enable binary
      // search on lookup

      // Write suffixes byte[] blob to terms dict output:
      out.writeVInt((int) (suffixWriter.size() << 1) | (isLeafBlock ? 1 : 0));
      suffixWriter.copyTo(out);
      suffixWriter.reset();

      // Write term stats byte[] blob
      out.writeVInt((int) statsWriter.size());
      statsWriter.copyTo(out);
      statsWriter.reset();

      // Write term meta data byte[] blob
      out.writeVInt((int) metaWriter.size());
      metaWriter.copyTo(out);
      metaWriter.reset();

      // if (DEBUG) {
      //   System.out.println("      fpEnd=" + out.getFilePointer());
      // }

      if (hasFloorLeadLabel) {
        // We already allocated to length+1 above:
        prefix.bytes[prefix.length++] = (byte) floorLeadLabel;
      }

      return new PendingBlock(
          prefix, startFP, hasTerms, totalTermCount, isFloor, floorLeadLabel, subIndices);
    }

    TermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      docsSeen = new FixedBitSet(maxDoc);
      postingsWriter.setField(fieldInfo);
    }

    /** Writes one term's worth of postings. */
    public void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
      /*
      if (DEBUG) {
        int[] tmp = new int[lastTerm.length];
        System.arraycopy(prefixStarts, 0, tmp, 0, tmp.length);
        System.out.println("BTTW: write term=" + brToString(text) + " prefixStarts=" + Arrays.toString(tmp) + " pending.size()=" + pending.size());
      }
      */

      BlockTermState state = postingsWriter.writeTerm(text, termsEnum, docsSeen, norms);
      if (state != null) {
        assert state.docFreq != 0;
        assert fieldInfo.getIndexOptions() == IndexOptions.DOCS
                || state.totalTermFreq >= state.docFreq
            : "postingsWriter=" + postingsWriter;
        sumDocFreq += state.docFreq;
        sumTotalTermFreq += state.totalTermFreq;
        pushTerm(text);

        PendingTerm term = new PendingTerm(BytesRef.deepCopyOf(text), state);
        pending.add(term);
        numTerms++;
        if (firstPendingTerm == null) {
          firstPendingTerm = term;
        }
        lastPendingTerm = term;
      }
    }

    /** Pushes the new term to the top of the stack, and writes new blocks. */
    private void pushTerm(BytesRef text) throws IOException {
      int limit = Math.min(lastTerm.length(), text.length);

      // Find common prefix between last term and current term:
      int pos = 0;
      while (pos < limit && lastTerm.byteAt(pos) == text.bytes[text.offset + pos]) {
        pos++;
      }

      // if (DEBUG) System.out.println("  shared=" + pos + "  lastTerm.length=" + lastTerm.length);

      // Close the "abandoned" suffix now:
      for (int i = lastTerm.length() - 1; i >= pos; i--) {

        // How many items on top of the stack share the current suffix
        // we are closing:
        int prefixTopSize = pending.size() - prefixStarts[i];
        if (prefixTopSize >= minItemsInBlock) {
          // if (DEBUG) System.out.println("pushTerm i=" + i + " prefixTopSize=" + prefixTopSize + "
          // minItemsInBlock=" + minItemsInBlock);
          writeBlocks(i + 1, prefixTopSize);
          prefixStarts[i] -= prefixTopSize - 1;
        }
      }

      if (prefixStarts.length < text.length) {
        prefixStarts = ArrayUtil.grow(prefixStarts, text.length);
      }

      // Init new tail:
      for (int i = pos; i < text.length; i++) {
        prefixStarts[i] = pending.size();
      }

      lastTerm.copyBytes(text);
    }

    // Finishes all terms in this field
    public void finish() throws IOException {
      if (numTerms > 0) {
        // if (DEBUG) System.out.println("BTTW.finish pending.size()=" + pending.size());

        // TODO: if pending.size() is already 1 with a non-zero prefix length
        // we can save writing a "degenerate" root block, but we have to
        // fix all the places that assume the root block's prefix is the empty string:
        writeBlocks(0, pending.size());

        // We better have one final "root" block:
        assert pending.size() == 1 && !pending.get(0).isTerm
            : "pending.size()=" + pending.size() + " pending=" + pending;
        final PendingBlock root = (PendingBlock) pending.get(0);
        assert root.prefix.length == 0;
        assert root.index.getEmptyOutput() != null;

        // Write FST to index
        indexStartFP = indexOut.getFilePointer();
        root.index.save(indexOut, indexOut);
        // System.out.println("  write FST " + indexStartFP + " field=" + fieldInfo.name);

        // if (SAVE_DOT_FILES || DEBUG) {
        //   final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
        //   Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
        //   Util.toDot(root.index, w, false, false);
        //   System.out.println("SAVED to " + dotFileName);
        //   w.close();
        // }

        assert firstPendingTerm != null;
        BytesRef minTerm = new BytesRef(firstPendingTerm.termBytes);

        assert lastPendingTerm != null;
        BytesRef maxTerm = new BytesRef(lastPendingTerm.termBytes);

        fields.add(
            new FieldMetaData(
                fieldInfo,
                ((PendingBlock) pending.get(0)).index.getEmptyOutput(),
                numTerms,
                indexStartFP,
                sumTotalTermFreq,
                sumDocFreq,
                docsSeen.cardinality(),
                minTerm,
                maxTerm));
      } else {
        assert docsSeen.cardinality() == 0;
      }
    }

    private final ByteBuffersDataOutput suffixWriter =
        ByteBuffersDataOutput.newResettableInstance();
    private final ByteBuffersDataOutput statsWriter = ByteBuffersDataOutput.newResettableInstance();
    private final ByteBuffersDataOutput metaWriter = ByteBuffersDataOutput.newResettableInstance();
  }

  private boolean closed;

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    boolean success = false;
    try {

      final long dirStart = out.getFilePointer();
      final long indexDirStart = indexOut.getFilePointer();

      out.writeVInt(fields.size());

      for (FieldMetaData field : fields) {
        // System.out.println("  field " + field.fieldInfo.name + " " + field.numTerms + " terms
        // longsSize=" + field.longsSize);
        out.writeVInt(field.fieldInfo.number);
        assert field.numTerms > 0;
        out.writeVLong(field.numTerms);
        out.writeVInt(field.rootCode.bytes.length);
        out.writeBytes(
            field.rootCode.bytes.bytes, field.rootCode.bytes.offset, field.rootCode.bytes.length);
        if (field.fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
          out.writeVLong(field.sumTotalTermFreq);
        }
        out.writeVLong(field.sumDocFreq);
        out.writeVInt(field.docCount);
        indexOut.writeVLong(field.indexStartFP);
        writeBytesRef(out, field.minTerm);
        writeBytesRef(out, field.maxTerm);
      }
      out.writeLong(dirStart);
      CodecUtil.writeFooter(out);
      indexOut.writeLong(indexDirStart);
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
