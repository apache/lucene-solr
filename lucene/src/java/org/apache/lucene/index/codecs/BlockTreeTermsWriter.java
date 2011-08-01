package org.apache.lucene.index.codecs;

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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.NoOutputs;
import org.apache.lucene.util.fst.Util;

// TODO: currently we encode all terms between two indexed
// terms as a block; but, we could decouple the two, ie
// allow several blocks in between two indexed terms

/**
 * Writes terms dict and index, block-encoding (column
 * stride) each term's metadata for each set of terms
 * between two index terms.
 *
 * @lucene.experimental
 */

// nocommit -- somehow save/print stats for debugging, eg
// how many normal blocks, floor blocks, etc.

public class BlockTreeTermsWriter extends FieldsConsumer {

  public static boolean DEBUG = false;
  public static boolean DEBUG2 = false;
  public static boolean SAVE_DOT_FILES = false;

  final static String CODEC_NAME = "BLOCK_TREE_TERMS_DICT";

  // Initial format
  public static final int VERSION_START = 0;

  public static final int VERSION_CURRENT = VERSION_START;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tim";
  static final String TERMS_INDEX_EXTENSION = "tip";

  protected final IndexOutput out;
  private final IndexOutput indexOut;
  final int minItemsInBlock;
  final int maxItemsInBlock;

  final BlockTreePostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;
  FieldInfo currentField;
  private final List<TermsWriter> fields = new ArrayList<TermsWriter>();
  private final String segment;

  // nocommit should take min block size?
  public BlockTreeTermsWriter(
                              SegmentWriteState state,
                              BlockTreePostingsWriterBase postingsWriter,
                              int minItemsInBlock,
                              int maxItemsInBlock)
    throws IOException
  {

    // nocommit -- make sure minItemsInBlock is > 1

    if (minItemsInBlock <= 0) {
      throw new IllegalArgumentException("minItemsInBlock must be >= 1; got " + minItemsInBlock);
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

    final String termsFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, TERMS_EXTENSION);
    out = state.directory.createOutput(termsFileName, state.context);
    boolean success = false;
    IndexOutput indexOut = null;
    try {
      fieldInfos = state.fieldInfos;
      this.minItemsInBlock = minItemsInBlock;
      this.maxItemsInBlock = maxItemsInBlock;
      writeHeader(out);

      //DEBUG = state.segmentName.equals("_4a");

      final String termsIndexFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, TERMS_INDEX_EXTENSION);
      indexOut = state.directory.createOutput(termsIndexFileName, state.context);
      writeIndexHeader(indexOut);

      currentField = null;
      this.postingsWriter = postingsWriter;
      segment = state.segmentName;

      // System.out.println("BTW.init seg=" + state.segmentName);

      postingsWriter.start(out);                          // have consumer write its format/header
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeSafely(true, out, indexOut);
      }
    }
    this.indexOut = indexOut;
  }
  
  protected void writeHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT); 
    out.writeLong(0);                             // leave space for end index pointer    
  }

  protected void writeIndexHeader(IndexOutput out) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT); 
    out.writeLong(0);                             // leave space for end index pointer    
  }

  protected void writeTrailer(long dirStart) throws IOException {
    out.seek(CodecUtil.headerLength(CODEC_NAME));
    out.writeLong(dirStart);    
  }

  protected void writeIndexTrailer(long dirStart) throws IOException {
    indexOut.seek(CodecUtil.headerLength(CODEC_NAME));
    indexOut.writeLong(dirStart);    
  }
  
  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    //DEBUG = field.name.equals("id");
    if (DEBUG2 || DEBUG) System.out.println("\nBTTW.addField seg=" + segment + " field=" + field.name);
    assert currentField == null || currentField.name.compareTo(field.name) < 0;
    currentField = field;
    final TermsWriter terms = new TermsWriter(field);
    fields.add(terms);
    return terms;
  }

  private static class PendingTerm {
    public final BytesRef term;
    public final TermStats stats;

    public PendingTerm(BytesRef term, TermStats stats) {
      this.term = term;
      this.stats = stats;
    }

    @Override
    public String toString() {
      return term.utf8ToString();
    }
  }

  static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
    // nocommit assert fp is "small enough"
    // nocommit use constants here instead of 1, 2:
    return (fp << 2) | (hasTerms ? 2 : 0) | (isFloor ? 1 : 0);
  }

  private static class PendingBlock {
    public final BytesRef prefix;
    public final long fp;
    public FST<BytesRef> index;
    public List<FST<BytesRef>> subIndices;
    public final boolean hasTerms;
    public final boolean isFloor;
    public final int floorLeadByte;

    public PendingBlock(BytesRef prefix, long fp, boolean hasTerms, boolean isFloor, int floorLeadByte, List<FST<BytesRef>> subIndices) {
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
      // nocommit -- vLong is bad for FST!!!  it writes LSB
      // first which means less byte[] prefix sharing I
      // think???  sheesh.
      scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));
      if (isFloor) {
        scratchBytes.writeVInt(floorBlocks.size());
        for (PendingBlock sub : floorBlocks) {
          assert sub.floorLeadByte != -1;
          if (DEBUG) {
            System.out.println("    write floorLeadByte=" + Integer.toHexString(sub.floorLeadByte&0xff));
          }
          scratchBytes.writeByte((byte) sub.floorLeadByte);
          assert sub.fp > fp;
          // nocommit -- why do we need hasTerms here?
          // nocommit -- need isFloor here?
          scratchBytes.writeVLong(((sub.fp - fp) << 1) | (sub.hasTerms ? 1 : 0));
        }
      }

      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final Builder<BytesRef> indexBuilder = new Builder<BytesRef>(FST.INPUT_TYPE.BYTE1,
                                                                   0, 0, true, true, Integer.MAX_VALUE,
                                                                   outputs, null);
      if (DEBUG) {
        System.out.println("  compile index for prefix=" + prefix);
      }
      indexBuilder.DEBUG = false;
      final byte[] bytes = new byte[(int) scratchBytes.getFilePointer()];
      assert bytes.length > 0;
      scratchBytes.writeTo(bytes, 0);
      indexBuilder.add(prefix, new BytesRef(bytes, 0, bytes.length));
      scratchBytes.reset();

      // Copy over index for all sub-blocks

      for(FST<BytesRef> subIndex : subIndices) {
        append(indexBuilder, subIndex);
      }

      if (floorBlocks != null) {
        for (PendingBlock sub : floorBlocks) {
          for(FST<BytesRef> subIndex : sub.subIndices) {
            append(indexBuilder, subIndex);
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
        if (DEBUG) {
          System.out.println("      add sub=" + indexEnt.input + " " + indexEnt.input + " output=" + indexEnt.output);
        }
        builder.add(indexEnt.input, indexEnt.output);
      }
    }
  }

  final RAMOutputStream scratchBytes = new RAMOutputStream();

  class TermsWriter extends TermsConsumer {
    private final FieldInfo fieldInfo;
    private final long termsStartPointer;
    private long numTerms;
    long sumTotalTermFreq;
    long sumDocFreq;
    long indexStartFP;

    // Used only to partition terms into the block tree; we
    // don't pull an FST from this builder:
    private final NoOutputs noOutputs;
    private final Builder<Object> blockBuilder;

    // PendingTerm or PendingBlock:
    private final List<Object> pending = new ArrayList<Object>();

    // This class assigns terms to blocks "naturally", ie,
    // according to the number of terms under a given prefix
    // that we encounter:
    private class FindBlocks extends Builder.FreezeTail<Object> {

      @Override
      public void freeze(final Builder.UnCompiledNode<Object>[] frontier, int prefixLenPlus1, final IntsRef lastInput) throws IOException {

        if (DEBUG) System.out.println("  freeze prefixLenPlus1=" + prefixLenPlus1);

        for(int idx=lastInput.length; idx >= prefixLenPlus1; idx--) {
          final Builder.UnCompiledNode<Object> node = frontier[idx];
          final Builder.UnCompiledNode<Object> parent = idx == 0 ? null : frontier[idx-1];

          long totCount = 0;

          if (node.isFinal) {
            totCount++;
          }

          //System.out.println("VISIT node=" + node + "
          //arcs=" + node.numArcs);
          for(int arcIdx=0;arcIdx<node.numArcs;arcIdx++) {
            @SuppressWarnings("unchecked") final Builder.UnCompiledNode<Object> target = (Builder.UnCompiledNode<Object>) node.arcs[arcIdx].target;
            totCount += target.inputCount;
            target.clear();
            node.arcs[arcIdx].target = null;
          }
          node.numArcs = 0;

          boolean forceBlock = false;

          // nocommit fixup

          if (idx == 1) {
            // nocommit -- make this 1 configurable -- maybe
            // 2 is better if there are many terms?

            // We force a block if prefix is length 1 and
            // there are any terms, so that the root block
            // doesn't have terms.

            // nocommit: instead, we should accum termCount &
            // blockCount into UnCompiledNode?
            for(int pendingIdx=0;pendingIdx<totCount && !forceBlock;pendingIdx++) {
              forceBlock |= pending.get(pending.size()-pendingIdx-1) instanceof PendingTerm;
            }
          }

          if (totCount >= minItemsInBlock || idx == 0 || forceBlock) {
            if (DEBUG2 || DEBUG) {
              if (totCount < minItemsInBlock && idx != 0) {
                System.out.println("  force block has terms");
              }
            }
            node.inputCount = writeBlocks(lastInput, idx, (int) totCount);
          } else {
            // stragglers!  carry count upwards
            node.inputCount = totCount;
          }
          frontier[idx] = new Builder.UnCompiledNode<Object>(blockBuilder, idx);
        }
      }
    }

    private int[] subBytes = new int[10];
    private int[] subTermCounts = new int[10];
    private int[] subTermCountSums = new int[10];
    private int[] subSubCounts = new int[10];

    // Write the top count entries on the pending stack as
    // one or more blocks.
    int writeBlocks(IntsRef prevTerm, int prefixLength, int count) throws IOException {
      if (prefixLength == 0 || count <= maxItemsInBlock) {
        // Not floor block
        final PendingBlock nonFloorBlock = writeBlock(prevTerm, prefixLength, prefixLength, count, count, 0, false, -1, true);
        nonFloorBlock.compileIndex(null, scratchBytes);
        pending.add(nonFloorBlock);
      } else {

        // nocommit -- we could enrich this format so that
        // we store min & max label for this block, then it
        // can be "authoritative"

        if (DEBUG) {
          final BytesRef prefix = new BytesRef(prefixLength);
          for(int m=0;m<prefixLength;m++) {
            prefix.bytes[m] = (byte) prevTerm.ints[m];
          }
          prefix.length = prefixLength;
          //System.out.println("\nWBS count=" + count + " prefix=" + prefix.utf8ToString() + " " + prefix);
          System.out.println("writeBlocks: prefix=" + prefix + " " + prefix + " count=" + count + " pending.size()=" + pending.size());
        }

        final int savLabel = prevTerm.ints[prevTerm.offset + prefixLength];

        // First pass: count up how many items fall under
        // each unique label after the prefix.
        
        // nocommit: this is wasteful since the builder had
        // already done this but we discarded it...
        
        final List<Object> slice = pending.subList(pending.size()-count, pending.size());
        int lastLabel = -1;
        int termCount = 0;
        int subCount = 0;
        int numSubs = 0;

        for(Object ent : slice) {
          final boolean isTerm = ent instanceof PendingTerm;
          final int label;
          if (isTerm) {
            PendingTerm term = (PendingTerm) ent;
            if (term.term.length == prefixLength) {
              assert lastLabel == -1;
              assert numSubs == 0;
              label = -1;
            } else {
              label = term.term.bytes[term.term.offset + prefixLength];
            }
          } else {
            PendingBlock block = (PendingBlock) ent;
            assert block.prefix.length > prefixLength;
            label = block.prefix.bytes[block.prefix.offset + prefixLength];
          }

          if (label != lastLabel && (termCount + subCount) != 0) {
            if (subBytes.length == numSubs) {
              subBytes = ArrayUtil.grow(subBytes);
              subTermCounts = ArrayUtil.grow(subTermCounts);
              subSubCounts = ArrayUtil.grow(subSubCounts);
            }
            subBytes[numSubs] = lastLabel;
            lastLabel = label;
            subTermCounts[numSubs] = termCount;
            subSubCounts[numSubs] = subCount;
            /*
            if (label == -1) {
              System.out.println("  sub " + -1 + " termCount=" + termCount + " subCount=" + subCount);
            } else {
              System.out.println("  sub " + Integer.toHexString(label) + " termCount=" + termCount + " subCount=" + subCount);
            }
            */
            termCount = subCount = 0;
            numSubs++;
          }

          if (isTerm) {
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

        subBytes[numSubs] = lastLabel;
        subTermCounts[numSubs] = termCount;
        subSubCounts[numSubs] = subCount;
        numSubs++;
        /*
        if (lastLabel == -1) {
          System.out.println("  sub " + -1 + " termCount=" + termCount + " subCount=" + subCount);
        } else {
          System.out.println("  sub " + Integer.toHexString(lastLabel) + " termCount=" + termCount + " subCount=" + subCount);
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

        // nocommit -- need viterbi search here?

        // Naive segmentation, not always best (it can produce
        // a too-small block as the last block):
        int pendingCount = 0;
        int startLabel = subBytes[0];
        int curStart = count;
        subCount = 0;

        final List<PendingBlock> floorBlocks = new ArrayList<PendingBlock>();
        PendingBlock firstBlock = null;

        for(int sub=0;sub<numSubs;sub++) {
          pendingCount += subTermCounts[sub] + subSubCounts[sub];
          subCount++;

          // greedily make a floor block as soon as we've
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
            //System.out.println("    " + curStart + " remain");
            pendingCount = 0;

            // nocommit -- not valid?  but if i change this
            // to allow the case where the sub did have
            // "many" floor'd sub-blocks somehow... then
            // it's valid?
            //assert subCount > 1: "subCount=" + subCount + " sub=" + sub + " of " + numSubs + " subTermCount=" + subTermCountSums[sub] + " subSubCount=" + subSubCounts[sub] + " depth=" + prefixLength;
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
              floorBlocks.add(writeBlock(prevTerm, prefixLength, prefixLength+1, curStart, curStart, 0, true, startLabel, true));
              break;
            }
          }
        }

        prevTerm.ints[prevTerm.offset + prefixLength] = savLabel;

        assert firstBlock != null;
        firstBlock.compileIndex(floorBlocks, scratchBytes);

        pending.add(firstBlock);
        if (DEBUG) System.out.println("  done pending.size()=" + pending.size());
      }

      return 1;
    }

    String brPrefixToString(BytesRef b) {
      // nocommit
      return b.toString();
      //return b.utf8ToString() + " " + b;
    }

    String brToString(BytesRef b) {
      // nocommit
      // return b.toString();
      return b.utf8ToString() + " " + b;
    }

    // TODO: we could block-write the term suffix pointers;
    // this would take more space but would enable binary
    // search on lookup
    private PendingBlock writeBlock(IntsRef prevTerm, int prefixLength, int indexPrefixLength, int start, int length, int futureTermCount, boolean isFloor, int floorLeadByte, boolean isLastInFloor) throws IOException {

      assert length > 0;

      final BytesRef prefix = new BytesRef(indexPrefixLength);
      for(int m=0;m<indexPrefixLength;m++) {
        prefix.bytes[m] = (byte) prevTerm.ints[m];
      }
      prefix.length = indexPrefixLength;

      /*if (isFloor) {
        System.out.println("  wb seg=" + segment + " prefix=" + prefix.utf8ToString() + " " + prefix + " field=" + fieldInfo.name + " prefix=" + prefixLength + " pending=" + pending.size() + " start=" + start + " length=" + length);
      } else {
        System.out.println("\nWB seg=" + segment + " prefix=" + prefix.utf8ToString() + " " + prefix + " field=" + fieldInfo.name + " prefix=" + prefixLength + " pending=" + pending.size() + " start=" + start + " length=" + length);
        }*/
      assert pending.size() >= start: "pending.size()=" + pending.size() + " start=" + start + " length=" + length;

      final List<Object> slice = pending.subList(pending.size()-start, pending.size()-start + length);

      final long startFP = out.getFilePointer();

      out.writeVInt((length<<1)|(isLastInFloor ? 1:0));

      if (DEBUG2 || DEBUG) {
        System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + " pending.size()=" + pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" + prefix + " entCount=" + length + " startFP=" + startFP + " futureTermCount=" + futureTermCount + (isFloor ? (" floorLeadByte=" + Integer.toHexString(floorLeadByte&0xff)) : "") + " isLastInFloor=" + isLastInFloor);
      }

      // 1st pass: pack term suffix bytes into byte[] blob
      // TODO: cutover to bulk int codec... simple64?
      int termCount = 0;

      final List<FST<BytesRef>> subIndices = new ArrayList<FST<BytesRef>>();

      boolean isLeafBlock = true;
      for (Object ent : slice) {
        if (ent instanceof PendingBlock) {
          isLeafBlock = false;
          break;
        }
      }

      for (Object ent : slice) {
        if (ent instanceof PendingTerm) {
          PendingTerm term = (PendingTerm) ent;
          // nocommit turn back on -- but need a separate
          //prefix (not indexPrefix, since that may be floor)
          //assert term.term.startsWith(prefix);
          final int suffix = term.term.length - prefixLength;
          if (DEBUG2 || DEBUG) {
            BytesRef suffixBytes = new BytesRef(suffix);
            System.arraycopy(term.term.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            suffixBytes.length = suffix;
            System.out.println("    write term suffix=" + suffixBytes);
          }
          if (isLeafBlock) {
            bytesWriter.writeVInt(suffix);
          } else {
            bytesWriter.writeVInt(suffix<<1);
          }
          bytesWriter.writeBytes(term.term.bytes, prefixLength, suffix);
          termCount++;
        } else {
          assert !isLeafBlock;
          PendingBlock block = (PendingBlock) ent;
          // nocommit turn back on -- but need a separate
          //prefix (not indexPrefix, since that may be floor)
          //assert block.prefix.startsWith(prefix);
          final int suffix = block.prefix.length - prefixLength;

          // nocommit: why does this trip?
          //assert suffix > 0;
          bytesWriter.writeVInt((suffix<<1)|1);
          bytesWriter.writeBytes(block.prefix.bytes, prefixLength, suffix);
          assert block.fp < startFP;

          if (DEBUG2 || DEBUG) {
            BytesRef suffixBytes = new BytesRef(suffix);
            System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            suffixBytes.length = suffix;
            System.out.println("    write sub-block suffix=" + brPrefixToString(suffixBytes) + " subFP=" + block.fp + " subCode=" + (startFP-block.fp) + " floor=" + block.isFloor);
          }

          bytesWriter.writeVLong(startFP - block.fp);
          subIndices.add(block.index);
        }
      }

      // Write suffix byte[] blob
      out.writeVInt((int) (bytesWriter.getFilePointer() << 1) | (isLeafBlock ? 1:0));
      bytesWriter.writeTo(out);
      bytesWriter.reset();

      // 2nd pass: write the TermStats as byte[] blob
      for(Object ent : slice) {
        if (ent instanceof PendingTerm) {
          PendingTerm term = (PendingTerm) ent;
          // nocommit -- turn back on, but need separate
          // prefix from indexPrefix because of floor
          //assert term.term.startsWith(prefix);
          bytesWriter.writeVInt(term.stats.docFreq);
          if (fieldInfo.indexOptions != IndexOptions.DOCS_ONLY) {
            assert term.stats.totalTermFreq >= term.stats.docFreq;
            bytesWriter.writeVLong(term.stats.totalTermFreq - term.stats.docFreq);
          }
          //if (DEBUG) System.out.println("    write dF=" + term.stats.docFreq + " totTF=" + term.stats.totalTermFreq);
        }
      }

      out.writeVInt((int) bytesWriter.getFilePointer());
      bytesWriter.writeTo(out);
      bytesWriter.reset();

      // 3rd pass: have postings writer write block
      // nocommit only if termCount != 0?
      postingsWriter.flushTermsBlock(futureTermCount+termCount, termCount);

      // Remove slice replaced by block:
      slice.clear();

      if (DEBUG) {
        System.out.println("      fpEnd=" + out.getFilePointer());
      }

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
                                         new FindBlocks());

      termsStartPointer = out.getFilePointer();
      postingsWriter.setField(fieldInfo);
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      //System.out.println("BTW.startTerm term=" + fieldInfo.name + ":" + text.utf8ToString() + " " + text + " seg=" + segment);
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
      if (DEBUG) System.out.println("BTTW.finishTerm term=" + fieldInfo.name + ":" + text.utf8ToString() + " " + text + " seg=" + segment + " df=" + stats.docFreq);

      blockBuilder.add(text, noOutputs.getNoOutput());
      pending.add(new PendingTerm(new BytesRef(text), stats));
      postingsWriter.finishTerm(stats);
      numTerms++;
    }

    // Finishes all terms in this field
    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq) throws IOException {
      // nocommit write sumDocFreq
      if (numTerms > 0) {
        blockBuilder.finish();

        // We better have one final "root" block:
        assert pending.size() == 1 && pending.get(0) instanceof PendingBlock: "pending.size()=" + pending.size() + " pending=" + pending;
        final PendingBlock root = (PendingBlock) pending.get(0);
        assert root.prefix.length == 0;
        assert root.index.getEmptyOutput() != null;

        this.sumTotalTermFreq = sumTotalTermFreq;
        this.sumDocFreq = sumDocFreq;

        // Write FST to index
        indexStartFP = indexOut.getFilePointer();
        root.index.save(indexOut);
        //System.out.println("  write FST " + indexStartFP + " field=" + fieldInfo.name);

        if (SAVE_DOT_FILES || DEBUG2 || DEBUG) {
          final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
          Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
          Util.toDot(root.index, w, false, false);
          System.out.println("SAVED to " + dotFileName);
          w.close();
        }
      }
    }

    private final RAMOutputStream bytesWriter = new RAMOutputStream();
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
          // nocommit: we may not need termsStartPointer?
          out.writeVLong(field.termsStartPointer);
          final BytesRef rootCode = ((PendingBlock) field.pending.get(0)).index.getEmptyOutput();
          assert rootCode != null: "field=" + field.fieldInfo.name + " numTerms=" + field.numTerms;
          out.writeVInt(rootCode.length);
          out.writeBytes(rootCode.bytes, rootCode.offset, rootCode.length);
          if (field.fieldInfo.indexOptions != IndexOptions.DOCS_ONLY) {
            out.writeVLong(field.sumTotalTermFreq);
          }
          out.writeVLong(field.sumDocFreq);
          indexOut.writeVLong(field.indexStartFP);
        }
      }
      writeTrailer(dirStart);
      writeIndexTrailer(indexDirStart);
    } catch (IOException ioe2) {
      ioe = ioe2;
    } finally {
      IOUtils.closeSafely(ioe, out, indexOut, postingsWriter);
    }
  }
}
