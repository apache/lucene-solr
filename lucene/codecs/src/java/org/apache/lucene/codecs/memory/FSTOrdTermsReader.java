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
package org.apache.lucene.codecs.memory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

/** 
 * FST-based terms dictionary reader.
 *
 * The FST index maps each term and its ord, and during seek 
 * the ord is used to fetch metadata from a single block.
 * The term dictionary is fully memory resident.
 *
 * @lucene.experimental
 */
public class FSTOrdTermsReader extends FieldsProducer {
  static final int INTERVAL = FSTOrdTermsWriter.SKIP_INTERVAL;
  final TreeMap<String, TermsReader> fields = new TreeMap<>();
  final PostingsReaderBase postingsReader;
  //static final boolean TEST = false;

  public FSTOrdTermsReader(SegmentReadState state, PostingsReaderBase postingsReader) throws IOException {
    final String termsIndexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, FSTOrdTermsWriter.TERMS_INDEX_EXTENSION);
    final String termsBlockFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, FSTOrdTermsWriter.TERMS_BLOCK_EXTENSION);

    this.postingsReader = postingsReader;
    ChecksumIndexInput indexIn = null;
    IndexInput blockIn = null;
    boolean success = false;
    try {
      indexIn = state.directory.openChecksumInput(termsIndexFileName, state.context);
      blockIn = state.directory.openInput(termsBlockFileName, state.context);
      int version = CodecUtil.checkIndexHeader(indexIn, FSTOrdTermsWriter.TERMS_INDEX_CODEC_NAME, 
                                                          FSTOrdTermsWriter.VERSION_START, 
                                                          FSTOrdTermsWriter.VERSION_CURRENT, 
                                                          state.segmentInfo.getId(), state.segmentSuffix);
      int version2 = CodecUtil.checkIndexHeader(blockIn, FSTOrdTermsWriter.TERMS_CODEC_NAME, 
                                                           FSTOrdTermsWriter.VERSION_START, 
                                                           FSTOrdTermsWriter.VERSION_CURRENT, 
                                                           state.segmentInfo.getId(), state.segmentSuffix);
      
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: index=" + version + ", terms=" + version2, blockIn);
      }

      CodecUtil.checksumEntireFile(blockIn);
      
      this.postingsReader.init(blockIn, state);
      seekDir(blockIn);

      final FieldInfos fieldInfos = state.fieldInfos;
      final int numFields = blockIn.readVInt();
      for (int i = 0; i < numFields; i++) {
        FieldInfo fieldInfo = fieldInfos.fieldInfo(blockIn.readVInt());
        boolean hasFreq = fieldInfo.getIndexOptions() != IndexOptions.DOCS;
        long numTerms = blockIn.readVLong();
        long sumTotalTermFreq = blockIn.readVLong();
        // if freqs are omitted, sumDocFreq=sumTotalTermFreq and we only write one value
        long sumDocFreq = hasFreq ? blockIn.readVLong() : sumTotalTermFreq;
        int docCount = blockIn.readVInt();
        int longsSize = blockIn.readVInt();
        FST<Long> index = new FST<>(indexIn, PositiveIntOutputs.getSingleton());

        TermsReader current = new TermsReader(fieldInfo, blockIn, numTerms, sumTotalTermFreq, sumDocFreq, docCount, longsSize, index);
        TermsReader previous = fields.put(fieldInfo.name, current);
        checkFieldSummary(state.segmentInfo, indexIn, blockIn, current, previous);
      }
      CodecUtil.checkFooter(indexIn);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(indexIn, blockIn);
      } else {
        IOUtils.closeWhileHandlingException(indexIn, blockIn);
      }
    }
  }

  private void seekDir(IndexInput in) throws IOException {
    in.seek(in.length() - CodecUtil.footerLength() - 8);
    in.seek(in.readLong());
  }
  private void checkFieldSummary(SegmentInfo info, IndexInput indexIn, IndexInput blockIn, TermsReader field, TermsReader previous) throws IOException {
    // #docs with field must be <= #docs
    if (field.docCount < 0 || field.docCount > info.maxDoc()) {
      throw new CorruptIndexException("invalid docCount: " + field.docCount + " maxDoc: " + info.maxDoc() + " (blockIn=" + blockIn + ")", indexIn);
    }
    // #postings must be >= #docs with field
    if (field.sumDocFreq < field.docCount) {
      throw new CorruptIndexException("invalid sumDocFreq: " + field.sumDocFreq + " docCount: " + field.docCount + " (blockIn=" + blockIn + ")", indexIn);
    }
    // #positions must be >= #postings
    if (field.sumTotalTermFreq < field.sumDocFreq) {
      throw new CorruptIndexException("invalid sumTotalTermFreq: " + field.sumTotalTermFreq + " sumDocFreq: " + field.sumDocFreq + " (blockIn=" + blockIn + ")", indexIn);
    }
    if (previous != null) {
      throw new CorruptIndexException("duplicate fields: " + field.fieldInfo.name + " (blockIn=" + blockIn + ")", indexIn);
    }
  }

  @Override
  public Iterator<String> iterator() {
    return Collections.unmodifiableSet(fields.keySet()).iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    assert field != null;
    return fields.get(field);
  }

  @Override
  public int size() {
    return fields.size();
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(postingsReader);
    } finally {
      fields.clear();
    }
  }

  final class TermsReader extends Terms implements Accountable {
    final FieldInfo fieldInfo;
    final long numTerms;
    final long sumTotalTermFreq;
    final long sumDocFreq;
    final int docCount;
    final int longsSize;
    final FST<Long> index;

    final int numSkipInfo;
    final long[] skipInfo;
    final byte[] statsBlock;
    final byte[] metaLongsBlock;
    final byte[] metaBytesBlock;

    TermsReader(FieldInfo fieldInfo, IndexInput blockIn, long numTerms, long sumTotalTermFreq, long sumDocFreq, int docCount, int longsSize, FST<Long> index) throws IOException {
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
      this.longsSize = longsSize;
      this.index = index;

      assert (numTerms & (~0xffffffffL)) == 0;
      final int numBlocks = (int)(numTerms + INTERVAL - 1) / INTERVAL;
      this.numSkipInfo = longsSize + 3;
      this.skipInfo = new long[numBlocks * numSkipInfo];
      this.statsBlock = new byte[(int)blockIn.readVLong()];
      this.metaLongsBlock = new byte[(int)blockIn.readVLong()];
      this.metaBytesBlock = new byte[(int)blockIn.readVLong()];

      int last = 0, next = 0;
      for (int i = 1; i < numBlocks; i++) {
        next = numSkipInfo * i;
        for (int j = 0; j < numSkipInfo; j++) {
          skipInfo[next + j] = skipInfo[last + j] + blockIn.readVLong();
        }
        last = next;
      }
      blockIn.readBytes(statsBlock, 0, statsBlock.length);
      blockIn.readBytes(metaLongsBlock, 0, metaLongsBlock.length);
      blockIn.readBytes(metaBytesBlock, 0, metaBytesBlock.length);
    }

    public boolean hasFreqs() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    @Override
    public boolean hasPositions() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
      return fieldInfo.hasPayloads();
    }

    @Override
    public long size() {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return sumDocFreq;
    }

    @Override
    public int getDocCount() throws IOException {
      return docCount;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
        throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
      }
      return new IntersectTermsEnum(compiled, startTerm);
    }

    @Override
    public long ramBytesUsed() {
      long ramBytesUsed = 0;
      if (index != null) {
        ramBytesUsed += index.ramBytesUsed();
        ramBytesUsed += RamUsageEstimator.sizeOf(metaBytesBlock);
        ramBytesUsed += RamUsageEstimator.sizeOf(metaLongsBlock);
        ramBytesUsed += RamUsageEstimator.sizeOf(skipInfo);
        ramBytesUsed += RamUsageEstimator.sizeOf(statsBlock);
      }
      return ramBytesUsed;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      if (index == null) {
        return Collections.emptyList();
      } else {
        return Collections.singletonList(Accountables.namedAccountable("terms", index));
      }
    }
    
    @Override
    public String toString() {
      return "FSTOrdTerms(terms=" + numTerms + ",postings=" + sumDocFreq + ",positions=" + sumTotalTermFreq + ",docs=" + docCount + ")";
    }

    // Only wraps common operations for PBF interact
    abstract class BaseTermsEnum extends org.apache.lucene.index.BaseTermsEnum {

      /* Current term's ord, starts from 0 */
      long ord;

      /* Current term stats + decoded metadata (customized by PBF) */
      final BlockTermState state;

      /* Datainput to load stats & metadata */
      final ByteArrayDataInput statsReader = new ByteArrayDataInput();
      final ByteArrayDataInput metaLongsReader = new ByteArrayDataInput();
      final ByteArrayDataInput metaBytesReader = new ByteArrayDataInput();

      /* To which block is buffered */ 
      int statsBlockOrd;
      int metaBlockOrd;

      /* Current buffered metadata (long[] & byte[]) */
      long[][] longs;
      int[] bytesStart;
      int[] bytesLength;

      /* Current buffered stats (df & ttf) */
      int[] docFreq;
      long[] totalTermFreq;

      BaseTermsEnum() throws IOException {
        this.state = postingsReader.newTermState();
        this.statsReader.reset(statsBlock);
        this.metaLongsReader.reset(metaLongsBlock);
        this.metaBytesReader.reset(metaBytesBlock);

        this.longs = new long[INTERVAL][longsSize];
        this.bytesStart = new int[INTERVAL];
        this.bytesLength = new int[INTERVAL];
        this.docFreq = new int[INTERVAL];
        this.totalTermFreq = new long[INTERVAL];
        this.statsBlockOrd = -1;
        this.metaBlockOrd = -1;
      }

      /** Decodes stats data into term state */
      void decodeStats() throws IOException {
        final int upto = (int)ord % INTERVAL;
        final int oldBlockOrd = statsBlockOrd;
        statsBlockOrd = (int)ord / INTERVAL;
        if (oldBlockOrd != statsBlockOrd) {
          refillStats();
        }
        state.docFreq = docFreq[upto];
        state.totalTermFreq = totalTermFreq[upto];
      }

      /** Let PBF decode metadata */
      void decodeMetaData() throws IOException {
        final int upto = (int)ord % INTERVAL;
        final int oldBlockOrd = metaBlockOrd;
        metaBlockOrd = (int)ord / INTERVAL;
        if (metaBlockOrd != oldBlockOrd) {
          refillMetadata();
        }
        metaBytesReader.setPosition(bytesStart[upto]);
        postingsReader.decodeTerm(longs[upto], metaBytesReader, fieldInfo, state, true);
      }

      /** Load current stats shard */
      final void refillStats() throws IOException {
        final int offset = statsBlockOrd * numSkipInfo;
        final int statsFP = (int)skipInfo[offset];
        statsReader.setPosition(statsFP);
        for (int i = 0; i < INTERVAL && !statsReader.eof(); i++) {
          int code = statsReader.readVInt();
          if (hasFreqs()) {
            docFreq[i] = (code >>> 1);
            if ((code & 1) == 1) {
              totalTermFreq[i] = docFreq[i];
            } else {
              totalTermFreq[i] = docFreq[i] + statsReader.readVLong();
            }
          } else {
            docFreq[i] = code;
            totalTermFreq[i] = code;
          }
        }
      }

      /** Load current metadata shard */
      final void refillMetadata() throws IOException {
        final int offset = metaBlockOrd * numSkipInfo;
        final int metaLongsFP = (int)skipInfo[offset + 1];
        final int metaBytesFP = (int)skipInfo[offset + 2];
        metaLongsReader.setPosition(metaLongsFP);
        for (int j = 0; j < longsSize; j++) {
          longs[0][j] = skipInfo[offset + 3 + j] + metaLongsReader.readVLong();
        }
        bytesStart[0] = metaBytesFP; 
        bytesLength[0] = (int)metaLongsReader.readVLong();
        for (int i = 1; i < INTERVAL && !metaLongsReader.eof(); i++) {
          for (int j = 0; j < longsSize; j++) {
            longs[i][j] = longs[i-1][j] + metaLongsReader.readVLong();
          }
          bytesStart[i] = bytesStart[i-1] + bytesLength[i-1];
          bytesLength[i] = (int)metaLongsReader.readVLong();
        }
      }

      @Override
      public TermState termState() throws IOException {
        decodeMetaData();
        return state.clone();
      }

      @Override
      public int docFreq() throws IOException {
        return state.docFreq;
      }

      @Override
      public long totalTermFreq() throws IOException {
        return state.totalTermFreq;
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        decodeMetaData();
        return postingsReader.postings(fieldInfo, state, reuse, flags);
      }

      @Override
      public ImpactsEnum impacts(int flags) throws IOException {
        decodeMetaData();
        return postingsReader.impacts(fieldInfo, state, flags);
      }

      // TODO: this can be achieved by making use of Util.getByOutput()
      //           and should have related tests
      @Override
      public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() {
        throw new UnsupportedOperationException();
      }
    }

    // Iterates through all terms in this field
    private final class SegmentTermsEnum extends BaseTermsEnum {
      final BytesRefFSTEnum<Long> fstEnum;
      /* Current term, null when enum ends or unpositioned */
      BytesRef term;

      /* True when current term's metadata is decoded */
      boolean decoded;

      /* True when current enum is 'positioned' by seekExact(TermState) */
      boolean seekPending;

      SegmentTermsEnum() throws IOException {
        this.fstEnum = new BytesRefFSTEnum<>(index);
        this.decoded = false;
        this.seekPending = false;
      }

      @Override
      public BytesRef term() throws IOException {
        return term;
      }

      @Override
      void decodeMetaData() throws IOException {
        if (!decoded && !seekPending) {
          super.decodeMetaData();
          decoded = true;
        }
      }

      // Update current enum according to FSTEnum
      void updateEnum(final InputOutput<Long> pair) throws IOException {
        if (pair == null) {
          term = null;
        } else {
          term = pair.input;
          ord = pair.output;
          decodeStats();
        }
        decoded = false;
        seekPending = false;
      }

      @Override
      public BytesRef next() throws IOException {
        if (seekPending) {  // previously positioned, but termOutputs not fetched
          seekPending = false;
          SeekStatus status = seekCeil(term);
          assert status == SeekStatus.FOUND;  // must positioned on valid term
        }
        updateEnum(fstEnum.next());
        return term;
      }

      @Override
      public boolean seekExact(BytesRef target) throws IOException {
        updateEnum(fstEnum.seekExact(target));
        return term != null;
      }

      @Override
      public SeekStatus seekCeil(BytesRef target) throws IOException {
        updateEnum(fstEnum.seekCeil(target));
        if (term == null) {
          return SeekStatus.END;
        } else {
          return term.equals(target) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
        }
      }

      @Override
      public void seekExact(BytesRef target, TermState otherState) {
        if (!target.equals(term)) {
          state.copyFrom(otherState);
          term = BytesRef.deepCopyOf(target);
          seekPending = true;
        }
      }
    }

    // Iterates intersect result with automaton (cannot seek!)
    private final class IntersectTermsEnum extends BaseTermsEnum {
      /* Current term, null when enum ends or unpositioned */
      BytesRefBuilder term;

      /* True when current term's metadata is decoded */
      boolean decoded;

      /* True when there is pending term when calling next() */
      boolean pending;

      /* stack to record how current term is constructed, 
       * used to accumulate metadata or rewind term:
       *   level == term.length + 1,
       *         == 0 when term is null */
      Frame[] stack;
      int level;

      /* term dict fst */
      final FST<Long> fst;
      final FST.BytesReader fstReader;
      final Outputs<Long> fstOutputs;

      /* query automaton to intersect with */
      final ByteRunAutomaton fsa;

      private final class Frame {
        /* fst stats */
        FST.Arc<Long> arc;

        Long output;

        /* automaton stats */
        int state;

        Frame() {
          this.arc = new FST.Arc<>();
          this.state = -1;
        }

        public String toString() {
          return "arc=" + arc + " state=" + state;
        }
      }

      IntersectTermsEnum(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
        //if (TEST) System.out.println("Enum init, startTerm=" + startTerm);
        this.fst = index;
        this.fstReader = fst.getBytesReader();
        this.fstOutputs = index.outputs;
        this.fsa = compiled.runAutomaton;
        this.level = -1;
        this.stack = new Frame[16];
        for (int i = 0 ; i < stack.length; i++) {
          this.stack[i] = new Frame();
        }

        Frame frame;
        frame = loadVirtualFrame(newFrame());
        this.level++;
        frame = loadFirstFrame(newFrame());
        pushFrame(frame);

        this.decoded = false;
        this.pending = false;

        if (startTerm == null) {
          pending = isAccept(topFrame());
        } else {
          doSeekCeil(startTerm);
          pending = (term == null || !startTerm.equals(term.get())) && isValid(topFrame()) && isAccept(topFrame());
        }
      }

      @Override
      public BytesRef term() throws IOException {
        return term == null ? null : term.get();
      }

      @Override
      void decodeMetaData() throws IOException {
        if (!decoded) {
          super.decodeMetaData();
          decoded = true;
        }
      }

      @Override
      void decodeStats() throws IOException {
        ord = topFrame().output;
        super.decodeStats();
      }

      @Override
      public SeekStatus seekCeil(BytesRef target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public BytesRef next() throws IOException {
        //if (TEST) System.out.println("Enum next()");
        if (pending) {
          pending = false;
          decodeStats();
          return term();
        }
        decoded = false;
      DFS:
        while (level > 0) {
          Frame frame = newFrame();
          if (loadExpandFrame(topFrame(), frame) != null) {  // has valid target
            pushFrame(frame);
            if (isAccept(frame)) {  // gotcha
              break;
            }
            continue;  // check next target
          } 
          frame = popFrame();
          while(level > 0) {
            if (loadNextFrame(topFrame(), frame) != null) {  // has valid sibling 
              pushFrame(frame);
              if (isAccept(frame)) {  // gotcha
                break DFS;
              }
              continue DFS;   // check next target 
            }
            frame = popFrame();
          }
          return null;
        }
        decodeStats();
        return term();
      }

      BytesRef doSeekCeil(BytesRef target) throws IOException {
        //if (TEST) System.out.println("Enum doSeekCeil()");
        Frame frame= null;
        int label, upto = 0, limit = target.length;
        while (upto < limit) {  // to target prefix, or ceil label (rewind prefix)
          frame = newFrame();
          label = target.bytes[upto] & 0xff;
          frame = loadCeilFrame(label, topFrame(), frame);
          if (frame == null || frame.arc.label() != label) {
            break;
          }
          assert isValid(frame);  // target must be fetched from automaton
          pushFrame(frame);
          upto++;
        }
        if (upto == limit) {  // got target
          return term();
        }
        if (frame != null) {  // got larger term('s prefix)
          pushFrame(frame);
          return isAccept(frame) ? term() : next();
        }
        while (level > 0) {   // got target's prefix, advance to larger term
          frame = popFrame();
          while (level > 0 && !canRewind(frame)) {
            frame = popFrame();
          }
          if (loadNextFrame(topFrame(), frame) != null) {
            pushFrame(frame);
            return isAccept(frame) ? term() : next();
          }
        }
        return null;
      }

      /** Virtual frame, never pop */
      Frame loadVirtualFrame(Frame frame) {
        frame.output = fstOutputs.getNoOutput();
        frame.state = -1;
        return frame;
      }

      /** Load frame for start arc(node) on fst */
      Frame loadFirstFrame(Frame frame) {
        frame.arc = fst.getFirstArc(frame.arc);
        frame.output = frame.arc.output();
        frame.state = 0;
        return frame;
      }

      /** Load frame for target arc(node) on fst */
      Frame loadExpandFrame(Frame top, Frame frame) throws IOException {
        if (!canGrow(top)) {
          return null;
        }
        frame.arc = fst.readFirstRealTargetArc(top.arc.target(), frame.arc, fstReader);
        frame.state = fsa.step(top.state, frame.arc.label());
        frame.output = frame.arc.output();
        //if (TEST) System.out.println(" loadExpand frame="+frame);
        if (frame.state == -1) {
          return loadNextFrame(top, frame);
        }
        return frame;
      }

      /** Load frame for sibling arc(node) on fst */
      Frame loadNextFrame(Frame top, Frame frame) throws IOException {
        if (!canRewind(frame)) {
          return null;
        }
        while (!frame.arc.isLast()) {
          frame.arc = fst.readNextRealArc(frame.arc, fstReader);
          frame.output = frame.arc.output();
          frame.state = fsa.step(top.state, frame.arc.label());
          if (frame.state != -1) {
            break;
          }
        }
        //if (TEST) System.out.println(" loadNext frame="+frame);
        if (frame.state == -1) {
          return null;
        }
        return frame;
      }

      /** Load frame for target arc(node) on fst, so that 
       *  arc.label &gt;= label and !fsa.reject(arc.label) */
      Frame loadCeilFrame(int label, Frame top, Frame frame) throws IOException {
        FST.Arc<Long> arc = frame.arc;
        arc = Util.readCeilArc(label, fst, top.arc, arc, fstReader);
        if (arc == null) {
          return null;
        }
        frame.state = fsa.step(top.state, arc.label());
        //if (TEST) System.out.println(" loadCeil frame="+frame);
        if (frame.state == -1) {
          return loadNextFrame(top, frame);
        }
        frame.output = arc.output();
        return frame;
      }

      boolean isAccept(Frame frame) {  // reach a term both fst&fsa accepts
        return fsa.isAccept(frame.state) && frame.arc.isFinal();
      }
      boolean isValid(Frame frame) {   // reach a prefix both fst&fsa won't reject
        return /*frame != null &&*/ frame.state != -1;
      }
      boolean canGrow(Frame frame) {   // can walk forward on both fst&fsa
        return frame.state != -1 && FST.targetHasArcs(frame.arc);
      }
      boolean canRewind(Frame frame) { // can jump to sibling
        return !frame.arc.isLast();
      }

      void pushFrame(Frame frame) {
        final FST.Arc<Long> arc = frame.arc;
        frame.output = fstOutputs.add(topFrame().output, frame.output);
        term = grow(arc.label());
        level++;
        assert frame == stack[level];
      }

      Frame popFrame() {
        term = shrink();
        return stack[level--];
      }

      Frame newFrame() {
        if (level+1 == stack.length) {
          final Frame[] temp = new Frame[ArrayUtil.oversize(level+2, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          System.arraycopy(stack, 0, temp, 0, stack.length);
          for (int i = stack.length; i < temp.length; i++) {
            temp[i] = new Frame();
          }
          stack = temp;
        }
        return stack[level+1];
      }

      Frame topFrame() {
        return stack[level];
      }

      BytesRefBuilder grow(int label) {
        if (term == null) {
          term = new BytesRefBuilder();
        } else {
          term.append((byte) label);
        }
        return term;
      }

      BytesRefBuilder shrink() {
        if (term.length() == 0) {
          term = null;
        } else {
          term.setLength(term.length() - 1);
        }
        return term;
      }
    }
  }

  static<T> void walk(FST<T> fst) throws IOException {
    final ArrayList<FST.Arc<T>> queue = new ArrayList<>();
    final BitSet seen = new BitSet();
    final FST.BytesReader reader = fst.getBytesReader();
    final FST.Arc<T> startArc = fst.getFirstArc(new FST.Arc<T>());
    queue.add(startArc);
    while (!queue.isEmpty()) {
      final FST.Arc<T> arc = queue.remove(0);
      final long node = arc.target();
      //System.out.println(arc);
      if (FST.targetHasArcs(arc) && !seen.get((int) node)) {
        seen.set((int) node);
        fst.readFirstRealTargetArc(node, arc, reader);
        while (true) {
          queue.add(new FST.Arc<T>().copyFrom(arc));
          if (arc.isLast()) {
            break;
          } else {
            fst.readNextRealArc(arc, reader);
          }
        }
      }
    }
  }
  
  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = postingsReader.ramBytesUsed();
    for (TermsReader r : fields.values()) {
      ramBytesUsed += r.ramBytesUsed();
    }
    return ramBytesUsed;
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>(Accountables.namedAccountables("field", fields));
    resources.add(Accountables.namedAccountable("delegate", postingsReader));
    return Collections.unmodifiableList(resources);
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + fields.size() + ",delegate=" + postingsReader + ")";
  }

  @Override
  public void checkIntegrity() throws IOException {
    postingsReader.checkIntegrity();
  }
}
