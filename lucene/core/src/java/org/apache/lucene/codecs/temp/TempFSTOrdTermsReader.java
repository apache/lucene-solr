package org.apache.lucene.codecs.temp;

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
import java.io.PrintWriter;
import java.io.File;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.TempPostingsReaderBase;
import org.apache.lucene.codecs.CodecUtil;

public class TempFSTOrdTermsReader extends FieldsProducer {
  static final int INTERVAL = TempFSTOrdTermsWriter.SKIP_INTERVAL;
  final TreeMap<String, TermsReader> fields = new TreeMap<String, TermsReader>();
  final TempPostingsReaderBase postingsReader;
  IndexInput indexIn = null;
  IndexInput blockIn = null;
  static final boolean TEST = false;

  public TempFSTOrdTermsReader(SegmentReadState state, TempPostingsReaderBase postingsReader) throws IOException {
    final String termsIndexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TempFSTOrdTermsWriter.TERMS_INDEX_EXTENSION);
    final String termsBlockFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TempFSTOrdTermsWriter.TERMS_BLOCK_EXTENSION);

    this.postingsReader = postingsReader;
    try {
      this.indexIn = state.directory.openInput(termsIndexFileName, state.context);
      this.blockIn = state.directory.openInput(termsBlockFileName, state.context);
      readHeader(indexIn);
      readHeader(blockIn);
      this.postingsReader.init(blockIn);
      seekDir(indexIn);
      seekDir(blockIn);

      final FieldInfos fieldInfos = state.fieldInfos;
      final int numFields = blockIn.readVInt();
      for (int i = 0; i < numFields; i++) {
        FieldInfo fieldInfo = fieldInfos.fieldInfo(blockIn.readVInt());
        boolean hasFreq = fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY;
        long numTerms = blockIn.readVLong();
        long sumTotalTermFreq = hasFreq ? blockIn.readVLong() : -1;
        long sumDocFreq = blockIn.readVLong();
        int docCount = blockIn.readVInt();
        int longsSize = blockIn.readVInt();
        FST<Long> index = new FST<Long>(indexIn, PositiveIntOutputs.getSingleton());

        TermsReader current = new TermsReader(fieldInfo, numTerms, sumTotalTermFreq, sumDocFreq, docCount, longsSize, index);
        TermsReader previous = fields.put(fieldInfo.name, current);
        checkFieldSummary(state.segmentInfo, current, previous);
      }
    } finally {
      IOUtils.close(indexIn, blockIn);
    }
  }

  private int readHeader(IndexInput in) throws IOException {
    return CodecUtil.checkHeader(in, TempFSTOrdTermsWriter.TERMS_CODEC_NAME,
                                     TempFSTOrdTermsWriter.TERMS_VERSION_START,
                                     TempFSTOrdTermsWriter.TERMS_VERSION_CURRENT);
  }
  private void seekDir(IndexInput in) throws IOException {
    in.seek(in.length() - 8);
    in.seek(in.readLong());
  }
  private void checkFieldSummary(SegmentInfo info, TermsReader field, TermsReader previous) throws IOException {
    // #docs with field must be <= #docs
    if (field.docCount < 0 || field.docCount > info.getDocCount()) {
      throw new CorruptIndexException("invalid docCount: " + field.docCount + " maxDoc: " + info.getDocCount() + " (resource=" + indexIn + ", " + blockIn + ")");
    }
    // #postings must be >= #docs with field
    if (field.sumDocFreq < field.docCount) {
      throw new CorruptIndexException("invalid sumDocFreq: " + field.sumDocFreq + " docCount: " + field.docCount + " (resource=" + indexIn + ", " + blockIn + ")");
    }
    // #positions must be >= #postings
    if (field.sumTotalTermFreq != -1 && field.sumTotalTermFreq < field.sumDocFreq) {
      throw new CorruptIndexException("invalid sumTotalTermFreq: " + field.sumTotalTermFreq + " sumDocFreq: " + field.sumDocFreq + " (resource=" + indexIn + ", " + blockIn + ")");
    }
    if (previous != null) {
      throw new CorruptIndexException("duplicate fields: " + field.fieldInfo.name + " (resource=" + indexIn + ", " + blockIn + ")");
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

  final class TermsReader extends Terms {
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

    TermsReader(FieldInfo fieldInfo, long numTerms, long sumTotalTermFreq, long sumDocFreq, int docCount, int longsSize, FST<Long> index) throws IOException {
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

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
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
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return super.intersect(compiled, startTerm);
    }

    // Only wraps common operations for PBF interact
    abstract class BaseTermsEnum extends TermsEnum {
      /* Current term, null when enum ends or unpositioned */
      BytesRef term;

      /* Current term's ord, starts from 0 */
      long ord;

      /* Current term stats + decoded metadata (customized by PBF) */
      final TempTermState state;

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
        this.term = null;
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
        metaBytesReader.reset(metaBytesBlock, bytesStart[upto], bytesLength[upto]);
        postingsReader.decodeTerm(longs[upto], metaBytesReader, fieldInfo, state);
      }

      /** Load current stats shard */
      final void refillStats() throws IOException {
        final int offset = statsBlockOrd * numSkipInfo;
        final int statsFP = (int)skipInfo[offset];
        statsReader.setPosition(statsFP);
        if (!hasFreqs()) {
          Arrays.fill(totalTermFreq, -1);
        }
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
          }
        }
      }

      /** Load current metadata shard */
      final void refillMetadata() throws IOException {
        final int offset = metaBlockOrd * numSkipInfo;
        final int metaLongsFP = (int)skipInfo[offset + 1];
        final int metaBytesFP = (int)skipInfo[offset + 2];
        metaLongsReader.setPosition(metaLongsFP);
        bytesStart[0] = metaBytesFP; 
        for (int j = 0; j < longsSize; j++) {
          longs[0][j] = skipInfo[offset + 3 + j] + metaLongsReader.readVLong();
        }
        bytesLength[0] = (int)metaLongsReader.readVLong();
        for (int i = 1; i < INTERVAL && !metaLongsReader.eof(); i++) {
          bytesStart[i] = bytesStart[i-1] + bytesLength[i-1];
          for (int j = 0; j < longsSize; j++) {
            longs[i][j] = longs[i-1][j] + metaLongsReader.readVLong();
          }
          bytesLength[i] = (int)metaLongsReader.readVLong();
        }
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public TermState termState() throws IOException {
        decodeMetaData();
        return state.clone();
      }

      @Override
      public BytesRef term() {
        return term;
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
      public long ord() {
        return ord;
      }

      @Override
      public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
        decodeMetaData();
        return postingsReader.docs(fieldInfo, state, liveDocs, reuse, flags);
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
        if (!hasPositions()) {
          return null;
        }
        decodeMetaData();
        return postingsReader.docsAndPositions(fieldInfo, state, liveDocs, reuse, flags);
      }

      // nocommit: this can be achieved by making use of Util.getByOutput()
      //           and should have related tests
      @Override
      public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
      }
    }


    // Iterates through all terms in this field
    private final class SegmentTermsEnum extends BaseTermsEnum {
      final BytesRefFSTEnum<Long> fstEnum;

      /* True when current term's metadata is decoded */
      boolean decoded;

      /* True when current enum is 'positioned' by seekExact(TermState) */
      boolean seekPending;

      SegmentTermsEnum() throws IOException {
        this.fstEnum = new BytesRefFSTEnum<Long>(index);
        this.decoded = false;
        this.seekPending = false;
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
          SeekStatus status = seekCeil(term, false);
          assert status == SeekStatus.FOUND;  // must positioned on valid term
        }
        updateEnum(fstEnum.next());
        return term;
      }

      @Override
      public boolean seekExact(BytesRef target, boolean useCache) throws IOException {
        updateEnum(fstEnum.seekExact(target));
        return term != null;
      }

      @Override
      public SeekStatus seekCeil(BytesRef target, boolean useCache) throws IOException {
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
  }

  static<T> void walk(FST<T> fst) throws IOException {
    final ArrayList<FST.Arc<T>> queue = new ArrayList<FST.Arc<T>>();
    final BitSet seen = new BitSet();
    final FST.BytesReader reader = fst.getBytesReader();
    final FST.Arc<T> startArc = fst.getFirstArc(new FST.Arc<T>());
    queue.add(startArc);
    while (!queue.isEmpty()) {
      final FST.Arc<T> arc = queue.remove(0);
      final long node = arc.target;
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
}
