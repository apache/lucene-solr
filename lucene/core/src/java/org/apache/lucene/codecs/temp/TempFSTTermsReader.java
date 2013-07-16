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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.TempPostingsReaderBase;
import org.apache.lucene.codecs.CodecUtil;

public class TempFSTTermsReader extends FieldsProducer {
  final TreeMap<String, TermsReader> fields = new TreeMap<String, TermsReader>();
  final TempPostingsReaderBase postingsReader;
  final IndexInput in;
  boolean DEBUG = false;
  //String tmpname;

  public TempFSTTermsReader(SegmentReadState state, TempPostingsReaderBase postingsReader) throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TempFSTTermsWriter.TERMS_EXTENSION);
    //tmpname = termsFileName;

    this.postingsReader = postingsReader;
    this.in = state.directory.openInput(termsFileName, state.context);

    boolean success = false;
    try {
      readHeader(in);
      this.postingsReader.init(in);
      seekDir(in);

      final FieldInfos fieldInfos = state.fieldInfos;
      final int numFields = in.readVInt();
      for (int i = 0; i < numFields; i++) {
        int fieldNumber = in.readVInt();
        FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
        long numTerms = in.readVLong(); 
        long sumTotalTermFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY ? -1 : in.readVLong();
        long sumDocFreq = in.readVLong();
        int docCount = in.readVInt();
        int longsSize = in.readVInt();
        TermsReader current = new TermsReader(fieldInfo, numTerms, sumTotalTermFreq, sumDocFreq, docCount, longsSize);
        TermsReader previous = fields.put(fieldInfo.name, current);
        checkFieldSummary(state.segmentInfo, current, previous);
      }
      success = true;
    } finally {
      if (!success) {
        in.close();
      }
    }
  }

  private int readHeader(IndexInput in) throws IOException {
    return CodecUtil.checkHeader(in, 
                                 TempFSTTermsWriter.TERMS_CODEC_NAME,
                                 TempFSTTermsWriter.TERMS_VERSION_START,
                                 TempFSTTermsWriter.TERMS_VERSION_CURRENT);
  }
  private void seekDir(IndexInput in) throws IOException {
    in.seek(in.length() - 8);
    in.seek(in.readLong());
  }
  private void checkFieldSummary(SegmentInfo info, TermsReader field, TermsReader previous) throws IOException {
    // #docs with field must be <= #docs
    if (field.docCount < 0 || field.docCount > info.getDocCount()) { 
      throw new CorruptIndexException("invalid docCount: " + field.docCount + " maxDoc: " + info.getDocCount() + " (resource=" + in + ")");
    }
    // #postings must be >= #docs with field
    if (field.sumDocFreq < field.docCount) {  
      throw new CorruptIndexException("invalid sumDocFreq: " + field.sumDocFreq + " docCount: " + field.docCount + " (resource=" + in + ")");
    }
    // #positions must be >= #postings
    if (field.sumTotalTermFreq != -1 && field.sumTotalTermFreq < field.sumDocFreq) { 
      throw new CorruptIndexException("invalid sumTotalTermFreq: " + field.sumTotalTermFreq + " sumDocFreq: " + field.sumDocFreq + " (resource=" + in + ")");
    }
    if (previous != null) {
      throw new CorruptIndexException("duplicate fields: " + field.fieldInfo.name + " (resource=" + in + ")");
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
      IOUtils.close(in, postingsReader);
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
    final FST<TempTermOutputs.TempMetaData> dict;

    TermsReader(FieldInfo fieldInfo, long numTerms, long sumTotalTermFreq, long sumDocFreq, int docCount, int longsSize) throws IOException {
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
      this.longsSize = longsSize;
      this.dict = new FST<TempTermOutputs.TempMetaData>(in, new TempTermOutputs(fieldInfo, longsSize));
      //PrintWriter pw = new PrintWriter(new File("ohohoh."+tmpname+".xxx.txt"));
      //Util.toDot(dict, pw, false, false);
      //pw.close();
    }

    // nocommit: implement intersect
    // nocommit: why do we need this comparator overridden again and again?
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return new SegmentTermsEnum();
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

    // Iterates through terms in this field
    final class SegmentTermsEnum extends TermsEnum {
      final BytesRefFSTEnum<TempTermOutputs.TempMetaData> fstEnum;

      /* Current term, null when enum ends or unpositioned */
      BytesRef term;

      /* Current term stats + decoded metadata (customized by PBF) */
      final TempTermState state;

      /* Current term stats + undecoded metadata (long[] & byte[]) */
      TempTermOutputs.TempMetaData meta;
      ByteArrayDataInput bytesReader;

      /* True when current term's metadata is decoded */
      boolean decoded;

      /* True when current enum is 'positioned' by seekExact(TermState) */
      boolean seekPending;

      SegmentTermsEnum() throws IOException {
        this.fstEnum = new BytesRefFSTEnum<TempTermOutputs.TempMetaData>(dict);
        this.state = postingsReader.newTermState();
        this.bytesReader = new ByteArrayDataInput();
        this.term = null;
        this.decoded = false;
        this.seekPending = false;
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

      // Let PBF decodes metadata from long[] and byte[]
      private void decodeMetaData() throws IOException {
        if (!decoded && !seekPending) {
          if (meta.bytes != null) {
            bytesReader.reset(meta.bytes, 0, meta.bytes.length);
          }
          postingsReader.decodeTerm(meta.longs, bytesReader, fieldInfo, state);
          decoded = true;
        }
      }

      // Update current enum according to FSTEnum
      private void updateEnum(final InputOutput<TempTermOutputs.TempMetaData> pair) {
        if (pair == null) {
          term = null;
        } else {
          term = pair.input;
          meta = pair.output;
          state.docFreq = meta.docFreq;
          state.totalTermFreq = meta.totalTermFreq;
        }
        decoded = false;
        seekPending = false;
      }

      @Override
      public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
        decodeMetaData();
        return postingsReader.docs(fieldInfo, state, liveDocs, reuse, flags);
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
        if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
          return null;
        }
        decodeMetaData();
        return postingsReader.docsAndPositions(fieldInfo, state, liveDocs, reuse, flags);
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
      public boolean seekExact(final BytesRef target, final boolean useCache) throws IOException {
        updateEnum(fstEnum.seekExact(target));
        return term != null;
      }

      @Override
      public SeekStatus seekCeil(final BytesRef target, final boolean useCache) throws IOException {
        updateEnum(fstEnum.seekCeil(target));
        if (term == null) {
          return SeekStatus.END;
        } else {
          return term.equals(target) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
        }
      }

      // nocommit: this method doesn't act as 'seekExact' right?
      @Override
      public void seekExact(BytesRef target, TermState otherState) {
        if (term == null || target.compareTo(term) != 0) {
          state.copyFrom(otherState);
          term = BytesRef.deepCopyOf(target);
          seekPending = true;
        }
      }

      // nocommit: do we need this?
      @Override
      public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() {
        throw new UnsupportedOperationException();
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
        //seen.set((int) node);
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
