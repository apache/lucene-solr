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
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RunAutomaton;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.TempPostingsReaderBase;
import org.apache.lucene.codecs.CodecUtil;


public class TempFSTTermsReader extends FieldsProducer {
  final TempPostingsReaderBase postingsReader;
  final IndexInput in;
  final TreeMap<String, FieldReader> fields = new TreeMap<String, FieldReader>();


  public TempFSTTermsReader(SegmentReadState state, TempPostingsReaderBase postingsReader) throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TempFSTTermsWriter.TERMS_EXTENSION);

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
        FieldReader current = new FieldReader(fieldInfo, numTerms, sumTotalTermFreq, sumDocFreq, docCount, longsSize);
        FieldReader previous = fields.put(fieldInfo.name, current);
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
    return CodecUtil.checkHeader(in, TempFSTTermsWriter.TERMS_CODEC_NAME,
                                 TempFSTTermsWriter.TERMS_VERSION_START,
                                 TempFSTTermsWriter.TERMS_VERSION_CURRENT);
  }
  private void seekDir(IndexInput in) throws IOException {
    in.seek(in.length() - 8);
    in.seek(in.readLong());
  }
  private void checkFieldSummary(SegmentInfo info, FieldReader field, FieldReader previous) throws IOException {
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

  final class FieldReader extends Terms {
    final FieldInfo fieldInfo;
    final long numTerms;
    final long sumTotalTermFreq;
    final long sumDocFreq;
    final int docCount;
    final int longsSize;
    final FST<TempTermOutputs.TempMetaData> dict;

    FieldReader(FieldInfo fieldInfo, long numTerms, long sumTotalTermFreq, long sumDocFreq, int docCount, int longsSize) throws IOException {
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
      this.longsSize = longsSize;
      this.dict = new FST<TempTermOutputs.TempMetaData>(in, new TempTermOutputs(longsSize));
      //PrintWriter pw = new PrintWriter(new File("../temp/xxx.txt"));
      //Util.toDot(dict, pw, false, false);
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
    private final class SegmentTermsEnum extends TermsEnum {
      SegmentTermsEnum() {
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public SeekStatus seekCeil(final BytesRef target, final boolean useCache) throws IOException {
        return null;
      }

      @Override
      public BytesRef next() throws IOException {
        return null;
      }

      @Override
      public BytesRef term() {
        return null;
      }
      
      @Override
      public int docFreq() throws IOException {
        return 0;
      }

      @Override
      public long totalTermFreq() throws IOException {
        return 0;
      }

      @Override
      public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
        return null;
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
        return null;
      }

      @Override
      public void seekExact(BytesRef target, TermState otherState) {
      }

      @Override
      public TermState termState() throws IOException {
        return null;
      }

      @Override
      public void seekExact(long ord) throws IOException {
      }

      @Override
      public long ord() {
        return 0;
      }
    }
  }
}
