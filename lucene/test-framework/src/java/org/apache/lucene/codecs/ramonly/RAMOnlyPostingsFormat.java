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
package org.apache.lucene.codecs.ramonly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/** Stores all postings data in RAM, but writes a small
 *  token (header + single int) to identify which "slot" the
 *  index is using in RAM HashMap.
 *
 *  NOTE: this codec sorts terms by reverse-unicode-order! */

public final class RAMOnlyPostingsFormat extends PostingsFormat {

  public RAMOnlyPostingsFormat() {
    super("RAMOnly");
  }
    
  // Postings state:
  static class RAMPostings extends FieldsProducer {
    final Map<String,RAMField> fieldToTerms = new TreeMap<>();

    @Override
    public Terms terms(String field) {
      return fieldToTerms.get(field);
    }

    @Override
    public int size() {
      return fieldToTerms.size();
    }

    @Override
    public Iterator<String> iterator() {
      return Collections.unmodifiableSet(fieldToTerms.keySet()).iterator();
    }

    @Override
    public void close() {
    }

    @Override
    public long ramBytesUsed() {
      long sizeInBytes = 0;
      for(RAMField field : fieldToTerms.values()) {
        sizeInBytes += field.ramBytesUsed();
      }
      return sizeInBytes;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      return Accountables.namedAccountables("field", fieldToTerms);
    }

    @Override
    public void checkIntegrity() throws IOException {}
  } 

  static class RAMField extends Terms implements Accountable {
    final String field;
    final SortedMap<String,RAMTerm> termToDocs = new TreeMap<>();
    long sumTotalTermFreq;
    long sumDocFreq;
    int docCount;
    final FieldInfo info;

    RAMField(String field, FieldInfo info) {
      this.field = field;
      this.info = info;
    }

    @Override
    public long ramBytesUsed() {
      long sizeInBytes = 0;
      for(RAMTerm term : termToDocs.values()) {
        sizeInBytes += term.ramBytesUsed();
      }
      return sizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      return Collections.emptyList();
    }

    @Override
    public long size() {
      return termToDocs.size();
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
    public TermsEnum iterator() {
      return new RAMTermsEnum(RAMOnlyPostingsFormat.RAMField.this);
    }

    @Override
    public boolean hasFreqs() {
      return info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
      return info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    @Override
    public boolean hasPositions() {
      return info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }
    
    @Override
    public boolean hasPayloads() {
      return info.hasPayloads();
    }
  }

  static class RAMTerm implements Accountable {
    final String term;
    long totalTermFreq;
    final List<RAMDoc> docs = new ArrayList<>();
    public RAMTerm(String term) {
      this.term = term;
    }

    @Override
    public long ramBytesUsed() {
      long sizeInBytes = 0;
      for(RAMDoc rDoc : docs) {
        sizeInBytes += rDoc.ramBytesUsed();
      }
      return sizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      return Collections.emptyList();
    }
  }

  static class RAMDoc implements Accountable {
    final int docID;
    final int[] positions;
    byte[][] payloads;

    public RAMDoc(int docID, int freq) {
      this.docID = docID;
      positions = new int[freq];
    }

    @Override
    public long ramBytesUsed() {
      long sizeInBytes = 0;
      sizeInBytes +=  (positions!=null) ? RamUsageEstimator.sizeOf(positions) : 0;
      
      if (payloads != null) {
        for(byte[] payload: payloads) {
          sizeInBytes += (payload!=null) ? RamUsageEstimator.sizeOf(payload) : 0;
        }
      }
      return sizeInBytes;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      return Collections.emptyList();
    }
  }

  // Classes for writing to the postings state
  private static class RAMFieldsConsumer extends FieldsConsumer {

    private final RAMPostings postings;
    private final RAMTermsConsumer termsConsumer = new RAMTermsConsumer();
    private final SegmentWriteState state;

    public RAMFieldsConsumer(SegmentWriteState writeState, RAMPostings postings) {
      this.postings = postings;
      this.state = writeState;
    }

    @Override
    public void write(Fields fields) throws IOException {
      for(String field : fields) {

        Terms terms = fields.terms(field);
        if (terms == null) {
          continue;
        }

        TermsEnum termsEnum = terms.iterator();

        FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
          throw new UnsupportedOperationException("this codec cannot index offsets");
        }

        RAMField ramField = new RAMField(field, fieldInfo);
        postings.fieldToTerms.put(field, ramField);
        termsConsumer.reset(ramField);

        FixedBitSet docsSeen = new FixedBitSet(state.segmentInfo.maxDoc());
        long sumTotalTermFreq = 0;
        long sumDocFreq = 0;
        PostingsEnum postingsEnum = null;
        int enumFlags;

        IndexOptions indexOptions = fieldInfo.getIndexOptions();
        boolean writeFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
        boolean writePositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        boolean writeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;        
        boolean writePayloads = fieldInfo.hasPayloads();

        if (writeFreqs == false) {
          enumFlags = 0;
        } else if (writePositions == false) {
          enumFlags = PostingsEnum.FREQS;
        } else if (writeOffsets == false) {
          if (writePayloads) {
            enumFlags = PostingsEnum.PAYLOADS;
          } else {
            enumFlags = 0;
          }
        } else {
          if (writePayloads) {
            enumFlags = PostingsEnum.PAYLOADS | PostingsEnum.OFFSETS;
          } else {
            enumFlags = PostingsEnum.OFFSETS;
          }
        }

        while (true) {
          BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          RAMPostingsWriterImpl postingsWriter = termsConsumer.startTerm(term);
          postingsEnum = termsEnum.postings(postingsEnum, enumFlags);

          int docFreq = 0;
          long totalTermFreq = 0;
          while (true) {
            int docID = postingsEnum.nextDoc();
            if (docID == PostingsEnum.NO_MORE_DOCS) {
              break;
            }
            docsSeen.set(docID);
            docFreq++;

            int freq;
            if (writeFreqs) {
              freq = postingsEnum.freq();
              totalTermFreq += freq;
            } else {
              freq = -1;
            }

            postingsWriter.startDoc(docID, freq);
            if (writePositions) {
              for (int i=0;i<freq;i++) {
                int pos = postingsEnum.nextPosition();
                BytesRef payload = writePayloads ? postingsEnum.getPayload() : null;
                int startOffset;
                int endOffset;
                if (writeOffsets) {
                  startOffset = postingsEnum.startOffset();
                  endOffset = postingsEnum.endOffset();
                } else {
                  startOffset = -1;
                  endOffset = -1;
                }
                postingsWriter.addPosition(pos, payload, startOffset, endOffset);
              }
            }

            postingsWriter.finishDoc();
          }
          termsConsumer.finishTerm(term, new TermStats(docFreq, totalTermFreq));
          sumDocFreq += docFreq;
          sumTotalTermFreq += totalTermFreq;
        }

        termsConsumer.finish(sumTotalTermFreq, sumDocFreq, docsSeen.cardinality());
      }
    }

    @Override
    public void close() throws IOException {
    }
  }

  private static class RAMTermsConsumer {
    private RAMField field;
    private final RAMPostingsWriterImpl postingsWriter = new RAMPostingsWriterImpl();
    RAMTerm current;
      
    void reset(RAMField field) {
      this.field = field;
    }
      
    public RAMPostingsWriterImpl startTerm(BytesRef text) {
      final String term = text.utf8ToString();
      current = new RAMTerm(term);
      postingsWriter.reset(current);
      return postingsWriter;
    }

    public void finishTerm(BytesRef text, TermStats stats) {
      assert stats.docFreq > 0;
      assert stats.docFreq == current.docs.size();
      current.totalTermFreq = stats.totalTermFreq;
      field.termToDocs.put(current.term, current);
    }

    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) {
      field.sumTotalTermFreq = sumTotalTermFreq;
      field.sumDocFreq = sumDocFreq;
      field.docCount = docCount;
    }
  }

  static class RAMPostingsWriterImpl {
    private RAMTerm term;
    private RAMDoc current;
    private int posUpto = 0;

    public void reset(RAMTerm term) {
      this.term = term;
    }

    public void startDoc(int docID, int freq) {
      current = new RAMDoc(docID, freq);
      term.docs.add(current);
      posUpto = 0;
    }

    public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) {
      assert startOffset == -1;
      assert endOffset == -1;
      current.positions[posUpto] = position;
      if (payload != null && payload.length > 0) {
        if (current.payloads == null) {
          current.payloads = new byte[current.positions.length][];
        }
        byte[] bytes = current.payloads[posUpto] = new byte[payload.length];
        System.arraycopy(payload.bytes, payload.offset, bytes, 0, payload.length);
      }
      posUpto++;
    }

    public void finishDoc() {
      assert posUpto == current.positions.length;
    }
  }

  static class RAMTermsEnum extends TermsEnum {
    Iterator<String> it;
    String current;
    private final RAMField ramField;

    public RAMTermsEnum(RAMField field) {
      this.ramField = field;
    }
      
    @Override
    public BytesRef next() {
      if (it == null) {
        if (current == null) {
          it = ramField.termToDocs.keySet().iterator();
        } else {
          it = ramField.termToDocs.tailMap(current).keySet().iterator();
        }
      }
      if (it.hasNext()) {
        current = it.next();
        return new BytesRef(current);
      } else {
        return null;
      }
    }

    @Override
    public SeekStatus seekCeil(BytesRef term) {
      current = term.utf8ToString();
      it = null;
      if (ramField.termToDocs.containsKey(current)) {
        return SeekStatus.FOUND;
      } else {
        if (current.compareTo(ramField.termToDocs.lastKey()) > 0) {
          return SeekStatus.END;
        } else {
          return SeekStatus.NOT_FOUND;
        }
      }
    }

    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef term() {
      // TODO: reuse BytesRef
      return new BytesRef(current);
    }

    @Override
    public int docFreq() {
      return ramField.termToDocs.get(current).docs.size();
    }

    @Override
    public long totalTermFreq() {
      return ramField.termToDocs.get(current).totalTermFreq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) {
      return new RAMDocsEnum(ramField.termToDocs.get(current));
    }

  }

  private static class RAMDocsEnum extends PostingsEnum {
    private final RAMTerm ramTerm;
    private RAMDoc current;
    int upto = -1;
    int posUpto = 0;

    public RAMDocsEnum(RAMTerm ramTerm) {
      this.ramTerm = ramTerm;
    }

    @Override
    public int advance(int targetDocID) throws IOException {
      return slowAdvance(targetDocID);
    }

    // TODO: override bulk read, for better perf
    @Override
    public int nextDoc() {
      upto++;
      if (upto < ramTerm.docs.size()) {
        current = ramTerm.docs.get(upto);
        posUpto = 0;
        return current.docID;
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() throws IOException {
      return current.positions.length;
    }

    @Override
    public int docID() {
      return current.docID;
    }

    @Override
    public int nextPosition() {
      assert posUpto < current.positions.length;
      return current.positions[posUpto++];
    }

    @Override
    public int startOffset() {
      return -1;
    }

    @Override
    public int endOffset() {
      return -1;
    }

    @Override
    public BytesRef getPayload() {
      if (current.payloads != null && current.payloads[posUpto-1] != null) {
        return new BytesRef(current.payloads[posUpto-1]);
      } else {
        return null;
      }
    }
    
    @Override
    public long cost() {
      return ramTerm.docs.size();
    } 
  }

  // Holds all indexes created, keyed by the ID assigned in fieldsConsumer
  private final Map<Integer,RAMPostings> state = new HashMap<>();

  private final AtomicInteger nextID = new AtomicInteger();

  private final String RAM_ONLY_NAME = "RAMOnly";
  private final static int VERSION_START = 0;
  private final static int VERSION_LATEST = VERSION_START;

  private static final String ID_EXTENSION = "id";

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState writeState) throws IOException {
    final int id = nextID.getAndIncrement();

    // TODO -- ok to do this up front instead of
    // on close....?  should be ok?
    // Write our ID:
    final String idFileName = IndexFileNames.segmentFileName(writeState.segmentInfo.name, writeState.segmentSuffix, ID_EXTENSION);
    IndexOutput out = writeState.directory.createOutput(idFileName, writeState.context);
    boolean success = false;
    try {
      CodecUtil.writeHeader(out, RAM_ONLY_NAME, VERSION_LATEST);
      out.writeVInt(id);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      } else {
        IOUtils.close(out);
      }
    }
    
    final RAMPostings postings = new RAMPostings();
    final RAMFieldsConsumer consumer = new RAMFieldsConsumer(writeState, postings);

    synchronized(state) {
      state.put(id, postings);
    }
    return consumer;
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState readState)
    throws IOException {

    // Load our ID:
    final String idFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, ID_EXTENSION);
    IndexInput in = readState.directory.openInput(idFileName, readState.context);
    boolean success = false;
    final int id;
    try {
      CodecUtil.checkHeader(in, RAM_ONLY_NAME, VERSION_START, VERSION_LATEST);
      id = in.readVInt();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(in);
      } else {
        IOUtils.close(in);
      }
    }
    
    synchronized(state) {
      return state.get(id);
    }
  }
}
