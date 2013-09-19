package org.apache.lucene.codecs.asserting;

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
import java.util.Comparator;
import java.util.Iterator;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PushFieldsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.index.AssertingAtomicReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

/**
 * Just like {@link Lucene41PostingsFormat} but with additional asserts.
 */
public final class AssertingPostingsFormat extends PostingsFormat {
  private final PostingsFormat in = new Lucene41PostingsFormat();
  
  public AssertingPostingsFormat() {
    super("Asserting");
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    FieldsConsumer fieldsConsumer = in.fieldsConsumer(state);
    if (fieldsConsumer instanceof PushFieldsConsumer) {
      return new AssertingPushFieldsConsumer(state, (PushFieldsConsumer) fieldsConsumer);
    } else {
      return new AssertingFieldsConsumer(state, fieldsConsumer);
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new AssertingFieldsProducer(in.fieldsProducer(state));
  }
  
  static class AssertingFieldsProducer extends FieldsProducer {
    private final FieldsProducer in;
    
    AssertingFieldsProducer(FieldsProducer in) {
      this.in = in;
    }
    
    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public Iterator<String> iterator() {
      Iterator<String> iterator = in.iterator();
      assert iterator != null;
      return iterator;
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = in.terms(field);
      return terms == null ? null : new AssertingAtomicReader.AssertingTerms(terms);
    }

    @Override
    public int size() {
      return in.size();
    }

    @Override
    public long ramBytesUsed() {
      return in.ramBytesUsed();
    }
  }

  static class AssertingPushFieldsConsumer extends PushFieldsConsumer {
    private final PushFieldsConsumer in;
    
    AssertingPushFieldsConsumer(SegmentWriteState writeState, PushFieldsConsumer in) {
      super(writeState);
      this.in = in;
    }
    
    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      TermsConsumer consumer = in.addField(field);
      assert consumer != null;
      return new AssertingTermsConsumer(consumer, field);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  static class AssertingFieldsConsumer extends FieldsConsumer {
    private final FieldsConsumer in;
    private final SegmentWriteState writeState;

    AssertingFieldsConsumer(SegmentWriteState writeState, FieldsConsumer in) {
      this.writeState = writeState;
      this.in = in;
    }
    
    @Override
    public void write(Fields fields) throws IOException {
      in.write(fields);

      // TODO: more asserts?  can we somehow run a
      // "limited" CheckIndex here???  Or ... can we improve
      // AssertingFieldsProducer and us it also to wrap the
      // incoming Fields here?
 
      String lastField = null;
      TermsEnum termsEnum = null;

      for(String field : fields) {

        FieldInfo fieldInfo = writeState.fieldInfos.fieldInfo(field);
        assert fieldInfo != null;
        assert lastField == null || lastField.compareTo(field) < 0;
        lastField = field;

        Terms terms = fields.terms(field);
        assert terms != null;

        termsEnum = terms.iterator(termsEnum);
        BytesRef lastTerm = null;
        DocsEnum docsEnum = null;
        DocsAndPositionsEnum posEnum = null;

        boolean hasFreqs = fieldInfo.getIndexOptions().compareTo(FieldInfo.IndexOptions.DOCS_AND_FREQS) >= 0;
        boolean hasPositions = fieldInfo.getIndexOptions().compareTo(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        boolean hasOffsets = fieldInfo.getIndexOptions().compareTo(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;

        assert hasPositions == terms.hasPositions();
        assert hasOffsets == terms.hasOffsets();

        while(true) {
          BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          assert lastTerm == null || lastTerm.compareTo(term) < 0;
          if (lastTerm == null) {
            lastTerm = BytesRef.deepCopyOf(term);
          } else {
            lastTerm.copyBytes(term);
          }

          if (hasPositions == false) {
            int flags = 0;
            if (hasFreqs) {
              flags = flags | DocsEnum.FLAG_FREQS;
            }
            docsEnum = termsEnum.docs(null, docsEnum, flags);
          } else {
            int flags = DocsAndPositionsEnum.FLAG_PAYLOADS;
            if (hasOffsets) {
              flags = flags | DocsAndPositionsEnum.FLAG_OFFSETS;
            }
            posEnum = termsEnum.docsAndPositions(null, posEnum, flags);
            docsEnum = posEnum;
          }

          int lastDocID = -1;

          while(true) {
            int docID = docsEnum.nextDoc();
            if (docID == DocsEnum.NO_MORE_DOCS) {
              break;
            }
            assert docID > lastDocID;
            lastDocID = docID;
            if (hasFreqs) {
              int freq = docsEnum.freq();
              assert freq > 0;

              if (hasPositions) {
                int lastPos = -1;
                int lastStartOffset = -1;
                for(int i=0;i<freq;i++) {
                  int pos = posEnum.nextPosition();
                  assert pos > lastPos;
                  lastPos = pos;

                  if (hasOffsets) {
                    int startOffset = posEnum.startOffset();
                    int endOffset = posEnum.endOffset();
                    assert endOffset > startOffset;
                    assert startOffset >= lastStartOffset;
                    lastStartOffset = startOffset;
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  
  static enum TermsConsumerState { INITIAL, START, FINISHED };
  static class AssertingTermsConsumer extends TermsConsumer {
    private final TermsConsumer in;
    private final FieldInfo fieldInfo;
    private BytesRef lastTerm = null;
    private TermsConsumerState state = TermsConsumerState.INITIAL;
    private AssertingPostingsConsumer lastPostingsConsumer = null;
    private long sumTotalTermFreq = 0;
    private long sumDocFreq = 0;
    private OpenBitSet visitedDocs = new OpenBitSet();
    private static final Comparator<BytesRef> termComp = BytesRef.getUTF8SortedAsUnicodeComparator();
    
    AssertingTermsConsumer(TermsConsumer in, FieldInfo fieldInfo) {
      this.in = in;
      this.fieldInfo = fieldInfo;
    }
    
    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      assert state == TermsConsumerState.INITIAL || state == TermsConsumerState.START && lastPostingsConsumer.docFreq == 0;
      state = TermsConsumerState.START;
      assert lastTerm == null || termComp.compare(text, lastTerm) > 0;
      lastTerm = BytesRef.deepCopyOf(text);
      return lastPostingsConsumer = new AssertingPostingsConsumer(in.startTerm(text), fieldInfo, visitedDocs);
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      assert state == TermsConsumerState.START;
      state = TermsConsumerState.INITIAL;
      assert text.equals(lastTerm);
      assert stats.docFreq > 0; // otherwise, this method should not be called.
      assert stats.docFreq == lastPostingsConsumer.docFreq;
      sumDocFreq += stats.docFreq;
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY) {
        assert stats.totalTermFreq == -1;
      } else {
        assert stats.totalTermFreq == lastPostingsConsumer.totalTermFreq;
        sumTotalTermFreq += stats.totalTermFreq;
      }
      in.finishTerm(text, stats);
    }

    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      assert state == TermsConsumerState.INITIAL || state == TermsConsumerState.START && lastPostingsConsumer.docFreq == 0;
      state = TermsConsumerState.FINISHED;
      assert docCount >= 0;
      assert docCount == visitedDocs.cardinality();
      assert sumDocFreq >= docCount;
      assert sumDocFreq == this.sumDocFreq;
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY) {
        assert sumTotalTermFreq == -1;
      } else {
        assert sumTotalTermFreq >= sumDocFreq;
        assert sumTotalTermFreq == this.sumTotalTermFreq;
      }
      in.finish(sumTotalTermFreq, sumDocFreq, docCount);
    }
  }
  
  static enum PostingsConsumerState { INITIAL, START };
  static class AssertingPostingsConsumer extends PostingsConsumer {
    private final PostingsConsumer in;
    private final FieldInfo fieldInfo;
    private final OpenBitSet visitedDocs;
    private PostingsConsumerState state = PostingsConsumerState.INITIAL;
    private int freq;
    private int positionCount;
    private int lastPosition = 0;
    private int lastStartOffset = 0;
    int docFreq = 0;
    long totalTermFreq = 0;
    
    AssertingPostingsConsumer(PostingsConsumer in, FieldInfo fieldInfo, OpenBitSet visitedDocs) {
      this.in = in;
      this.fieldInfo = fieldInfo;
      this.visitedDocs = visitedDocs;
    }

    @Override
    public void startDoc(int docID, int freq) throws IOException {
      assert state == PostingsConsumerState.INITIAL;
      state = PostingsConsumerState.START;
      assert docID >= 0;
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY) {
        assert freq == -1;
        this.freq = 0; // we don't expect any positions here
      } else {
        assert freq > 0;
        this.freq = freq;
        totalTermFreq += freq;
      }
      this.positionCount = 0;
      this.lastPosition = 0;
      this.lastStartOffset = 0;
      docFreq++;
      visitedDocs.set(docID);
      in.startDoc(docID, freq);
    }

    @Override
    public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
      assert state == PostingsConsumerState.START;
      assert positionCount < freq;
      positionCount++;
      assert position >= lastPosition || position == -1; /* we still allow -1 from old 3.x indexes */
      lastPosition = position;
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
        assert startOffset >= 0;
        assert startOffset >= lastStartOffset;
        lastStartOffset = startOffset;
        assert endOffset >= startOffset;
      } else {
        assert startOffset == -1;
        assert endOffset == -1;
      }
      if (payload != null) {
        assert fieldInfo.hasPayloads();
      }
      in.addPosition(position, payload, startOffset, endOffset);
    }

    @Override
    public void finishDoc() throws IOException {
      assert state == PostingsConsumerState.START;
      state = PostingsConsumerState.INITIAL;
      if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
        assert positionCount == 0; // we should not have fed any positions!
      } else {
        assert positionCount == freq;
      }
      in.finishDoc();
    }
  }
}
