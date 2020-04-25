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
package org.apache.lucene.codecs.asserting;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.AssertingLeafReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.TestUtil;

/**
 * Just like the default postings format but with additional asserts.
 */
public final class AssertingPostingsFormat extends PostingsFormat {
  private final PostingsFormat in = TestUtil.getDefaultPostingsFormat();
  
  public AssertingPostingsFormat() {
    super("Asserting");
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new AssertingFieldsConsumer(state, in.fieldsConsumer(state));
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new AssertingFieldsProducer(in.fieldsProducer(state));
  }
  
  static class AssertingFieldsProducer extends FieldsProducer {
    private final FieldsProducer in;
    
    AssertingFieldsProducer(FieldsProducer in) {
      this.in = in;
      // do a few simple checks on init
      assert toString() != null;
      assert ramBytesUsed() >= 0;
      assert getChildResources() != null;
    }
    
    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
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
      return terms == null ? null : new AssertingLeafReader.AssertingTerms(terms);
    }

    @Override
    public int size() {
      return in.size();
    }

    @Override
    public long ramBytesUsed() {
      long v = in.ramBytesUsed();
      assert v >= 0;
      return v;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      Collection<Accountable> res = in.getChildResources();
      TestUtil.checkReadOnly(res);
      return res;
    }

    @Override
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }
    
    @Override
    public FieldsProducer getMergeInstance() {
      return new AssertingFieldsProducer(in.getMergeInstance());
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
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
    public void write(Fields fields, NormsProducer norms) throws IOException {
      in.write(fields, norms);

      // TODO: more asserts?  can we somehow run a
      // "limited" CheckIndex here???  Or ... can we improve
      // AssertingFieldsProducer and us it also to wrap the
      // incoming Fields here?
 
      String lastField = null;

      for(String field : fields) {

        FieldInfo fieldInfo = writeState.fieldInfos.fieldInfo(field);
        assert fieldInfo != null;
        assert lastField == null || lastField.compareTo(field) < 0;
        lastField = field;

        Terms terms = fields.terms(field);
        if (terms == null) {
          continue;
        }
        assert terms != null;

        TermsEnum termsEnum = terms.iterator();
        BytesRefBuilder lastTerm = null;
        PostingsEnum postingsEnum = null;

        boolean hasFreqs = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
        boolean hasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        boolean hasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        boolean hasPayloads = terms.hasPayloads();

        assert hasPositions == terms.hasPositions();
        assert hasOffsets == terms.hasOffsets();

        while(true) {
          BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          assert lastTerm == null || lastTerm.get().compareTo(term) < 0;
          if (lastTerm == null) {
            lastTerm = new BytesRefBuilder();
            lastTerm.append(term);
          } else {
            lastTerm.copyBytes(term);
          }

          int flags = 0;
          if (hasPositions == false) {
            if (hasFreqs) {
              flags = flags | PostingsEnum.FREQS;
            }
            postingsEnum = termsEnum.postings(postingsEnum, flags);
          } else {
            flags = PostingsEnum.POSITIONS;
            if (hasPayloads) {
              flags |= PostingsEnum.PAYLOADS;
            }
            if (hasOffsets) {
              flags = flags | PostingsEnum.OFFSETS;
            }
            postingsEnum = termsEnum.postings(postingsEnum, flags);
          }

          assert postingsEnum != null : "termsEnum=" + termsEnum + " hasPositions=" + hasPositions;

          int lastDocID = -1;

          while(true) {
            int docID = postingsEnum.nextDoc();
            if (docID == PostingsEnum.NO_MORE_DOCS) {
              break;
            }
            assert docID > lastDocID;
            lastDocID = docID;
            if (hasFreqs) {
              int freq = postingsEnum.freq();
              assert freq > 0;

              if (hasPositions) {
                int lastPos = -1;
                int lastStartOffset = -1;
                for(int i=0;i<freq;i++) {
                  int pos = postingsEnum.nextPosition();
                  assert pos >= lastPos: "pos=" + pos + " vs lastPos=" + lastPos + " i=" + i + " freq=" + freq;
                  assert pos <= IndexWriter.MAX_POSITION: "pos=" + pos + " is > IndexWriter.MAX_POSITION=" + IndexWriter.MAX_POSITION;
                  lastPos = pos;

                  if (hasOffsets) {
                    int startOffset = postingsEnum.startOffset();
                    int endOffset = postingsEnum.endOffset();
                    assert endOffset >= startOffset;
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

    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }
  }
}
