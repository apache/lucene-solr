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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.AssertingAtomicReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;

/**
 * Just like {@link Lucene40PostingsFormat} but with additional asserts.
 */
public class AssertingPostingsFormat extends PostingsFormat {
  private final PostingsFormat in = new Lucene40PostingsFormat();
  
  public AssertingPostingsFormat() {
    super("Asserting");
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new AssertingFieldsConsumer(in.fieldsConsumer(state));
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
    public FieldsEnum iterator() throws IOException {
      FieldsEnum iterator = in.iterator();
      assert iterator != null;
      return new AssertingAtomicReader.AssertingFieldsEnum(iterator);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = in.terms(field);
      return terms == null ? null : new AssertingAtomicReader.AssertingTerms(terms);
    }

    @Override
    public int size() throws IOException {
      return in.size();
    }

    @Override
    public long getUniqueTermCount() throws IOException {
      return in.getUniqueTermCount();
    }
  }
  
  static class AssertingFieldsConsumer extends FieldsConsumer {
    private final FieldsConsumer in;
    
    AssertingFieldsConsumer(FieldsConsumer in) {
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
  
  static enum TermsConsumerState { INITIAL, START, FINISHED };
  static class AssertingTermsConsumer extends TermsConsumer {
    private final TermsConsumer in;
    private final FieldInfo fieldInfo;
    private BytesRef lastTerm = null;
    private TermsConsumerState state = TermsConsumerState.INITIAL;
    
    AssertingTermsConsumer(TermsConsumer in, FieldInfo fieldInfo) {
      this.in = in;
      this.fieldInfo = fieldInfo;
    }
    
    // TODO: AssertingPostingsConsumer
    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      // TODO: assert that if state == START (no finishTerm called), that no actual docs were fed.
      // TODO: this makes the api really confusing! we should try to clean this up!
      assert state == TermsConsumerState.INITIAL || state == TermsConsumerState.START;
      state = TermsConsumerState.START;
      assert lastTerm == null || in.getComparator().compare(text, lastTerm) > 0;
      lastTerm = BytesRef.deepCopyOf(text);
      return in.startTerm(text);
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      assert state == TermsConsumerState.START;
      state = TermsConsumerState.INITIAL;
      assert text.equals(lastTerm);
      assert stats.docFreq > 0; // otherwise, this method should not be called.
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY) {
        assert stats.totalTermFreq == -1;
      }
      in.finishTerm(text, stats);
    }

    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      // TODO: assert that if state == START (no finishTerm called), that no actual docs were fed.
      // TODO: this makes the api really confusing! we should try to clean this up!
      assert state == TermsConsumerState.INITIAL || state == TermsConsumerState.START;
      state = TermsConsumerState.FINISHED;
      assert docCount >= 0;
      assert sumDocFreq >= docCount;
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY) {
        assert sumTotalTermFreq == -1;
      } else {
        assert sumTotalTermFreq >= sumDocFreq;        
      }
      in.finish(sumTotalTermFreq, sumDocFreq, docCount);
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return in.getComparator();
    }
  }
}
