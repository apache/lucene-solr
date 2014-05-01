package org.apache.lucene.codecs.cranky;

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
import java.util.Random;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

class CrankyPostingsFormat extends PostingsFormat {
  final PostingsFormat delegate;
  final Random random;
  
  CrankyPostingsFormat(PostingsFormat delegate, Random random) {
    // we impersonate the passed-in codec, so we don't need to be in SPI,
    // and so we dont change file formats
    super(delegate.getName());
    this.delegate = delegate;
    this.random = random;
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from PostingsFormat.fieldsConsumer()");
    }  
    return new CrankyFieldsConsumer(delegate.fieldsConsumer(state), random);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return delegate.fieldsProducer(state);
  }
  
  static class CrankyFieldsConsumer extends FieldsConsumer {
    final FieldsConsumer delegate;
    final Random random;
    
    CrankyFieldsConsumer(FieldsConsumer delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }
    
    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from FieldsConsumer.addField()");
      }
      return new CrankyTermsConsumer(delegate.addField(field), random);
    }

    @Override
    public void merge(MergeState mergeState, Fields fields) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from FieldsConsumer.merge()");
      }  
      super.merge(mergeState, fields);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from FieldsConsumer.close()");
      }  
    }
  }
  
  static class CrankyTermsConsumer extends TermsConsumer {
    final TermsConsumer delegate;
    final Random random;
    
    CrankyTermsConsumer(TermsConsumer delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }

    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermsConsumer.startTerm()");
      }  
      return new CrankyPostingsConsumer(delegate.startTerm(text), random);
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from TermsConsumer.finishTerm()");
      }
      delegate.finishTerm(text, stats);
    }

    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from TermsConsumer.finish()");
      }
      delegate.finish(sumTotalTermFreq, sumDocFreq, docCount);
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from TermsConsumer.getComparator()");
      }
      return delegate.getComparator();
    }

    @Override
    public void merge(MergeState mergeState, IndexOptions indexOptions, TermsEnum termsEnum) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from TermsConsumer.merge()");
      }
      super.merge(mergeState, indexOptions, termsEnum);
    }
  }
  
  static class CrankyPostingsConsumer extends PostingsConsumer {
    final PostingsConsumer delegate;
    final Random random;
    
    CrankyPostingsConsumer(PostingsConsumer delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }

    @Override
    public void startDoc(int docID, int freq) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from PostingsConsumer.startDoc()");
      }
      delegate.startDoc(docID, freq);
    }

    @Override
    public void finishDoc() throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from PostingsConsumer.finishDoc()");
      }
      delegate.finishDoc();
    }
    
    @Override
    public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from PostingsConsumer.addPosition()");
      }
      delegate.addPosition(position, payload, startOffset, endOffset);
    }

    @Override
    public TermStats merge(MergeState mergeState, IndexOptions indexOptions, DocsEnum postings, FixedBitSet visitedDocs) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from PostingsConsumer.merge()");
      }
      return super.merge(mergeState, indexOptions, postings, visitedDocs);
    }
  }
}
