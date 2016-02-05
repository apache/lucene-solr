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
package org.apache.lucene.codecs.bloom;

import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.TestUtil;

/**
 * A class used for testing {@link BloomFilteringPostingsFormat} with a concrete
 * delegate (Lucene41). Creates a Bloom filter on ALL fields and with tiny
 * amounts of memory reserved for the filter. DO NOT USE IN A PRODUCTION
 * APPLICATION This is not a realistic application of Bloom Filters as they
 * ordinarily are larger and operate on only primary key type fields.
 */
public final class TestBloomFilteredLucenePostings extends PostingsFormat {
  
  private BloomFilteringPostingsFormat delegate;
  
  // Special class used to avoid OOM exceptions where Junit tests create many
  // fields.
  static class LowMemoryBloomFactory extends BloomFilterFactory {
    @Override
    public FuzzySet getSetForField(SegmentWriteState state,FieldInfo info) {
      return FuzzySet.createSetBasedOnMaxMemory(1024);
    }
    
    @Override
    public boolean isSaturated(FuzzySet bloomFilter, FieldInfo fieldInfo) {
      // For test purposes always maintain the BloomFilter - even past the point
      // of usefulness when all bits are set
      return false;
    }
  }
  
  public TestBloomFilteredLucenePostings() {
    super("TestBloomFilteredLucenePostings");
    delegate = new BloomFilteringPostingsFormat(TestUtil.getDefaultPostingsFormat(),
        new LowMemoryBloomFactory());
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return delegate.fieldsConsumer(state);
  }
  
  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return delegate.fieldsProducer(state);
  }

  @Override
  public String toString() {
    return "TestBloomFilteredLucenePostings(" + delegate + ")";
  }
}
