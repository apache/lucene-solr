package org.apache.lucene.codecs.bloom;

/**
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.codecs.bloom.FuzzySet.ContainsResult;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * <p>
 * A {@link PostingsFormat} useful for low doc-frequency fields such as primary
 * keys. Bloom filters are maintained in a ".blm" file which offers "fast-fail"
 * for reads in segments known to have no record of the key. A choice of
 * delegate PostingsFormat is used to record all other Postings data.
 * </p>
 * <p>
 * A choice of {@link BloomFilterFactory} can be passed to tailor Bloom Filter
 * settings on a per-field basis. The default configuration is
 * {@link DefaultBloomFilterFactory} which allocates a ~8mb bitset and hashes
 * values using {@link MurmurHash2}. This should be suitable for most purposes.
 * </p>
 * <p>
 * The format of the blm file is as follows:
 * </p>
 * <ul>
 * <li>BloomFilter (.blm) --&gt; Header, DelegatePostingsFormatName,
 * NumFilteredFields, Filter<sup>NumFilteredFields</sup></li>
 * <li>Filter --&gt; FieldNumber, FuzzySet</li>
 * <li>FuzzySet --&gt;See {@link FuzzySet#serialize(DataOutput)}</li>
 * <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * <li>DelegatePostingsFormatName --&gt; {@link DataOutput#writeString(String)
 * String} The name of a ServiceProvider registered {@link PostingsFormat}</li>
 * <li>NumFilteredFields --&gt; {@link DataOutput#writeInt Uint32}</li>
 * <li>FieldNumber --&gt; {@link DataOutput#writeInt Uint32} The number of the
 * field in this segment</li>
 * </ul>
 * @lucene.experimental
 */
public final class BloomFilteringPostingsFormat extends PostingsFormat {
  
  public static final String BLOOM_CODEC_NAME = "BloomFilter";
  public static final int BLOOM_CODEC_VERSION = 1;
  
  /** Extension of Bloom Filters file */
  static final String BLOOM_EXTENSION = "blm";
  
  BloomFilterFactory bloomFilterFactory = new DefaultBloomFilterFactory();
  private PostingsFormat delegatePostingsFormat;
  
  /**
   * Creates Bloom filters for a selection of fields created in the index. This
   * is recorded as a set of Bitsets held as a segment summary in an additional
   * "blm" file. This PostingsFormat delegates to a choice of delegate
   * PostingsFormat for encoding all other postings data.
   * 
   * @param delegatePostingsFormat
   *          The PostingsFormat that records all the non-bloom filter data i.e.
   *          postings info.
   * @param bloomFilterFactory
   *          The {@link BloomFilterFactory} responsible for sizing BloomFilters
   *          appropriately
   */
  public BloomFilteringPostingsFormat(PostingsFormat delegatePostingsFormat,
      BloomFilterFactory bloomFilterFactory) {
    super(BLOOM_CODEC_NAME);
    this.delegatePostingsFormat = delegatePostingsFormat;
    this.bloomFilterFactory = bloomFilterFactory;
  }
  
  /**
   * Creates Bloom filters for a selection of fields created in the index. This
   * is recorded as a set of Bitsets held as a segment summary in an additional
   * "blm" file. This PostingsFormat delegates to a choice of delegate
   * PostingsFormat for encoding all other postings data. This choice of
   * constructor defaults to the {@link DefaultBloomFilterFactory} for
   * configuring per-field BloomFilters.
   * 
   * @param delegatePostingsFormat
   *          The PostingsFormat that records all the non-bloom filter data i.e.
   *          postings info.
   */
  public BloomFilteringPostingsFormat(PostingsFormat delegatePostingsFormat) {
    this(delegatePostingsFormat, new DefaultBloomFilterFactory());
  }
  
  // Used only by core Lucene at read-time via Service Provider instantiation -
  // do not use at Write-time in application code.
  public BloomFilteringPostingsFormat() {
    super(BLOOM_CODEC_NAME);
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    if (delegatePostingsFormat == null) {
      throw new UnsupportedOperationException("Error - " + getClass().getName()
          + " has been constructed without a choice of PostingsFormat");
    }
    return new BloomFilteredFieldsConsumer(
        delegatePostingsFormat.fieldsConsumer(state), state,
        delegatePostingsFormat);
  }
  
  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return new BloomFilteredFieldsProducer(state);
  }
  
  public class BloomFilteredFieldsProducer extends FieldsProducer {
    private FieldsProducer delegateFieldsProducer;
    HashMap<String,FuzzySet> bloomsByFieldName = new HashMap<String,FuzzySet>();
    
    public BloomFilteredFieldsProducer(SegmentReadState state)
        throws IOException {
      
      String bloomFileName = IndexFileNames.segmentFileName(
          state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
      IndexInput bloomIn = null;
      boolean success = false;
      try {
        bloomIn = state.directory.openInput(bloomFileName, state.context);
        CodecUtil.checkHeader(bloomIn, BLOOM_CODEC_NAME, BLOOM_CODEC_VERSION,
            BLOOM_CODEC_VERSION);
        // // Load the hash function used in the BloomFilter
        // hashFunction = HashFunction.forName(bloomIn.readString());
        // Load the delegate postings format
        PostingsFormat delegatePostingsFormat = PostingsFormat.forName(bloomIn
            .readString());
        
        this.delegateFieldsProducer = delegatePostingsFormat
            .fieldsProducer(state);
        int numBlooms = bloomIn.readInt();
        for (int i = 0; i < numBlooms; i++) {
          int fieldNum = bloomIn.readInt();
          FuzzySet bloom = FuzzySet.deserialize(bloomIn);
          FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
          bloomsByFieldName.put(fieldInfo.name, bloom);
        }
        IOUtils.close(bloomIn);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(bloomIn, delegateFieldsProducer);
        }
      }
    }
    
    @Override
    public Iterator<String> iterator() {
      return delegateFieldsProducer.iterator();
    }
    
    @Override
    public void close() throws IOException {
      delegateFieldsProducer.close();
    }
    
    @Override
    public Terms terms(String field) throws IOException {
      FuzzySet filter = bloomsByFieldName.get(field);
      if (filter == null) {
        return delegateFieldsProducer.terms(field);
      } else {
        Terms result = delegateFieldsProducer.terms(field);
        if (result == null) {
          return null;
        }
        return new BloomFilteredTerms(result, filter);
      }
    }
    
    @Override
    public int size() {
      return delegateFieldsProducer.size();
    }
    
    public long getUniqueTermCount() throws IOException {
      return delegateFieldsProducer.getUniqueTermCount();
    }
    
    class BloomFilteredTerms extends Terms {
      private Terms delegateTerms;
      private FuzzySet filter;
      
      public BloomFilteredTerms(Terms terms, FuzzySet filter) {
        this.delegateTerms = terms;
        this.filter = filter;
      }
      
      @Override
      public TermsEnum intersect(CompiledAutomaton compiled,
          final BytesRef startTerm) throws IOException {
        return delegateTerms.intersect(compiled, startTerm);
      }
      
      @Override
      public TermsEnum iterator(TermsEnum reuse) throws IOException {
        if ((reuse != null) && (reuse instanceof BloomFilteredTermsEnum)) {
          // recycle the existing BloomFilteredTermsEnum by asking the delegate
          // to recycle its contained TermsEnum
          BloomFilteredTermsEnum bfte = (BloomFilteredTermsEnum) reuse;
          if (bfte.filter == filter) {
            bfte.reset(delegateTerms, bfte.delegateTermsEnum);
            return bfte;
          }
        }
        // We have been handed something we cannot reuse (either null, wrong
        // class or wrong filter) so allocate a new object
        return new BloomFilteredTermsEnum(delegateTerms, reuse, filter);
      }
      
      @Override
      public Comparator<BytesRef> getComparator() {
        return delegateTerms.getComparator();
      }
      
      @Override
      public long size() throws IOException {
        return delegateTerms.size();
      }
      
      @Override
      public long getSumTotalTermFreq() throws IOException {
        return delegateTerms.getSumTotalTermFreq();
      }
      
      @Override
      public long getSumDocFreq() throws IOException {
        return delegateTerms.getSumDocFreq();
      }
      
      @Override
      public int getDocCount() throws IOException {
        return delegateTerms.getDocCount();
      }

      @Override
      public boolean hasOffsets() {
        return delegateTerms.hasOffsets();
      }

      @Override
      public boolean hasPositions() {
        return delegateTerms.hasPositions();
      }
      
      @Override
      public boolean hasPayloads() {
        return delegateTerms.hasPayloads();
      }
    }
    
    final class BloomFilteredTermsEnum extends TermsEnum {
      private Terms delegateTerms;
      private TermsEnum delegateTermsEnum;
      private TermsEnum reuseDelegate;
      private final FuzzySet filter;
      
      public BloomFilteredTermsEnum(Terms delegateTerms, TermsEnum reuseDelegate, FuzzySet filter) throws IOException {
        this.delegateTerms = delegateTerms;
        this.reuseDelegate = reuseDelegate;
        this.filter = filter;
      }
      
      void reset(Terms delegateTerms, TermsEnum reuseDelegate) throws IOException {
        this.delegateTerms = delegateTerms;
        this.reuseDelegate = reuseDelegate;
        this.delegateTermsEnum = null;
      }
      
      private final TermsEnum delegate() throws IOException {
        if (delegateTermsEnum == null) {
          /* pull the iterator only if we really need it -
           * this can be a relativly heavy operation depending on the 
           * delegate postings format and they underlying directory
           * (clone IndexInput) */
          delegateTermsEnum = delegateTerms.iterator(reuseDelegate);
        }
        return delegateTermsEnum;
      }
      
      @Override
      public final BytesRef next() throws IOException {
        return delegate().next();
      }
      
      @Override
      public final Comparator<BytesRef> getComparator() {
        return delegateTerms.getComparator();
      }
      
      @Override
      public final boolean seekExact(BytesRef text, boolean useCache)
          throws IOException {
        // The magical fail-fast speed up that is the entire point of all of
        // this code - save a disk seek if there is a match on an in-memory
        // structure
        // that may occasionally give a false positive but guaranteed no false
        // negatives
        if (filter.contains(text) == ContainsResult.NO) {
          return false;
        }
        return delegate().seekExact(text, useCache);
      }
      
      @Override
      public final SeekStatus seekCeil(BytesRef text, boolean useCache)
          throws IOException {
        return delegate().seekCeil(text, useCache);
      }
      
      @Override
      public final void seekExact(long ord) throws IOException {
        delegate().seekExact(ord);
      }
      
      @Override
      public final BytesRef term() throws IOException {
        return delegate().term();
      }
      
      @Override
      public final long ord() throws IOException {
        return delegate().ord();
      }
      
      @Override
      public final int docFreq() throws IOException {
        return delegate().docFreq();
      }
      
      @Override
      public final long totalTermFreq() throws IOException {
        return delegate().totalTermFreq();
      }
      

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
          DocsAndPositionsEnum reuse, int flags) throws IOException {
        return delegate().docsAndPositions(liveDocs, reuse, flags);
      }

      @Override
      public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags)
          throws IOException {
        return delegate().docs(liveDocs, reuse, flags);
      }
      
      
    }
    
  }
  
  class BloomFilteredFieldsConsumer extends FieldsConsumer {
    private FieldsConsumer delegateFieldsConsumer;
    private Map<FieldInfo,FuzzySet> bloomFilters = new HashMap<FieldInfo,FuzzySet>();
    private SegmentWriteState state;
    
    
    public BloomFilteredFieldsConsumer(FieldsConsumer fieldsConsumer,
        SegmentWriteState state, PostingsFormat delegatePostingsFormat) {
      this.delegateFieldsConsumer = fieldsConsumer;
      this.state = state;
    }
    
    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      FuzzySet bloomFilter = bloomFilterFactory.getSetForField(state,field);
      if (bloomFilter != null) {
        assert bloomFilters.containsKey(field) == false;
        bloomFilters.put(field, bloomFilter);
        return new WrappedTermsConsumer(delegateFieldsConsumer.addField(field),bloomFilter);
      } else {
        // No, use the unfiltered fieldsConsumer - we are not interested in
        // recording any term Bitsets.
        return delegateFieldsConsumer.addField(field);
      }
    }
    
    @Override
    public void close() throws IOException {
      delegateFieldsConsumer.close();
      // Now we are done accumulating values for these fields
      List<Entry<FieldInfo,FuzzySet>> nonSaturatedBlooms = new ArrayList<Map.Entry<FieldInfo,FuzzySet>>();
      
      for (Entry<FieldInfo,FuzzySet> entry : bloomFilters.entrySet()) {
        FuzzySet bloomFilter = entry.getValue();
        if(!bloomFilterFactory.isSaturated(bloomFilter,entry.getKey())){          
          nonSaturatedBlooms.add(entry);
        }
      }
      String bloomFileName = IndexFileNames.segmentFileName(
          state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
      IndexOutput bloomOutput = null;
      try {
        bloomOutput = state.directory
            .createOutput(bloomFileName, state.context);
        CodecUtil.writeHeader(bloomOutput, BLOOM_CODEC_NAME,
            BLOOM_CODEC_VERSION);
        // remember the name of the postings format we will delegate to
        bloomOutput.writeString(delegatePostingsFormat.getName());
        
        // First field in the output file is the number of fields+blooms saved
        bloomOutput.writeInt(nonSaturatedBlooms.size());
        for (Entry<FieldInfo,FuzzySet> entry : nonSaturatedBlooms) {
          FieldInfo fieldInfo = entry.getKey();
          FuzzySet bloomFilter = entry.getValue();
          bloomOutput.writeInt(fieldInfo.number);
          saveAppropriatelySizedBloomFilter(bloomOutput, bloomFilter, fieldInfo);
        }
      } finally {
        IOUtils.close(bloomOutput);
      }
      //We are done with large bitsets so no need to keep them hanging around
      bloomFilters.clear(); 
    }
    
    private void saveAppropriatelySizedBloomFilter(IndexOutput bloomOutput,
        FuzzySet bloomFilter, FieldInfo fieldInfo) throws IOException {
      
      FuzzySet rightSizedSet = bloomFilterFactory.downsize(fieldInfo,
          bloomFilter);
      if (rightSizedSet == null) {
        rightSizedSet = bloomFilter;
      }
      rightSizedSet.serialize(bloomOutput);
    }
    
  }
  
  class WrappedTermsConsumer extends TermsConsumer {
    private TermsConsumer delegateTermsConsumer;
    private FuzzySet bloomFilter;
    
    public WrappedTermsConsumer(TermsConsumer termsConsumer,FuzzySet bloomFilter) {
      this.delegateTermsConsumer = termsConsumer;
      this.bloomFilter = bloomFilter;
    }
    
    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      return delegateTermsConsumer.startTerm(text);
    }
    
    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      
      // Record this term in our BloomFilter
      if (stats.docFreq > 0) {
        bloomFilter.addValue(text);
      }
      delegateTermsConsumer.finishTerm(text, stats);
    }
    
    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount)
        throws IOException {
      delegateTermsConsumer.finish(sumTotalTermFreq, sumDocFreq, docCount);
    }
    
    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return delegateTermsConsumer.getComparator();
    }
    
  }
  
}
