package org.apache.lucene.search.similarities;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Index searcher implementation that takes an {@link BasicStats} instance and
 * returns statistics accordingly. Most of the methods are not implemented, so
 * it can only be used for Similarity unit testing.
 */
public class SpoofIndexSearcher extends IndexSearcher {
  public SpoofIndexSearcher(BasicStats stats) {
    super(new SpoofIndexReader(stats));
  }
  
  public static class SpoofIndexReader extends IndexReader {
    /** The stats the reader has to return. */
    protected BasicStats stats;
    /** The fields the reader has to return. */
    protected SpoofFields fields;
    
    public SpoofIndexReader(BasicStats stats) {
      this.stats = stats;
      this.fields = new SpoofFields(stats);
    }

    @Override
    public int numDocs() {
      return stats.getNumberOfDocuments();
    }

    @Override
    public int maxDoc() {
      return stats.getNumberOfDocuments();
    }

    @Override
    public Fields fields() throws IOException {
      return fields;
    }

    @Override
    public Collection<String> getFieldNames(FieldOption fldOption) {
      return Arrays.asList(new String[]{"spoof"});
    }

    @Override
    public ReaderContext getTopReaderContext() {
      return new AtomicReaderContext(this);
    }
    
    @Override
    public boolean hasDeletions() {
      return false;
    }

    // ------------------------ Not implemented methods ------------------------
    
    @Override
    public TermFreqVector[] getTermFreqVectors(int docNumber)
        throws IOException {
      return null;
    }

    @Override
    public TermFreqVector getTermFreqVector(int docNumber, String field)
        throws IOException {
      return null;
    }

    @Override
    public void getTermFreqVector(int docNumber, String field,
        TermVectorMapper mapper) throws IOException {
    }

    @Override
    public void getTermFreqVector(int docNumber, TermVectorMapper mapper)
        throws IOException {
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    }

    @Override
    public byte[] norms(String field) throws IOException {
      return null;
    }

    @Override
    protected void doSetNorm(int doc, String field, byte value)
        throws CorruptIndexException, IOException {
    }

    @Override
    public PerDocValues perDocValues() throws IOException {
      return null;
    }

    @Override
    protected void doDelete(int docNum) throws CorruptIndexException,
        IOException {
    }

    @Override
    protected void doUndeleteAll() throws CorruptIndexException, IOException {
    }

    @Override
    protected void doCommit(Map<String,String> commitUserData)
        throws IOException {
    }

    @Override
    protected void doClose() throws IOException {
    }

    @Override
    public Bits getLiveDocs() {
      return null;
    }
  }
  
  /** Spoof Fields class for Similarity testing. */
  public static class SpoofFields extends Fields {
    /** The stats the object has to return. */
    protected SpoofTerms terms;
    
    public SpoofFields(BasicStats stats) {
      this.terms = new SpoofTerms(stats);
    }
    
    @Override
    public Terms terms(String field) throws IOException {
      return terms;
    }
    
    // ------------------------ Not implemented methods ------------------------

    @Override
    public FieldsEnum iterator() throws IOException {
      return null;
    }
  }
  
  /** Spoof Terms class for Similarity testing. */
  public static class SpoofTerms extends Terms {
    /** The stats the object has to return. */
    protected BasicStats stats;
    
    public SpoofTerms(BasicStats stats) {
      this.stats = stats;
    }
    
    @Override
    public long getSumTotalTermFreq() throws IOException {
      return stats.getNumberOfFieldTokens();
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return stats.getDocFreq();
    }
    
    @Override
    public int getDocCount() throws IOException {
      return stats.getDocFreq();
    }    
    
    // ------------------------ Not implemented methods ------------------------

    
    @Override
    public TermsEnum iterator() throws IOException {
      return null;
    }

    @Override
    public long getUniqueTermCount() throws IOException {
      return -1;
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return null;
    }
  }
}
