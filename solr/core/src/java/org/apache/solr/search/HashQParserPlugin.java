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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.StrField;

import com.google.common.primitives.Longs;

/**
* syntax fq={!hash workers=11 worker=4 keys=field1,field2}
* */

public class HashQParserPlugin extends QParserPlugin {

  public static final String NAME = "hash";


  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
    return new HashQParser(query, localParams, params, request);
  }

  private class HashQParser extends QParser {

    public HashQParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      super(query, localParams, params, request);
    }

    public Query parse() {
      int workers = localParams.getInt("workers", 0);
      int worker = localParams.getInt("worker", 0);
      String keys = params.get("partitionKeys");
      keys = keys.replace(" ", "");
      return new HashQuery(keys, workers, worker);
    }
  }

  private class HashQuery extends ExtendedQueryBase implements PostFilter {

    private String keysParam;
    private int workers;
    private int worker;

    public boolean getCache() {
      if(getCost() > 99) {
        return false;
      } else {
        return super.getCache();
      }
    }

    public int hashCode() {
      return classHash() + 
          31 * keysParam.hashCode() + 
          31 * workers + 
          31 * worker;
    }

    public boolean equals(Object other) {
      return sameClassAs(other) &&
             equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(HashQuery other) {
      return keysParam.equals(other.keysParam) && 
             workers == other.workers && 
             worker == other.worker;
    }

    public HashQuery(String keysParam, int workers, int worker) {
      this.keysParam = keysParam;
      this.workers = workers;
      this.worker = worker;
    }

    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {

      String[] keys = keysParam.split(",");
      SolrIndexSearcher solrIndexSearcher = (SolrIndexSearcher)searcher;
      IndexReaderContext context = solrIndexSearcher.getTopReaderContext();

      List<LeafReaderContext> leaves =  context.leaves();
      FixedBitSet[] fixedBitSets = new FixedBitSet[leaves.size()];

      for(LeafReaderContext leaf : leaves) {
        try {
          SegmentPartitioner segmentPartitioner = new SegmentPartitioner(leaf,worker,workers, keys, solrIndexSearcher);
          segmentPartitioner.run();
          fixedBitSets[segmentPartitioner.context.ord] = segmentPartitioner.docs;
        } catch(Exception e) {
          throw new IOException(e);
        }
      }

      ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(new BitsFilter(fixedBitSets));
      return searcher.rewrite(constantScoreQuery).createWeight(searcher, false, boost);
    }

    public class BitsFilter extends Filter {
      private FixedBitSet[] bitSets;
      public BitsFilter(FixedBitSet[] bitSets) {
        this.bitSets = bitSets;
      }

      public String toString(String s) {
        return s;
      }

      public DocIdSet getDocIdSet(LeafReaderContext context, Bits bits) {
        return BitsFilteredDocIdSet.wrap(new BitDocIdSet(bitSets[context.ord]), bits);
      }

      @Override
      public boolean equals(Object other) {
        return sameClassAs(other) &&
               equalsTo(getClass().cast(other));
      }

      private boolean equalsTo(BitsFilter other) {
        return Arrays.equals(bitSets, other.bitSets);
      }

      @Override
      public int hashCode() {
        return classHash() + Arrays.asList(bitSets).hashCode();
      }
    }


    class SegmentPartitioner implements Runnable {

      public LeafReaderContext context;
      private int worker;
      private int workers;
      private HashKey k;
      public FixedBitSet docs;
      public SegmentPartitioner(LeafReaderContext context,
                                int worker,
                                int workers,
                                String[] keys,
                                SolrIndexSearcher solrIndexSearcher) {
        this.context = context;
        this.worker = worker;
        this.workers = workers;

        HashKey[] hashKeys = new HashKey[keys.length];
        IndexSchema schema = solrIndexSearcher.getSchema();
        for(int i=0; i<keys.length; i++) {
          String key = keys[i];
          FieldType ft = schema.getField(key).getType();
          HashKey h = null;
          if(ft instanceof StrField) {
            h = new BytesHash(key, ft);
          } else {
            h = new NumericHash(key);
          }
          hashKeys[i] = h;
        }

        k = (hashKeys.length > 1) ? new CompositeHash(hashKeys) : hashKeys[0];
      }

      public void run() {
        LeafReader reader = context.reader();

        try {
          k.setNextReader(context);
          this.docs = new FixedBitSet(reader.maxDoc());
          int maxDoc = reader.maxDoc();
          for(int i=0; i<maxDoc; i++) {
            if((k.hashCode(i) & 0x7FFFFFFF) % workers == worker) {
              docs.set(i);
            }
          }
        }catch(Exception e) {
         throw new RuntimeException(e);
        }
      }
    }

    public DelegatingCollector getFilterCollector(IndexSearcher indexSearcher) {
      String[] keys = keysParam.split(",");
      HashKey[] hashKeys = new HashKey[keys.length];
      SolrIndexSearcher searcher = (SolrIndexSearcher)indexSearcher;
      IndexSchema schema = searcher.getSchema();
      for(int i=0; i<keys.length; i++) {
        String key = keys[i];
        FieldType ft = schema.getField(key).getType();
        HashKey h = null;
        if(ft instanceof StrField) {
          h = new BytesHash(key, ft);
        } else {
          h = new NumericHash(key);
        }
        hashKeys[i] = h;
      }
      HashKey k = (hashKeys.length > 1) ? new CompositeHash(hashKeys) : hashKeys[0];
      return new HashCollector(k, workers, worker);
    }
  }

  private class HashCollector extends DelegatingCollector {
    private int worker;
    private int workers;
    private HashKey hashKey;
    private LeafCollector leafCollector;

    public HashCollector(HashKey hashKey, int workers, int worker) {
      this.hashKey = hashKey;
      this.workers = workers;
      this.worker = worker;
    }

    public void setScorer(Scorer scorer) throws IOException{
      leafCollector.setScorer(scorer);
    }

    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.hashKey.setNextReader(context);
      this.leafCollector = delegate.getLeafCollector(context);
    }

    public void collect(int doc) throws IOException {
      if((hashKey.hashCode(doc) & 0x7FFFFFFF) % workers == worker) {
        leafCollector.collect(doc);
      }
    }
  }

  private interface HashKey {
    public void setNextReader(LeafReaderContext reader) throws IOException;
    public long hashCode(int doc) throws IOException;
  }

  private class BytesHash implements HashKey {

    private SortedDocValues values;
    private String field;
    private FieldType fieldType;
    private CharsRefBuilder charsRefBuilder = new CharsRefBuilder();

    public BytesHash(String field, FieldType fieldType) {
      this.field = field;
      this.fieldType = fieldType;
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      values = context.reader().getSortedDocValues(field);
    }

    public long hashCode(int doc) throws IOException {
      if (doc > values.docID()) {
        values.advance(doc);
      }
      BytesRef ref;
      if (doc == values.docID()) {
        ref = values.binaryValue();
      } else {
        ref = null;
      }
      this.fieldType.indexedToReadable(ref, charsRefBuilder);
      CharsRef charsRef = charsRefBuilder.get();
      return charsRef.hashCode();
    }
  }

  private class NumericHash implements HashKey {

    private NumericDocValues values;
    private String field;

    public NumericHash(String field) {
      this.field = field;
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      values = context.reader().getNumericDocValues(field);
    }

    public long hashCode(int doc) throws IOException {
      int valuesDocID = values.docID();
      if (valuesDocID < doc) {
        valuesDocID = values.advance(doc);
      }
      long l;
      if (valuesDocID == doc) {
        l = values.longValue();
      } else {
        l = 0;
      }
      return Longs.hashCode(l);
    }
  }

  private class ZeroHash implements HashKey {

    public long hashCode(int doc) {
      return 0;
    }

    public void setNextReader(LeafReaderContext context) {

    }
  }

  private class CompositeHash implements HashKey {

    private HashKey key1;
    private HashKey key2;
    private HashKey key3;
    private HashKey key4;

    public CompositeHash(HashKey[] hashKeys) {
      key1 = hashKeys[0];
      key2 = hashKeys[1];
      key3 = (hashKeys.length > 2) ? hashKeys[2] : new ZeroHash();
      key4 = (hashKeys.length > 3) ? hashKeys[3] : new ZeroHash();
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      key1.setNextReader(context);
      key2.setNextReader(context);
      key3.setNextReader(context);
      key4.setNextReader(context);
    }

    public long hashCode(int doc) throws IOException {
      return key1.hashCode(doc)+key2.hashCode(doc)+key3.hashCode(doc)+key4.hashCode(doc);
    }
  }
}
