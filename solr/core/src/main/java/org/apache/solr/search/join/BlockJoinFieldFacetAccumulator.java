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
package org.apache.solr.search.join;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.DocValuesFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * This class is responsible for collecting block join facet counts for particular field
 */
class BlockJoinFieldFacetAccumulator {
  private String fieldName;
  private FieldType fieldType;
  private int currentSegment = -1;
  // for term lookups only
  private SortedSetDocValues topSSDV;
  private int[] globalCounts;
  private SortedSetDocValues segmentSSDV;
  // elems are : facet value counter<<32 | last parent doc num 
  private long[] segmentAccums = new long[0];
  // for mapping per-segment ords to global ones
  private OrdinalMap ordinalMap;
  private SchemaField schemaField;
  private SortedDocValues segmentSDV;
  
  BlockJoinFieldFacetAccumulator(String fieldName, SolrIndexSearcher searcher) throws IOException {
    this.fieldName = fieldName;
    schemaField = searcher.getSchema().getField(fieldName);
    fieldType = schemaField.getType();
    ordinalMap = null;
    if (schemaField.multiValued()) {
      topSSDV = searcher.getSlowAtomicReader().getSortedSetDocValues(fieldName);
      if (topSSDV instanceof MultiDocValues.MultiSortedSetDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedSetDocValues) topSSDV).mapping;
      }
    } else {
      SortedDocValues single = searcher.getSlowAtomicReader().getSortedDocValues(fieldName);
      if (single instanceof MultiDocValues.MultiSortedDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedDocValues) single).mapping;
      }
      if (single != null) {
        topSSDV = DocValues.singleton(single);
      }
    }
  }
  
  private boolean initSegmentData(String fieldName, LeafReaderContext leaf) throws IOException {
    segmentSSDV = DocValues.getSortedSet(leaf.reader(), fieldName);
    segmentAccums  = ArrayUtil.grow(segmentAccums, (int)segmentSSDV.getValueCount()+1);//+1
    // zero counts, -1 parent
    Arrays.fill(segmentAccums,0,(int)segmentSSDV.getValueCount()+1, 0x00000000ffffffffL);
    segmentSDV = DocValues.unwrapSingleton(segmentSSDV);
    return segmentSSDV.getValueCount()!=0;// perhaps we need to count "missings"?? 
  }
  
  interface AggregatableDocIter extends DocIterator {
    void reset();
    /** a key to aggregate the current document */
    int getAggKey();
    
  }
  static class SortedIntsAggDocIterator implements AggregatableDocIter {
    private int[] childDocs;
    private int childCount;
    private int parentDoc;
    private int pos=-1;
    
    public SortedIntsAggDocIterator(int[] childDocs, int childCount, int parentDoc) {
      this.childDocs = childDocs;
      this.childCount = childCount;
      this.parentDoc = parentDoc;
    }

    
    @Override
    public boolean hasNext() {
      return pos<childCount;
    }

    @Override
    public Integer next() {
      return nextDoc();
    }

    @Override
    public int nextDoc() {
      return childDocs[pos++];
    }

    @Override
    public float score() {
      return 0;
    }
    @Override
    public void reset() {
      pos=0;
    }
    @Override
    public int getAggKey(){
      return parentDoc;
    }
  }

  void updateCountsWithMatchedBlock(AggregatableDocIter iter) throws IOException {
    if (segmentSDV != null) {
      // some codecs may optimize SORTED_SET storage for single-valued fields
      for (iter.reset(); iter.hasNext(); ) {
        final int docNum = iter.nextDoc();
        if (docNum > segmentSDV.docID()) {
          segmentSDV.advance(docNum);
        }
        
        int term;
        if (docNum == segmentSDV.docID()) {
          term = segmentSDV.ordValue();
        } else {
          term = -1;
        }
        accumulateTermOrd(term, iter.getAggKey());
        //System.out.println("doc# "+docNum+" "+fieldName+" term# "+term+" tick "+Long.toHexString(segmentAccums[1+term]));
      }
    } else {
      for (iter.reset(); iter.hasNext(); ) {
        final int docNum = iter.nextDoc();
        if (docNum > segmentSSDV.docID()) {
          segmentSSDV.advance(docNum);
        }
        if (docNum == segmentSSDV.docID()) {
          int term = (int) segmentSSDV.nextOrd();
          do { // absent values are designated by term=-1, first iteration counts [0] as "missing", and exit, otherwise it spins 
            accumulateTermOrd(term, iter.getAggKey());
          } while (term>=0 && (term = (int) segmentSSDV.nextOrd()) >= 0);
        }
      }
    }
  }
  
  String getFieldName() {
    return fieldName;
  }
  
  /** copy paste from {@link DocValuesFacets} */
  NamedList<Integer> getFacetValue() throws IOException {
    NamedList<Integer> facetValue = new NamedList<>();
    final CharsRefBuilder charsRef = new CharsRefBuilder(); // if there is no globs, take segment's ones
    for (int i = 1; i< (globalCounts!=null ? globalCounts.length: segmentAccums.length); i++) {
      int count = globalCounts!=null ? globalCounts[i] : (int)(segmentAccums [i]>>32);
      if (count > 0) {
        BytesRef term = topSSDV.lookupOrd(-1 + i);
        fieldType.indexedToReadable(term, charsRef);
        facetValue.add(charsRef.toString(), count);
      }
    }
    return facetValue;
  }
  
  // @todo we can track in max term nums to loop only changed range while migrating and labeling 
  private void accumulateTermOrd(int term, int parentDoc) {
    long accum = segmentAccums[1+term];
    if(((int)(accum & 0xffffffffL))!=parentDoc)
    {// incrementing older 32, reset smaller 32, set them to the new parent
      segmentAccums[1+term] = ((accum +(0x1L<<32))&0xffffffffL<<32)|parentDoc;
    }
  }
  
  void setNextReader(LeafReaderContext context) throws IOException {
    initSegmentData(fieldName, context);
    currentSegment = context.ord;
  }
  
  void migrateGlobal(){
    if (currentSegment<0 // no hits
        || segmentAccums.length==0 
        || ordinalMap==null) { // single segment
      return;
    }
    
    if(globalCounts==null){
      // it might be just a single segment 
        globalCounts = new int[(int) ordinalMap.getValueCount()+ /*[0] for missing*/1];
    }else{
      assert currentSegment>=0;
    }
    
    migrateGlobal(globalCounts, segmentAccums, currentSegment, ordinalMap);
  }

  /** folds counts in segment ordinal space (segCounts) into global ordinal space (counts) 
   * copy paste-from {@link DocValuesFacets#migrateGlobal(int[], int[], int, OrdinalMap)}*/
  void migrateGlobal(int counts[], long segCounts[], int subIndex, OrdinalMap map) {
    
    final LongValues ordMap = map.getGlobalOrds(subIndex);
    // missing count
    counts[0] += (int) (segCounts[0]>>32);
    
    // migrate actual ordinals
    for (int ord = 1; ord <= segmentSSDV.getValueCount(); ord++) {
      int count = (int) (segCounts[ord]>>32);
      if (count != 0) {
        counts[1+(int) ordMap.get(ord-1)] += count;
      }
    }
  }
}
