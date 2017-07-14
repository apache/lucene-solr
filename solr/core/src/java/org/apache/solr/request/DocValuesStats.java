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
package org.apache.solr.request;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.FieldFacetStats;
import org.apache.solr.handler.component.StatsField;
import org.apache.solr.handler.component.StatsValues;
import org.apache.solr.handler.component.StatsValuesFactory;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Computes term stats for docvalues field (single or multivalued).
 * <p>
 * Instead of working on a top-level reader view (binary-search per docid),
 * it collects per-segment, but maps ordinals to global ordinal space using
 * MultiDocValues' OrdinalMap.
 */
public class DocValuesStats {
  private DocValuesStats() {}
  
  public static StatsValues getCounts(SolrIndexSearcher searcher, StatsField statsField, DocSet docs, String[] facet) throws IOException {

    final SchemaField schemaField = statsField.getSchemaField(); 

    assert null != statsField.getSchemaField()
      : "DocValuesStats requires a StatsField using a SchemaField";

    final String fieldName = schemaField.getName();
    final FieldType ft = schemaField.getType();
    final StatsValues res = StatsValuesFactory.createStatsValues(statsField);
    
    //Initialize facetstats, if facets have been passed in
    final FieldFacetStats[] facetStats = new FieldFacetStats[facet.length];
    int upto = 0;
       
    for (String facetField : facet) {
      SchemaField fsf = searcher.getSchema().getField(facetField);
      if ( fsf.multiValued()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Stats can only facet on single-valued fields, not: " + facetField );
      }
      
      SchemaField facetSchemaField = searcher.getSchema().getField(facetField);
      facetStats[upto++] = new FieldFacetStats(searcher, facetSchemaField, statsField);
    }
    // TODO: remove multiValuedFieldCache(), check dv type / uninversion type?
    final boolean multiValued = schemaField.multiValued() || ft.multiValuedFieldCache();

    SortedSetDocValues si; // for term lookups only
    OrdinalMap ordinalMap = null; // for mapping per-segment ords to global ones
    if (multiValued) {
      si = searcher.getSlowAtomicReader().getSortedSetDocValues(fieldName);
      
      if (si instanceof MultiSortedSetDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedSetDocValues)si).mapping;
      }
    } else {
      SortedDocValues single = searcher.getSlowAtomicReader().getSortedDocValues(fieldName);
      si = single == null ? null : DocValues.singleton(single);
      if (single instanceof MultiDocValues.MultiSortedDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedDocValues)single).mapping;
      }
    }
    if (si == null) {
      si = DocValues.emptySortedSet();
    }
    if (si.getValueCount() >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Currently this stats method is limited to " + Integer.MAX_VALUE + " unique terms");
    }
    
    int missingDocCountTotal = 0;
    final int nTerms = (int) si.getValueCount();    
    // count collection array only needs to be as big as the number of terms we are
    // going to collect counts for.
    final int[] counts = new int[nTerms];
    
    Filter filter = docs.getTopFilter();
    List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
    
    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      LeafReaderContext leaf = leaves.get(subIndex);
      DocIdSet dis = filter.getDocIdSet(leaf, null); // solr docsets already exclude any deleted docs
      DocIdSetIterator disi = null;
      
      if (dis != null) {
        disi = dis.iterator();
      }
      if (disi != null) {
        int docBase = leaf.docBase;
        
        if (multiValued) {
          SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(fieldName);
          if (sub == null) {
            sub = DocValues.emptySortedSet();
          }
          SortedDocValues singleton = DocValues.unwrapSingleton(sub);
          if (singleton != null) {
            // some codecs may optimize SORTED_SET storage for single-valued fields
            missingDocCountTotal += accumSingle(counts, docBase, facetStats, singleton, disi, subIndex, ordinalMap);
          } else {
            missingDocCountTotal += accumMulti(counts, docBase, facetStats, sub, disi, subIndex, ordinalMap);
          }
        } else {
          SortedDocValues sub = leaf.reader().getSortedDocValues(fieldName);
          if (sub == null) {
            sub = DocValues.emptySorted();
          }
          missingDocCountTotal += accumSingle(counts, docBase, facetStats, sub, disi, subIndex, ordinalMap);
        }
      }
    }
    // add results in index order
    for (int ord = 0; ord < counts.length; ord++) {
      int count = counts[ord];

      if (count > 0) {
        final BytesRef value = si.lookupOrd(ord);
        res.accumulate(value, count);
        for (FieldFacetStats f : facetStats) {
          f.accumulateTermNum(ord, value);
        }
      }
    }
    res.addMissing(missingDocCountTotal);
    
    if (facetStats.length > 0) {
      for (FieldFacetStats f : facetStats) {
        Map<String,StatsValues> facetStatsValues = f.facetStatsValues;
        f.accumulateMissing();
        res.addFacet(f.name, facetStatsValues);
      }
    }
    
    return res;
  }

  /** accumulates per-segment single-valued stats */
  static int accumSingle(int counts[], int docBase, FieldFacetStats[] facetStats, SortedDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map) throws IOException {
    final LongValues ordMap = map == null ? null : map.getGlobalOrds(subIndex);
    int missingDocCount = 0;
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (doc > si.docID()) {
        si.advance(doc);
      }
      if (doc == si.docID()) {
        int term = si.ordValue();
        if (map != null) {
          term = (int) ordMap.get(term);
        }
        counts[term]++;
        for (FieldFacetStats f : facetStats) {
          f.facetTermNum(docBase + doc, term);
        }
      }else{
        for (FieldFacetStats f : facetStats) {
          f.facetMissingNum(docBase + doc);
        }
        
        missingDocCount++;
      }
    }
    return missingDocCount;
  }
  
  /** accumulates per-segment multi-valued stats */
  
  static int accumMulti(int counts[], int docBase, FieldFacetStats[] facetStats, SortedSetDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map) throws IOException {
    final LongValues ordMap = map == null ? null : map.getGlobalOrds(subIndex);
    int missingDocCount = 0;
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (doc > si.docID()) {
        si.advance(doc);
      }
      if (doc == si.docID()) {
        long ord;
        while ((ord = si.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          int term = (int) ord;
          if (map != null) {
            term = (int) ordMap.get(term);
          }
          counts[term]++;
          for (FieldFacetStats f : facetStats) {
            f.facetTermNum(docBase + doc, term);
          }
        }
      } else {
        for (FieldFacetStats f : facetStats) {
          f.facetMissingNum(docBase + doc);
        }
        
        missingDocCount++;
      }
    }
    
    return missingDocCount;
  }
}
