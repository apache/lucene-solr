package org.apache.solr.request;

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
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.handler.component.FieldFacetStats;
import org.apache.solr.handler.component.StatsValues;
import org.apache.solr.handler.component.StatsValuesFactory;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
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
  
  public static StatsValues getCounts(SolrIndexSearcher searcher, String fieldName, DocSet docs, boolean calcDistinct, String[] facet) throws IOException {
    SchemaField schemaField = searcher.getSchema().getField(fieldName);
    FieldType ft = schemaField.getType();
    StatsValues res = StatsValuesFactory.createStatsValues(schemaField, calcDistinct);
    
    //Initialize facetstats, if facets have been passed in
    final FieldFacetStats[] facetStats = new FieldFacetStats[facet.length];
    int upto = 0;
    for (String facetField : facet) {
      SchemaField facetSchemaField = searcher.getSchema().getField(facetField);
      facetStats[upto++] = new FieldFacetStats(searcher, facetField, schemaField, facetSchemaField, calcDistinct);
    }
    
    // TODO: remove multiValuedFieldCache(), check dv type / uninversion type?
    final boolean multiValued = schemaField.multiValued() || ft.multiValuedFieldCache();

    SortedSetDocValues si; // for term lookups only
    OrdinalMap ordinalMap = null; // for mapping per-segment ords to global ones
    if (multiValued) {
      si = searcher.getAtomicReader().getSortedSetDocValues(fieldName);
      if (si instanceof MultiSortedSetDocValues) {
        ordinalMap = ((MultiSortedSetDocValues)si).mapping;
      }
    } else {
      SortedDocValues single = searcher.getAtomicReader().getSortedDocValues(fieldName);
      si = single == null ? null : DocValues.singleton(single);
      if (single instanceof MultiSortedDocValues) {
        ordinalMap = ((MultiSortedDocValues)single).mapping;
      }
    }
    if (si == null) {
      si = DocValues.EMPTY_SORTED_SET;
    }
    if (si.getValueCount() >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Currently this stats method is limited to " + Integer.MAX_VALUE + " unique terms");
    }

    DocSet missing = docs.andNot( searcher.getDocSet(new TermRangeQuery(fieldName, null, null, false, false)));

    final int nTerms = (int) si.getValueCount();   
    
    // count collection array only needs to be as big as the number of terms we are
    // going to collect counts for.
    final int[] counts = new int[nTerms];
    
    Filter filter = docs.getTopFilter();
    List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      AtomicReaderContext leaf = leaves.get(subIndex);
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
            sub = DocValues.EMPTY_SORTED_SET;
          }
          final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
          if (singleton != null) {
            // some codecs may optimize SORTED_SET storage for single-valued fields
            accumSingle(counts, docBase, facetStats, singleton, disi, subIndex, ordinalMap);
          } else {
            accumMulti(counts, docBase, facetStats, sub, disi, subIndex, ordinalMap);
          }
        } else {
          SortedDocValues sub = leaf.reader().getSortedDocValues(fieldName);
          if (sub == null) {
            sub = DocValues.EMPTY_SORTED;
          }
          accumSingle(counts, docBase, facetStats, sub, disi, subIndex, ordinalMap);
        }
      }
    }
    
    // add results in index order
    BytesRef value = new BytesRef();
    for (int ord = 0; ord < counts.length; ord++) {
      int count = counts[ord];
      if (count > 0) {
        si.lookupOrd(ord, value);
        res.accumulate(value, count);
        for (FieldFacetStats f : facetStats) {
          f.accumulateTermNum(ord, value);
        }
      }
    }

    res.addMissing(missing.size());
    if (facetStats.length > 0) {
      for (FieldFacetStats f : facetStats) {
        Map<String, StatsValues> facetStatsValues = f.facetStatsValues;
        FieldType facetType = searcher.getSchema().getFieldType(f.name);
        for (Map.Entry<String,StatsValues> entry : facetStatsValues.entrySet()) {
          String termLabel = entry.getKey();
          int missingCount = searcher.numDocs(new TermQuery(new Term(f.name, facetType.toInternal(termLabel))), missing);
          entry.getValue().addMissing(missingCount);
        }
        res.addFacet(f.name, facetStatsValues);
      }
    }
    return res;
  }

  /** accumulates per-segment single-valued stats */
  static void accumSingle(int counts[], int docBase, FieldFacetStats[] facetStats, SortedDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      int term = si.getOrd(doc);
      if (term >= 0) {
        if (map != null) {
          term = (int) map.getGlobalOrd(subIndex, term);
        }
        counts[term]++;
        for (FieldFacetStats f : facetStats) {
          f.facetTermNum(docBase + doc, term);
        }
      }
    }
  }
  
  /** accumulates per-segment multi-valued stats */
  static void accumMulti(int counts[], int docBase, FieldFacetStats[] facetStats, SortedSetDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      si.setDocument(doc);
      long ord;
      while ((ord = si.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        int term = (int) ord;
        if (map != null) {
          term = (int) map.getGlobalOrd(subIndex, term);
        }
        counts[term]++;
        for (FieldFacetStats f : facetStats) {
          f.facetTermNum(docBase + doc, term);
        }
      }
    }
  }
}
