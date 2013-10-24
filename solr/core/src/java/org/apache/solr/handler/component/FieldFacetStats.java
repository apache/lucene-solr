package org.apache.solr.handler.component;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;


/**
 * FieldFacetStats is a utility to accumulate statistics on a set of values in one field,
 * for facet values present in another field.
 * <p>
 * 9/10/2009 - Moved out of StatsComponent to allow open access to UnInvertedField
 * <p/>
 * @see org.apache.solr.handler.component.StatsComponent
 *
 */

public class FieldFacetStats {
  public final String name;
  final SchemaField facet_sf;
  final SchemaField field_sf;

  public final Map<String, StatsValues> facetStatsValues;

  List<HashMap<String, Integer>> facetStatsTerms;

  final AtomicReader topLevelReader;
  AtomicReaderContext leave;
  final ValueSource valueSource;
  AtomicReaderContext context;
  FunctionValues values;

  SortedDocValues topLevelSortedValues = null;

  private final BytesRef tempBR = new BytesRef();

  public FieldFacetStats(SolrIndexSearcher searcher, String name, SchemaField field_sf, SchemaField facet_sf) {
    this.name = name;
    this.field_sf = field_sf;
    this.facet_sf = facet_sf;

    topLevelReader = searcher.getAtomicReader();
    valueSource = facet_sf.getType().getValueSource(facet_sf, null);

    facetStatsValues = new HashMap<String, StatsValues>();
    facetStatsTerms = new ArrayList<HashMap<String, Integer>>();
  }

  private StatsValues getStatsValues(String key) throws IOException {
    StatsValues stats = facetStatsValues.get(key);
    if (stats == null) {
      stats = StatsValuesFactory.createStatsValues(field_sf);
      facetStatsValues.put(key, stats);
      stats.setNextReader(context);
    }
    return stats;
  }

  // docID is relative to the context
  public void facet(int docID) throws IOException {
    final String key = values.exists(docID)
        ? values.strVal(docID)
        : null;
    final StatsValues stats = getStatsValues(key);
    stats.accumulate(docID);
  }

  // Function to keep track of facet counts for term number.
  // Currently only used by UnInvertedField stats
  public boolean facetTermNum(int docID, int statsTermNum) throws IOException {
    if (topLevelSortedValues == null) {
      topLevelSortedValues = FieldCache.DEFAULT.getTermsIndex(topLevelReader, name);
    }
    
    int term = topLevelSortedValues.getOrd(docID);
    int arrIdx = term;
    if (arrIdx >= 0 && arrIdx < topLevelSortedValues.getValueCount()) {
      final BytesRef br;
      if (term == -1) {
        br = null;
      } else {
        br = tempBR;
        topLevelSortedValues.lookupOrd(term, tempBR);
      }
      String key = br == null ? null : br.utf8ToString();
      while (facetStatsTerms.size() <= statsTermNum) {
        facetStatsTerms.add(new HashMap<String, Integer>());
      }
      final Map<String, Integer> statsTermCounts = facetStatsTerms.get(statsTermNum);
      Integer statsTermCount = statsTermCounts.get(key);
      if (statsTermCount == null) {
        statsTermCounts.put(key, 1);
      } else {
        statsTermCounts.put(key, statsTermCount + 1);
      }
      return true;
    }
    return false;
  }


  //function to accumulate counts for statsTermNum to specified value
  public boolean accumulateTermNum(int statsTermNum, BytesRef value) throws IOException {
    if (value == null) return false;
    while (facetStatsTerms.size() <= statsTermNum) {
      facetStatsTerms.add(new HashMap<String, Integer>());
    }
    for (Map.Entry<String, Integer> stringIntegerEntry : facetStatsTerms.get(statsTermNum).entrySet()) {
      Map.Entry pairs = (Map.Entry) stringIntegerEntry;
      String key = (String) pairs.getKey();
      StatsValues facetStats = facetStatsValues.get(key);
      if (facetStats == null) {
        facetStats = StatsValuesFactory.createStatsValues(field_sf);
        facetStatsValues.put(key, facetStats);
      }
      Integer count = (Integer) pairs.getValue();
      if (count != null) {
        facetStats.accumulate(value, count);
      }
    }
    return true;
  }

  public void setNextReader(AtomicReaderContext ctx) throws IOException {
    this.context = ctx;
    values = valueSource.getValues(Collections.emptyMap(), ctx);
    for (StatsValues stats : facetStatsValues.values()) {
      stats.setNextReader(ctx);
    }
  }

}


