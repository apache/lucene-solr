package org.apache.solr.handler.component;
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

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 9/10/2009 - Moved out of StatsComponent to allow open access to UnInvertedField
 * FieldFacetStats is a utility to accumulate statistics on a set of values in one field,
 * for facet values present in another field.
 * <p/>
 * @see org.apache.solr.handler.component.StatsComponent
 *
 */

public class FieldFacetStats {
  public final String name;
  final FieldCache.DocTermsIndex si;
  final SchemaField facet_sf;
  final SchemaField field_sf;

  final int startTermIndex;
  final int endTermIndex;
  final int nTerms;

  final int numStatsTerms;

  public final Map<String, StatsValues> facetStatsValues;

  final List<HashMap<String, Integer>> facetStatsTerms;

  private final BytesRef tempBR = new BytesRef();

  public FieldFacetStats(String name, FieldCache.DocTermsIndex si, SchemaField field_sf, SchemaField facet_sf, int numStatsTerms) {
    this.name = name;
    this.si = si;
    this.field_sf = field_sf;
    this.facet_sf = facet_sf;
    this.numStatsTerms = numStatsTerms;

    startTermIndex = 1;
    endTermIndex = si.numOrd();
    nTerms = endTermIndex - startTermIndex;

    facetStatsValues = new HashMap<String, StatsValues>();

    // for mv stats field, we'll want to keep track of terms
    facetStatsTerms = new ArrayList<HashMap<String, Integer>>();
    if (numStatsTerms == 0) return;
    int i = 0;
    for (; i < numStatsTerms; i++) {
      facetStatsTerms.add(new HashMap<String, Integer>());
    }
  }

  BytesRef getTermText(int docID, BytesRef ret) {
    final int ord = si.getOrd(docID);
    if (ord == 0) {
      return null;
    } else {
      return si.lookup(ord, ret);
    }
  }

  public boolean facet(int docID, BytesRef v) {
    int term = si.getOrd(docID);
    int arrIdx = term - startTermIndex;
    if (arrIdx >= 0 && arrIdx < nTerms) {
      final BytesRef br = si.lookup(term, tempBR);
      String key = (br == null)?null:facet_sf.getType().indexedToReadable(br.utf8ToString());
      StatsValues stats = facetStatsValues.get(key);
      if (stats == null) {
        stats = StatsValuesFactory.createStatsValues(field_sf);
        facetStatsValues.put(key, stats);
      }

      if (v != null && v.length>0) {
        stats.accumulate(v);
      } else {
        stats.missing();
        return false;
      }
      return true;
    }
    return false;
  }


  // Function to keep track of facet counts for term number.
  // Currently only used by UnInvertedField stats
  public boolean facetTermNum(int docID, int statsTermNum) {

    int term = si.getOrd(docID);
    int arrIdx = term - startTermIndex;
    if (arrIdx >= 0 && arrIdx < nTerms) {
      final BytesRef br = si.lookup(term, tempBR);
      String key = br == null ? null : br.utf8ToString();
      HashMap<String, Integer> statsTermCounts = facetStatsTerms.get(statsTermNum);
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
  public boolean accumulateTermNum(int statsTermNum, BytesRef value) {
    if (value == null) return false;
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

}


