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
package org.apache.solr.analytics.facet;

import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.analytics.util.FacetRangeGenerator;
import org.apache.solr.analytics.util.FacetRangeGenerator.FacetRange;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.Filter;

/**
 * A facet that groups data by a discrete set of ranges.
 */
public class RangeFacet extends AbstractSolrQueryFacet {
  protected final SchemaField field;
  protected final String start;
  protected final String end;
  protected final List<String> gaps;
  protected boolean hardEnd = false;
  protected EnumSet<FacetRangeInclude> include;
  protected EnumSet<FacetRangeOther> others;

  public RangeFacet(String name, SchemaField field, String start, String end, List<String> gaps) {
    super(name);
    this.field = field;
    this.start = start;
    this.end = end;
    this.gaps = gaps;
    include = EnumSet.of(FacetRangeInclude.LOWER);
    others = EnumSet.of(FacetRangeOther.NONE);
  }

  @Override
  public void createFacetValueExecuters(final Filter filter, SolrQueryRequest queryRequest, Consumer<FacetValueQueryExecuter> consumer) {
    // Computes the end points of the ranges in the rangeFacet
    final FacetRangeGenerator<? extends Comparable<?>> rec = FacetRangeGenerator.create(this);
    final SchemaField sf = field;

    // Create a rangeFacetAccumulator for each range and
    // collect the documents for that range.
    for (FacetRange range : rec.getRanges()) {
      Query q = sf.getType().getRangeQuery(null, sf, range.lower, range.upper, range.includeLower,range.includeUpper);
      // The searcher sends docIds to the RangeFacetAccumulator which forwards
      // them to <code>collectRange()</code> in this class for collection.
      Query rangeQuery = new BooleanQuery.Builder()
          .add(q, Occur.MUST)
          .add(filter, Occur.FILTER)
          .build();

      ReductionDataCollection dataCol = collectionManager.newDataCollection();
      reductionData.put(range.toString(), dataCol);
      consumer.accept(new FacetValueQueryExecuter(dataCol, rangeQuery));
    }
  }

  public String getStart() {
    return start;
  }

  public String getEnd() {
    return end;
  }

  public EnumSet<FacetRangeInclude> getInclude() {
    return include;
  }

  public void setInclude(EnumSet<FacetRangeInclude> include) {
    this.include = include;
  }

  public List<String> getGaps() {
    return gaps;
  }

  public boolean isHardEnd() {
    return hardEnd;
  }

  public void setHardEnd(boolean hardEnd) {
    this.hardEnd = hardEnd;
  }

  public EnumSet<FacetRangeOther> getOthers() {
    return others;
  }

  public void setOthers(EnumSet<FacetRangeOther> others) {
    this.others = others;
  }

  public SchemaField getField() {
    return field;
  }
}