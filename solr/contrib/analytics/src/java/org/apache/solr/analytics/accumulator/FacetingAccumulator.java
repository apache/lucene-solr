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

package org.apache.solr.analytics.accumulator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.solr.analytics.accumulator.facet.FacetValueAccumulator;
import org.apache.solr.analytics.accumulator.facet.FieldFacetAccumulator;
import org.apache.solr.analytics.accumulator.facet.QueryFacetAccumulator;
import org.apache.solr.analytics.accumulator.facet.RangeFacetAccumulator;
import org.apache.solr.analytics.expression.Expression;
import org.apache.solr.analytics.expression.ExpressionFactory;
import org.apache.solr.analytics.request.AnalyticsContentHandler;
import org.apache.solr.analytics.request.AnalyticsRequest;
import org.apache.solr.analytics.request.FieldFacetRequest;
import org.apache.solr.analytics.request.FieldFacetRequest.FacetSortSpecification;
import org.apache.solr.analytics.request.QueryFacetRequest;
import org.apache.solr.analytics.request.RangeFacetRequest;
import org.apache.solr.analytics.statistics.StatsCollector;
import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.analytics.util.RangeEndpointCalculator;
import org.apache.solr.analytics.util.RangeEndpointCalculator.FacetRange;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

import com.google.common.collect.Iterables;

/**
 * A <code>FacetingAccumulator</code> manages the StatsCollectors and Expressions for facets.
 */
public class FacetingAccumulator extends BasicAccumulator implements FacetValueAccumulator {
  public static final String MISSING_VALUE = "(MISSING)";
  protected boolean basicsAndFieldFacetsComputed;
  protected int leafNum;
  protected LeafReaderContext leaf;
  protected final AnalyticsRequest analyticsRequest;
  protected final Map<String,Map<String,Expression[]>> fieldFacetExpressions;
  protected final Map<String,Map<String,Expression[]>> rangeFacetExpressions;
  protected final Map<String,Map<String,Expression[]>> queryFacetExpressions;
  protected final Map<String,Map<String,StatsCollector[]>> fieldFacetCollectors;
  protected final Map<String,Map<String,StatsCollector[]>> rangeFacetCollectors;
  protected final Map<String,Map<String,StatsCollector[]>> queryFacetCollectors;
  protected final List<FieldFacetAccumulator> facetAccumulators;
  protected final Set<String> hiddenFieldFacets;
  /** the current value of this stat field */
  protected final SolrQueryRequest queryRequest;
  
  protected List<RangeFacetRequest> rangeFacets = null;
  protected List<QueryFacetRequest> queryFacets = null;
  
  protected long queryCount;
  
  public FacetingAccumulator(SolrIndexSearcher searcher, DocSet docs, AnalyticsRequest request, SolrQueryRequest queryRequest) throws IOException {
    // The parent Basic Accumulator keeps track of overall stats while
    // the Faceting Accumulator only manages the facet stats
    super(searcher, docs, request);
    this.analyticsRequest = request;
    this.queryRequest = queryRequest;
    basicsAndFieldFacetsComputed = false;
    List<FieldFacetRequest> fieldFreqs = request.getFieldFacets();
    List<RangeFacetRequest> rangeFreqs = request.getRangeFacets();
    List<QueryFacetRequest> queryFreqs = request.getQueryFacets();

    this.fieldFacetExpressions = new TreeMap<>();
    this.rangeFacetExpressions = new LinkedHashMap<>(rangeFreqs.size());
    this.queryFacetExpressions = new LinkedHashMap<>(queryFreqs.size());
    this.fieldFacetCollectors = new LinkedHashMap<>(fieldFreqs.size());
    this.rangeFacetCollectors = new LinkedHashMap<>(rangeFreqs.size());
    this.queryFacetCollectors = new LinkedHashMap<>(queryFreqs.size());
    this.facetAccumulators = new ArrayList<>();
    this.hiddenFieldFacets = new HashSet<>();
    
    /**
     * For each field facet request add a bucket to the {@link Expression} map and {@link StatsCollector} map.
     * Field facets are computed during the initial collection of documents, therefore
     * the FieldFacetAccumulators are created initially.
     */
    for( FieldFacetRequest freq : fieldFreqs ){
      final FieldFacetRequest fr = (FieldFacetRequest) freq;
      if (fr.isHidden()) {
        hiddenFieldFacets.add(fr.getName());
      }
      final SchemaField ff = fr.getField();
      final FieldFacetAccumulator facc = FieldFacetAccumulator.create(searcher, this, ff);
      facetAccumulators.add(facc);
      fieldFacetExpressions.put(freq.getName(), new TreeMap<String, Expression[]>() );
      fieldFacetCollectors.put(freq.getName(), new TreeMap<String,StatsCollector[]>());
    }
    /**
     * For each range and query facet request add a bucket to the corresponding
     * {@link Expression} map and {@link StatsCollector} map.
     * Range and Query Facets are computed in the post processing, so the accumulators
     * are not created initially.
     */
    for( RangeFacetRequest freq : rangeFreqs ){
      if( rangeFacets == null ) rangeFacets = new ArrayList<>();
      rangeFacets.add(freq);
      rangeFacetExpressions.put(freq.getName(), new LinkedHashMap<String,Expression[]>() );
      rangeFacetCollectors.put(freq.getName(), new LinkedHashMap<String,StatsCollector[]>());
    }
    for( QueryFacetRequest freq : queryFreqs ){
      if( queryFacets == null ) queryFacets = new ArrayList<>();
      queryFacets.add(freq);
      queryFacetExpressions.put(freq.getName(), new LinkedHashMap<String,Expression[]>() );
      queryFacetCollectors.put(freq.getName(), new LinkedHashMap<String,StatsCollector[]>());
    }
    this.queryCount = 0l;
  }
  
  public static FacetingAccumulator create(SolrIndexSearcher searcher, DocSet docs, AnalyticsRequest request, SolrQueryRequest queryRequest) throws IOException {
    return new FacetingAccumulator(searcher,docs,request,queryRequest);
  }

  /**
   * Update the readers for the {@link BasicAccumulator}, field facets and field facet {@link StatsCollector}s.
   * @param context The context to read documents from.
   * @throws IOException if there is an error setting the next reader
   */
  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    super.doSetNextReader(context);
    for( Map<String,StatsCollector[]> valueList : fieldFacetCollectors.values() ){
      for (StatsCollector[] statsCollectorList : valueList.values()) {
        for (StatsCollector statsCollector : statsCollectorList) {
          statsCollector.setNextReader(context);
        }
      }
    }
    for (FieldFacetAccumulator fa : facetAccumulators) {
      fa.getLeafCollector(context);
    }
  }
  
  /**
   * Updates the reader for all of the range facet {@link StatsCollector}s.
   * @param context The context to read documents from.
   * @throws IOException if there is an error setting the next reader
   */
  public void setRangeStatsCollectorReaders(LeafReaderContext context) throws IOException {
    super.getLeafCollector(context);
    for( Map<String,StatsCollector[]> rangeList : rangeFacetCollectors.values() ){
      for (StatsCollector[] statsCollectorList : rangeList.values()) {
        for (StatsCollector statsCollector : statsCollectorList) {
          statsCollector.setNextReader(context);
        }
      }
    }
  }

  
  /**
   * Updates the reader for all of the query facet {@link StatsCollector}s.
   * @param context The context to read documents from.
   * @throws IOException if there is an error setting the next reader
   */
  public void setQueryStatsCollectorReaders(LeafReaderContext context) throws IOException {
    super.getLeafCollector(context);
    for( Map<String,StatsCollector[]> queryList : queryFacetCollectors.values() ){
      for (StatsCollector[] statsCollectorList : queryList.values()) {
        for (StatsCollector statsCollector : statsCollectorList) {
          statsCollector.setNextReader(context);
        }
      }
    }
  }

  /**
   * Called from Analytics stats, adds documents to the field 
   * facets and the super {@link BasicAccumulator}.
   */
  @Override
  public void collect(int doc) throws IOException {
    for( FieldFacetAccumulator fa : facetAccumulators ){
      fa.collect(doc);
    }
    super.collect(doc);
  }
  
  /**
   * Given a document, fieldFacet field and facetValue, adds the document to the
   * {@link StatsCollector}s held in the bucket corresponding to the fieldFacet field and facetValue.
   * Called during initial document collection.
   */
  @Override
  public void collectField(int doc, String facetField, String facetValue) throws IOException {
    Map<String,StatsCollector[]> map = fieldFacetCollectors.get(facetField);
    StatsCollector[] statsCollectors = map.get(facetValue);
    // If the facetValue has not been seen yet, a StatsCollector array is
    // created and associated with that bucket.
    if( statsCollectors == null ){
      statsCollectors = statsCollectorArraySupplier.get();
      map.put(facetValue,statsCollectors);
      fieldFacetExpressions.get(facetField).put(facetValue,makeExpressions(statsCollectors));
      for (StatsCollector statsCollector : statsCollectors) {
        statsCollector.setNextReader(context);
      }
    }
    for (StatsCollector statsCollector : statsCollectors) {
      statsCollector.collect(doc);
    }
  }
  
  /**
   * Given a document, rangeFacet field and range, adds the document to the
   * {@link StatsCollector}s held in the bucket corresponding to the rangeFacet field and range.
   * Called during post processing.
   */
  @Override
  public void collectRange(int doc, String facetField, String range) throws IOException {
    Map<String,StatsCollector[]> map = rangeFacetCollectors.get(facetField);
    StatsCollector[] statsCollectors = map.get(range);
    // If the range has not been seen yet, a StatsCollector array is
    // created and associated with that bucket.
    if( statsCollectors == null ){
      statsCollectors = statsCollectorArraySupplier.get();
      map.put(range,statsCollectors);
      rangeFacetExpressions.get(facetField).put(range,makeExpressions(statsCollectors));
      for (StatsCollector statsCollector : statsCollectors) {
        statsCollector.setNextReader(context);
      }
    }
    for (StatsCollector statsCollector : statsCollectors) {
      statsCollector.collect(doc);
    }
  }
  
  /**
   * Given a document, queryFacet name and query, adds the document to the
   * {@link StatsCollector}s held in the bucket corresponding to the queryFacet name and query.
   * Called during post processing.
   */
  @Override
  public void collectQuery(int doc, String facetName, String query) throws IOException {
    Map<String,StatsCollector[]> map = queryFacetCollectors.get(facetName);
    StatsCollector[] statsCollectors = map.get(query);
    // If the query has not been seen yet, a StatsCollector array is
    // created and associated with that bucket.
    if( statsCollectors == null ){
      statsCollectors = statsCollectorArraySupplier.get();
      map.put(query,statsCollectors);
      queryFacetExpressions.get(facetName).put(query,makeExpressions(statsCollectors));
      for (StatsCollector statsCollector : statsCollectors) {
        statsCollector.setNextReader(context);
      }
    }
    for (StatsCollector statsCollector : statsCollectors) {
      statsCollector.collect(doc);
    }
  }

  /**
   * A comparator to compare expression values for field facet sorting.
   */
  public static class EntryComparator implements Comparator<Entry<String,Expression[]>> {
    private final Comparator<Expression> comp;
    private final int comparatorExpressionPlace;
   
    public EntryComparator(Comparator<Expression> comp, int comparatorExpressionPlace) {
      this.comp = comp;
      this.comparatorExpressionPlace = comparatorExpressionPlace;
    }

    @Override
    public int compare(Entry<String,Expression[]> o1, Entry<String,Expression[]> o2) {
      return comp.compare(o1.getValue()[comparatorExpressionPlace], o2.getValue()[comparatorExpressionPlace]);
    }
  }
  
  /**
   * Finalizes the statistics within the each facet bucket before exporting;
   */
  @Override
  public void compute() {
    if (!basicsAndFieldFacetsComputed) {
      super.compute();
      for( Map<String, StatsCollector[]> f : fieldFacetCollectors.values() ){
        for( StatsCollector[] arr : f.values() ){
          for( StatsCollector b : arr ){
            b.compute();
          }
        }
      }
      basicsAndFieldFacetsComputed = true;
    }
  }
  
  /**
   * Finalizes the statistics within the a specific query facet before exporting;
   */
  public void computeQueryFacet(String facet) {
    Map<String, StatsCollector[]> f = queryFacetCollectors.get(facet);
    for( StatsCollector[] arr : f.values() ){
      for( StatsCollector b : arr ){
        b.compute();
      }
    }
  }
  
  /**
   * Finalizes the statistics within the a specific range facet before exporting;
   */
  public void computeRangeFacet(String facet) {
    Map<String, StatsCollector[]> f = rangeFacetCollectors.get(facet);
    for( StatsCollector[] arr : f.values() ){
      for( StatsCollector b : arr ){
        b.compute();
      }
    }
  }
  
  /**
   * Returns the value of an expression to use in a range or query facet.
   * @param expressionName the name of the expression
   * @param fieldFacet the facet field
   * @param facetValue the facet value
   * @return String String representation of pivot value
   */
  @SuppressWarnings({ "deprecation", "rawtypes" })
  public String getResult(String expressionName, String fieldFacet, String facetValue) {
    if (facetValue.contains(AnalyticsParams.RESULT) && !facetValue.contains(AnalyticsParams.QUERY_RESULT)) {
      try {
        String[] pivotStr = ExpressionFactory.getArguments(facetValue.substring(facetValue.indexOf('(')+1,facetValue.lastIndexOf(')')).trim());
        if (pivotStr.length==1) {
          facetValue = getResult(pivotStr[0]);
        } else if (pivotStr.length==3) {
          facetValue = getResult(pivotStr[0],pivotStr[1],pivotStr[2]);
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+facetValue+" has an invalid amount of arguments.");
        }
      } catch (IndexOutOfBoundsException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+facetValue+" is invalid. Lacks parentheses.",e);
      }
    } 
    if (fieldFacetExpressions.get(fieldFacet)!=null) {
      Expression[] facetExpressions = fieldFacetExpressions.get(fieldFacet).get(facetValue);
      for (int count = 0; count < expressionNames.length; count++) {
        if (expressionName.equals(expressionNames[count])) {
          Comparable value = facetExpressions[count].getValue();
          if (value.getClass().equals(Date.class)) {
            return TrieDateField.formatExternal((Date)value);
          } else {
            return value.toString();
          }
        }
      }
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"Field Facet Pivot expression "+expressionName+" not found.");
  }
  
  /**
   * Returns the value of an expression to use in a range or query facet.
   * @param currentFacet the name of the current facet
   * @param expressionName the name of the expression
   * @param queryFacet the facet query
   * @param facetValue the field value
   * @return String String representation of pivot value
   */
  @SuppressWarnings({ "deprecation", "rawtypes" })
  public String getQueryResult(String currentFacet, String expressionName, String queryFacet, String facetValue) {
    if (facetValue.contains(AnalyticsParams.RESULT) && !facetValue.contains(AnalyticsParams.QUERY_RESULT)) {
      try {
        String[] pivotStr = ExpressionFactory.getArguments(facetValue.substring(facetValue.indexOf('(')+1,facetValue.lastIndexOf(')')).trim());
        if (pivotStr.length==1) {
          facetValue = getResult(pivotStr[0]);
        } else if (pivotStr.length==3) {
          facetValue = getResult(pivotStr[0],pivotStr[1],pivotStr[2]);
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+facetValue+" has an invalid amount of arguments.");
        }
      } catch (IndexOutOfBoundsException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"Result request "+facetValue+" is invalid. Lacks parentheses.",e);
      }
    } 
    if (facetValue.contains(AnalyticsParams.QUERY_RESULT)) {
      try {
        String[] pivotStr = ExpressionFactory.getArguments(facetValue.substring(facetValue.indexOf('(')+1,facetValue.lastIndexOf(')')).trim());
        if (pivotStr.length==1) {
          facetValue = getResult(pivotStr[0]);
        } else if (pivotStr.length==3) {
          facetValue = getQueryResult(currentFacet,pivotStr[0],pivotStr[1],pivotStr[2]);
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST,"Result request "+facetValue+" has an invalid amount of arguments.");
        }
      } catch (IndexOutOfBoundsException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"Result request "+facetValue+" is invalid. Lacks parentheses.",e);
      }
    } 
    if (queryFacetExpressions.get(queryFacet)!=null) {
      Expression[] facetExpressions = queryFacetExpressions.get(queryFacet).get(facetValue);
      for (int count = 0; count < expressionNames.length; count++) {
        if (expressionName.equals(expressionNames[count])) {
          Comparable value = facetExpressions[count].getValue();
          if (value.getClass().equals(Date.class)) {
            return TrieDateField.formatExternal((Date)value);
          } else {
            return value.toString();
          }
        }
      }
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"Field Facet Pivot expression "+expressionName+" not found.");
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public NamedList<?> export() {
    final NamedList<Object> base = (NamedList<Object>)super.export();
    NamedList<NamedList<?>> facetList = new NamedList<>();
    
    // Add the field facet buckets to the output
    base.add("fieldFacets",facetList);
    for( FieldFacetRequest freq : request.getFieldFacets() ){
      final String name = freq.getName();
      if (hiddenFieldFacets.contains(name)) {
        continue;
      }
      final Map<String,Expression[]> buckets = fieldFacetExpressions.get(name);
      final NamedList<Object> bucketBase = new NamedList<>();

      Iterable<Entry<String,Expression[]>> iter = buckets.entrySet();
      
      final FieldFacetRequest fr = (FieldFacetRequest) freq;
     
      final FacetSortSpecification sort = fr.getSort();
      final int limit = fr.getLimit();
      final int offset = fr.getOffset();
      final boolean showMissing = fr.showsMissing();
      if (!showMissing) {
        buckets.remove(MISSING_VALUE);
      }
      // Sorting the buckets if a sort specification is provided
      if( sort != null && buckets.values().iterator().hasNext()){
        int sortPlace = Arrays.binarySearch(expressionNames, sort.getStatistic());
        final Expression first = buckets.values().iterator().next()[sortPlace];
        final Comparator<Expression> comp = (Comparator<Expression>) first.comparator(sort.getDirection());
        
        final List<Entry<String,Expression[]>> sorted = new ArrayList<>(buckets.size());
        Iterables.addAll(sorted, iter);
        Collections.sort(sorted, new EntryComparator(comp,sortPlace));
        iter = sorted;
      }
      // apply the limit
      if( limit > AnalyticsContentHandler.DEFAULT_FACET_LIMIT ){
        if( offset > 0 ){
          iter = Iterables.skip(iter, offset);
        }
        iter = Iterables.limit(iter, limit);
      }
      
      // Export each expression in the bucket.
      for( Entry<String,Expression[]> bucket : iter ){
        bucketBase.add(bucket.getKey(),export(bucket.getValue()));
      }
      
      facetList.add(name, bucketBase);
    }

    // Add the range facet buckets to the output
    facetList = new NamedList<>();
    base.add("rangeFacets",facetList);
    for( RangeFacetRequest freq : request.getRangeFacets() ){
      final String name = freq.getName();
      final Map<String,Expression[]> buckets = rangeFacetExpressions.get(name);
      final NamedList<Object> bucketBase = new NamedList<>();

      Iterable<Entry<String,Expression[]>> iter = buckets.entrySet();
      
      for( Entry<String,Expression[]> bucket : iter ){
        bucketBase.add(bucket.getKey(),export(bucket.getValue()));
      }
      
      facetList.add(name, bucketBase);
    }
    
    // Add the query facet buckets to the output
    facetList = new NamedList<>();
    base.add("queryFacets",facetList);
    for( QueryFacetRequest freq : request.getQueryFacets() ){
      final String name = freq.getName();
      final Map<String,Expression[]> buckets = queryFacetExpressions.get(name);
      final NamedList<Object> bucketBase = new NamedList<>();

      Iterable<Entry<String,Expression[]>> iter = buckets.entrySet();
      
      for( Entry<String,Expression[]> bucket : iter ){
        bucketBase.add(bucket.getKey(),export(bucket.getValue()));
      }
      
      facetList.add(name, bucketBase);
    }

    return base;
  }
  
  /**
   * Exports a list of expressions as a NamedList
   * @param expressionArr an array of expressions
   * @return named list of expressions
   */
  public NamedList<?> export(Expression[] expressionArr) {
    NamedList<Object> base = new NamedList<>();
    for (int count = 0; count < expressionArr.length; count++) {
      if (!hiddenExpressions.contains(expressionNames[count])) {
        base.add(expressionNames[count], expressionArr[count].getValue());
      }
    }
    return base;
  }

  /**
   * Processes the query and range facets.
   * Must be called if range and/or query facets are supported.
   */
  @Override
  public void postProcess() throws IOException {
    super.compute();
    for( Map<String, StatsCollector[]> f : fieldFacetCollectors.values() ){
      for( StatsCollector[] arr : f.values() ){
        for( StatsCollector b : arr ){
          b.compute();
        }
      }
    }
    basicsAndFieldFacetsComputed = true;
    final Filter filter = docs.getTopFilter();
    if( rangeFacets != null ){
      processRangeFacets(filter); 
    }
    if( queryFacets != null ){
      processQueryFacets(filter); 
    }
  }
  
  /**
   * Initiates the collecting of query facets
   * @param filter the base filter to work against
   * @throws IOException if searching failed
   */
  public void processQueryFacets(final Filter filter) throws IOException {
    for( QueryFacetRequest qfr : queryFacets ){
      for( String query : qfr.getQueries() ){
        if (query.contains(AnalyticsParams.RESULT) && !query.contains(AnalyticsParams.QUERY_RESULT)) {
          try {
            String[] pivotStr = ExpressionFactory.getArguments(query.substring(query.indexOf('(')+1,query.lastIndexOf(')')).trim());
            if (pivotStr.length==1) {
              query = getResult(pivotStr[0]);
            } else if (pivotStr.length==3) {
              query = getResult(pivotStr[0],pivotStr[1],pivotStr[2]);
            } else {
              throw new SolrException(ErrorCode.BAD_REQUEST,"Result request "+query+" has an invalid amount of arguments.");
            }
          } catch (IndexOutOfBoundsException e) {
            throw new SolrException(ErrorCode.BAD_REQUEST,"Result request "+query+" is invalid. Lacks parentheses.",e);
          }
        } else if (query.contains(AnalyticsParams.QUERY_RESULT)) {
          try {
            String[] pivotStr = ExpressionFactory.getArguments(query.substring(query.indexOf('(')+1,query.lastIndexOf(')')).trim());
            if (pivotStr.length==3) {
              query = getQueryResult(qfr.getName(),pivotStr[0],pivotStr[1],pivotStr[2]);
            } else {
              throw new SolrException(ErrorCode.BAD_REQUEST,"Result request "+query+" has an invalid amount of arguments.");
            }
          } catch (IndexOutOfBoundsException e) {
            throw new SolrException(ErrorCode.BAD_REQUEST,"Result request "+query+" is invalid. Lacks parentheses.",e);
          }
        }
        QueryFacetAccumulator qAcc = new QueryFacetAccumulator(this,qfr.getName(),query);
        final Query q;
        try {
          q = QParser.getParser(query, null, queryRequest).getQuery();
        } catch( SyntaxError e ){
          throw new SolrException(ErrorCode.BAD_REQUEST,"Invalid query '"+query+"'",e);
        }
        // The searcher sends docIds to the QueryFacetAccumulator which forwards
        // them to <code>collectQuery()</code> in this class for collection.
        searcher.search(q, filter, qAcc);
        computeQueryFacet(qfr.getName());
        queryCount++;
      }
    }
  }
  
  @Override
  public long getNumQueries() {
    return queryCount;
  }

  /**
   * Initiates the collecting of range facets
   * @param filter the base filter to use
   * @throws IOException if searching fails
   */
  public void processRangeFacets(final Filter filter) throws IOException {
    for( RangeFacetRequest rfr : rangeFacets ){
      String[] pivotStr;
      String start = rfr.getStart();
      if (start.contains(AnalyticsParams.QUERY_RESULT)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"Query result requests can not be used in Range Facets");
      } else if (start.contains(AnalyticsParams.RESULT)) {
        try {
          pivotStr = ExpressionFactory.getArguments(start.substring(start.indexOf('(')+1,start.indexOf(')')).trim());
          if (pivotStr.length==1) {
            rfr.setStart(getResult(pivotStr[0]));
          } else if (pivotStr.length==3) {
            rfr.setStart(getResult(pivotStr[0],pivotStr[1],pivotStr[2]));
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+start+" has an invalid amount of arguments.");
          }
        } catch (IndexOutOfBoundsException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+start+" is invalid. Lacks parentheses.",e);
        }
      }
      String end = rfr.getEnd();
      if (end.contains(AnalyticsParams.QUERY_RESULT)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Query result requests can not be used in Range Facets");
      } else if (end.contains(AnalyticsParams.RESULT)) {
        try {
          pivotStr = ExpressionFactory.getArguments(end.substring(end.indexOf('(')+1,end.indexOf(')')).trim());
          if (pivotStr.length==1) {
            rfr.setEnd(getResult(pivotStr[0]));
          } else if (pivotStr.length==3) {
            rfr.setEnd(getResult(pivotStr[0],pivotStr[1],pivotStr[2]));
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+end+" has an invalid amount of arguments.");
          }
        } catch (IndexOutOfBoundsException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+end+" is invalid. Lacks parentheses.",e);
        }
      }
      String[] gaps = rfr.getGaps();
      for (int count = 0; count<gaps.length; count++){
        String gap = gaps[count];
        if (gap.contains(AnalyticsParams.QUERY_RESULT)) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Query result requests can not be used in Range Facets");
        } else if (gap.contains(AnalyticsParams.RESULT)) {
          try {
            pivotStr = ExpressionFactory.getArguments(gap.substring(gap.indexOf('(')+1,gap.indexOf(')')).trim());
            if (pivotStr.length==1) {
              gaps[count]=getResult(pivotStr[0]);
            } else if (pivotStr.length==3) {
              gaps[count]=getResult(pivotStr[0],pivotStr[1],pivotStr[2]);
            } else {
              throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+gap+" has an invalid amount of arguments.");
            }
          } catch (IndexOutOfBoundsException e) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Result request "+gap+" is invalid. Lacks parentheses.",e);
          }
        }
      }
      // Computes the end points of the ranges in the rangeFacet
      final RangeEndpointCalculator<? extends Comparable<?>> rec = RangeEndpointCalculator.create(rfr);
      final SchemaField sf = rfr.getField();
      
      // Create a rangeFacetAccumulator for each range and 
      // collect the documents for that range.
      for( FacetRange range : rec.getRanges() ){
        final String upper;
        final String lower;
        String facetValue = "";
        if( range.lower == null ){
          facetValue = "(*";
          lower = null;
        } else {
          lower = range.lower;
          facetValue = ((range.includeLower)?"[":"(") + range.lower;
        }
        facetValue+=" TO ";
        if( range.upper == null ){
          upper = null;
          facetValue += "*)";
        } else {
          upper = range.upper;
          facetValue += range.upper + ((range.includeUpper)?"]":")");
        }
        
        Query q = sf.getType().getRangeQuery(null, sf, lower, upper, range.includeLower,range.includeUpper);
        RangeFacetAccumulator rAcc = new RangeFacetAccumulator(this,rfr.getName(),facetValue);
        // The searcher sends docIds to the RangeFacetAccumulator which forwards
        // them to <code>collectRange()</code> in this class for collection.
        searcher.search(q, filter, rAcc);
        computeRangeFacet(sf.getName());
      }
    }
  }
}
