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

package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.DocValuesStats;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

/**
 * Models all of the information associated with a single {@link StatsParams#STATS_FIELD}
 * instance.
 *
 * @see StatsComponent
 */
public class StatsField {
  
  /**
   * An enumeration representing the sumer set of all possible stat values that can be computed.
   * Each of these enum values can be specified as a local param in a <code>stats.field</code> 
   * (eg: <code>stats.field={!min=true mean=true}my_field_name</code>) but not all enum values 
   * are valid for all field types (eg: <code>mean</code> is meaningless for String fields)
   *
   * @lucene.internal
   * @lucene.experimental
   */
  public static enum Stat {
    min(true),
    max(true),
    missing(true),
    sum(true),
    count(true),
    mean(false, sum, count),
    sumOfSquares(true),
    stddev(false, sum, count, sumOfSquares),
    calcdistinct(true);
    
    private final List<Stat> distribDeps;
    
    /**
     * Sole constructor for Stat enum values
     * @param deps the set of stat values, other then this one, which are a distributed 
     *        dependency and must be computed and returned by each individual shards in 
     *        order to compute <i>this</i> stat over the entire distributed result set.
     * @param selfDep indicates that when computing this stat across a distributed result 
     *        set, each shard must compute this stat <i>in addition to</i> any other 
     *        distributed dependences.
     * @see #getDistribDeps
     */
    Stat(boolean selfDep, Stat... deps) {
      distribDeps = new ArrayList<Stat>(deps.length+1);
      distribDeps.addAll(Arrays.asList(deps));
      if (selfDep) { 
        distribDeps.add(this);
      }
    }
    
    /**
     * Given a String, returns the corrisponding Stat enum value if any, otherwise returns null.
     */
    public static Stat forName(String paramKey) {
      try {
        return Stat.valueOf(paramKey);
      } catch (IllegalArgumentException e) {
        return null;
      }
    }
    
    /**
     * The stats that must be computed and returned by each shard involved in a distributed 
     * request in order to compute the overall value for this stat across the entire distributed 
     * result set.  A Stat instance may include itself in the <code>getDistribDeps()</code> result,
     * but that is not always the case.
     */
    public EnumSet<Stat> getDistribDeps() {
      return EnumSet.copyOf(this.distribDeps);
    }
  }

  /**
   * The set of stats computed by default when no localparams are used to specify explicit stats 
   */
  public final static Set<Stat> DEFAULT_STATS = Collections.<Stat>unmodifiableSet
    (EnumSet.of(Stat.min, Stat.max, Stat.missing, Stat.sum, Stat.count, Stat.mean, Stat.sumOfSquares, Stat.stddev));

  private final SolrIndexSearcher searcher;
  private final ResponseBuilder rb;
  private final String originalParam; // for error messages
  private final SolrParams localParams;
  private final ValueSource valueSource; // may be null if simple field stats
  private final SchemaField schemaField; // may be null if function/query stats
  private final String key;
  private final boolean  topLevelCalcDistinct;
  private final String[] facets;
  private final List<String> tagList;
  private final List<String> excludeTagList;
  private final EnumSet<Stat> statsToCalculate = EnumSet.noneOf(Stat.class);
  private final EnumSet<Stat> statsInResponse = EnumSet.noneOf(Stat.class);
  private final boolean isShard;

  /**
   * @param rb the current request/response
   * @param statsParam the raw {@link StatsParams#STATS_FIELD} string
   */
  public StatsField(ResponseBuilder rb, String statsParam) { 
    this.rb = rb;
    this.searcher = rb.req.getSearcher();
    this.originalParam = statsParam;

    SolrParams params = rb.req.getParams();
    try {
      isShard = params.getBool("isShard", false);
      SolrParams localParams = QueryParsing.getLocalParams(originalParam, params);
      if (null == localParams) {
        // simplest possible input: bare string (field name)
        ModifiableSolrParams customParams = new ModifiableSolrParams();
        customParams.add(QueryParsing.V, originalParam);
        localParams = customParams;
      }

      this.localParams = localParams;
      

      String parserName = localParams.get(QueryParsing.TYPE);
      SchemaField sf = null;
      ValueSource vs = null;

      if ( StringUtils.isBlank(parserName) ) {

        // basic request for field stats
        sf = searcher.getSchema().getField(localParams.get(QueryParsing.V));

      } else {
        // we have a non trivial request to compute stats over a query (or function)

        // NOTE we could use QParser.getParser(...) here, but that would redundently
        // reparse everything.  ( TODO: refactor a common method in QParser ?)
        QParserPlugin qplug = rb.req.getCore().getQueryPlugin(parserName);
        QParser qp =  qplug.createParser(localParams.get(QueryParsing.V), 
                                         localParams, params, rb.req);

        // figure out what type of query we are dealing, get the most direct ValueSource
        vs = extractValueSource(qp.parse());

        // if this ValueSource directly corrisponds to a SchemaField, act as if
        // we were asked to compute stats on it directly
        // ie:  "stats.field={!func key=foo}field(foo)" == "stats.field=foo"
        sf = extractSchemaField(vs, searcher.getSchema());
        if (null != sf) {
          vs = null;
        }
      }
      
      assert ( (null == vs) ^ (null == sf) ) : "exactly one of vs & sf must be null";
      
      this.schemaField = sf;
      this.valueSource = vs;

    } catch (SyntaxError e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to parse " + 
                              StatsParams.STATS_FIELD + ": " + originalParam + " due to: "
                              + e.getMessage(), e);
    }

    // allow explicit setting of the response key via localparams...
    this.key = localParams.get(CommonParams.OUTPUT_KEY, 
                               // default to the main param value...
                               localParams.get(CommonParams.VALUE, 
                                               // default to entire original param str.
                                               originalParam));

    this.topLevelCalcDistinct = null == schemaField
        ? params.getBool(StatsParams.STATS_CALC_DISTINCT, false) 
        : params.getFieldBool(schemaField.getName(), StatsParams.STATS_CALC_DISTINCT, false);
        
    populateStatsSets();
        
    String[] facets = params.getFieldParams(key, StatsParams.STATS_FACET);
    this.facets = (null == facets) ? new String[0] : facets;
    String tagStr = localParams.get(CommonParams.TAG);
    this.tagList = (null == tagStr)
        ? Collections.<String>emptyList()
        : StrUtils.splitSmart(tagStr,',');

    // figure out if we need a special base DocSet
    String excludeStr = localParams.get(CommonParams.EXCLUDE);
    this.excludeTagList = (null == excludeStr) 
      ? Collections.<String>emptyList()
      : StrUtils.splitSmart(excludeStr,',');

    assert ( (null == this.valueSource) ^ (null == this.schemaField) ) 
      : "exactly one of valueSource & schemaField must be null";
  }

  /**
   * Inspects a {@link Query} to see if it directly maps to a {@link ValueSource},
   * and if so returns it -- otherwise wraps it as needed.
   *
   * @param q Query whose scores we have been asked to compute stats of
   * @returns a ValueSource to use for computing the stats
   */
  private static ValueSource extractValueSource(Query q) {
    return (q instanceof FunctionQuery) ?
      // Common case: we're wrapping a func, so we can directly pull out ValueSource
      ((FunctionQuery) q).getValueSource() :
      // asked to compute stats over a query, wrap it up as a ValueSource
      new QueryValueSource(q, 0.0F);
  }

  /**
   * Inspects a {@link ValueSource} to see if it directly maps to a {@link SchemaField}, 
   * and if so returns it.
   *
   * @param vs ValueSource we've been asked to compute stats of
   * @param schema The Schema to use
   * @returns Corrisponding {@link SchemaField} or null if the ValueSource is more complex
   * @see FieldCacheSource
   */
  private static SchemaField extractSchemaField(ValueSource vs, IndexSchema schema) {
    if (vs instanceof FieldCacheSource) {
      String fieldName = ((FieldCacheSource)vs).getField();
      return schema.getField(fieldName);
    }
    return null;
  }

  /** 
   * The key to be used when refering to this {@link StatsField} instance in the 
   * response tp clients.
   */
  public String getOutputKey() {
    return key;
  }

  /**
   * Computes a base {@link DocSet} for the current request to be used
   * when computing global stats for the local index.
   *
   * This is typically the same as the main DocSet for the {@link ResponseBuilder}
   * unless {@link CommonParams#TAG tag}ged filter queries have been excluded using 
   * the {@link CommonParams#EXCLUDE ex} local param
   */
  public DocSet computeBaseDocSet() throws IOException {

    DocSet docs = rb.getResults().docSet;
    Map<?,?> tagMap = (Map<?,?>) rb.req.getContext().get("tags");

    if (excludeTagList.isEmpty() || null == tagMap) {
      // either the exclude list is empty, or there
      // aren't any tagged filters to exclude anyway.
      return docs;
    }

    IdentityHashMap<Query,Boolean> excludeSet = new IdentityHashMap<Query,Boolean>();
    for (String excludeTag : excludeTagList) {
      Object olst = tagMap.get(excludeTag);
      // tagMap has entries of List<String,List<QParser>>, but subject to change in the future
      if (!(olst instanceof Collection)) continue;
      for (Object o : (Collection<?>)olst) {
        if (!(o instanceof QParser)) continue;
        QParser qp = (QParser)o;
        try {
          excludeSet.put(qp.getQuery(), Boolean.TRUE);
        } catch (SyntaxError e) {
          // this shouldn't be possible since the request should have already
          // failed when attempting to execute the query, but just in case...
          throw new SolrException(ErrorCode.BAD_REQUEST, "Excluded query can't be parsed: " + 
                                  originalParam + " due to: " + e.getMessage(), e);
        }
      }
    }
    if (excludeSet.size() == 0) return docs;
    
    List<Query> qlist = new ArrayList<Query>();
    
    // add the base query
    if (!excludeSet.containsKey(rb.getQuery())) {
      qlist.add(rb.getQuery());
    }
    
    // add the filters
    if (rb.getFilters() != null) {
      for (Query q : rb.getFilters()) {
        if (!excludeSet.containsKey(q)) {
          qlist.add(q);
        }
      }
    }
    
    // get the new base docset for this facet
    return searcher.getDocSet(qlist);
  }

  /**
   * Computes the {@link StatsValues} for this {@link StatsField} relative to the 
   * specified {@link DocSet} 
   * @see #computeBaseDocSet
   */
  public StatsValues computeLocalStatsValues(DocSet base) throws IOException {

    if (statsToCalculate.isEmpty()) { 
      // perf optimization for the case where we compute nothing
      // ie: stats.field={!min=$domin}myfield&domin=false
      return StatsValuesFactory.createStatsValues(this);
    }

    if (null != schemaField 
        && (schemaField.multiValued() || schemaField.getType().multiValuedFieldCache())) {

      // TODO: should this also be used for single-valued string fields? (should work fine)
      return DocValuesStats.getCounts(searcher, this, base, facets);
    } else {
      // either a single valued field we pull from FieldCache, or an explicit
      // function ValueSource
      return computeLocalValueSourceStats(base);
    }
  }

  private StatsValues computeLocalValueSourceStats(DocSet base) throws IOException {

    IndexSchema schema = searcher.getSchema();

    final StatsValues allstats = StatsValuesFactory.createStatsValues(this);

    List<FieldFacetStats> facetStats = new ArrayList<>();
    for( String facetField : facets ) {
      SchemaField fsf = schema.getField(facetField);

      if ( fsf.multiValued()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Stats can only facet on single-valued fields, not: " + facetField );
      }

      facetStats.add(new FieldFacetStats(searcher, fsf, this));
    }

    final Iterator<LeafReaderContext> ctxIt = searcher.getIndexReader().leaves().iterator();
    LeafReaderContext ctx = null;
    for (DocIterator docsIt = base.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc()) {
        // advance
        do {
          ctx = ctxIt.next();
        } while (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc());
        assert doc >= ctx.docBase;

        // propagate the context among accumulators.
        allstats.setNextReader(ctx);
        for (FieldFacetStats f : facetStats) {
          f.setNextReader(ctx);
        }
      }

      // accumulate
      allstats.accumulate(doc - ctx.docBase);
      for (FieldFacetStats f : facetStats) {
        f.facet(doc - ctx.docBase);
      }
    }

    for (FieldFacetStats f : facetStats) {
      allstats.addFacet(f.name, f.facetStatsValues);
    }
    return allstats;
  }

  /**
   * The searcher that should be used for processing local stats
   * @see SolrQueryRequest#getSearcher
   */
  public SolrIndexSearcher getSearcher() {
    // see AbstractStatsValues.setNextReader

    return searcher;
  }

  /**
   * The {@link SchemaField} whose results these stats are computed over, may be null 
   * if the stats are computed over the results of a function or query
   *
   * @see #getValueSource
   */
  public SchemaField getSchemaField() {
    return schemaField;
  }

  /**
   * The {@link ValueSource} of a function or query whose results these stats are computed 
   * over, may be null if the stats are directly over a {@link SchemaField}
   *
   * @see #getValueSource
   */
  public ValueSource getValueSource() {
    return valueSource;
  }

  public List<String> getTagList() {
    return tagList;
  }

  public String toString() {
    return "StatsField<" + originalParam + ">";
  }


  /**
   * A helper method which inspects the {@link #localParams} associated with this StatsField, 
   * and uses them to populate the {@link #statsInResponse} and {@link #statsToCalculate} data 
   * structures
   */
  private void populateStatsSets() {
    
    boolean statSpecifiedByLocalParam = false;
    // local individual stat
    Iterator<String> itParams = localParams.getParameterNamesIterator();
    while (itParams.hasNext()) {
      String paramKey = itParams.next();
        Stat stat = Stat.forName(paramKey);
        if (stat != null) {
          statSpecifiedByLocalParam = true;
          // TODO: this isn't going to work for planned "non-boolean' stats - eg: SOLR-6350, SOLR-6968
          if (localParams.getBool(paramKey, false)) {
            statsInResponse.add(stat);
            statsToCalculate.addAll(stat.getDistribDeps());
          }
        }
    }
    
    // if no individual stat setting. 
    if ( ! statSpecifiedByLocalParam ) {
      statsInResponse.addAll(DEFAULT_STATS);
      for (Stat stat : statsInResponse) {
        statsToCalculate.addAll(stat.getDistribDeps());
      }
    }

    // calcDistinct has special "default" behavior using top level CalcDistinct param
    if (topLevelCalcDistinct && localParams.getBool(Stat.calcdistinct.toString(), true)) {
      statsInResponse.add(Stat.calcdistinct);
      statsToCalculate.addAll(Stat.calcdistinct.getDistribDeps());
    }
  }

  public boolean calculateStats(Stat stat) {
    return statsToCalculate.contains(stat);
  }
  
  public boolean includeInResponse(Stat stat) {
    if (isShard) {
      return statsToCalculate.contains(stat);
    }
   
    if (statsInResponse.contains(stat)) {
      return true;
    }
    return false;
  }


}
