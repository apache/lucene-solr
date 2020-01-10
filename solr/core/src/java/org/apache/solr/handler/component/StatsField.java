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

import org.apache.commons.lang3.StringUtils;
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
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.hll.HLL;
import org.apache.solr.util.hll.HLLType;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

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
    distinctValues(true),
    countDistinct(false, distinctValues),
    percentiles(true){
      /** special for percentiles **/
      boolean parseParams(StatsField sf) {
        String percentileParas = sf.localParams.get(this.name());
        if (percentileParas != null) {
          List<Double> percentiles = new ArrayList<Double>();
          try {
            for (String percentile : StrUtils.splitSmart(percentileParas, ',')) {
              percentiles.add(Double.parseDouble(percentile));
            }
            if (!percentiles.isEmpty()) {
              sf.percentilesList.addAll(percentiles);
              sf.tdigestCompression = sf.localParams.getDouble("tdigestCompression", 
                                                               sf.tdigestCompression);
              return true;
            }
          } catch (NumberFormatException e) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to parse "
                + StatsParams.STATS_FIELD + " local params: " + sf.localParams + " due to: "
                + e.getMessage(), e);
          }

        }
        return false;
      }
    },
    cardinality(true) { 
      /** special for percentiles **/
      boolean parseParams(StatsField sf) {
        try {
          sf.hllOpts = HllOptions.parseHllOptions(sf.localParams, sf.schemaField);
          return (null != sf.hllOpts);
        } catch (Exception e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to parse "
              + StatsParams.STATS_FIELD + " local params: " + sf.localParams + " due to: "
              + e.getMessage(), e);
        }
      }
    };

    private final List<Stat> distribDeps;
    
    /**
     * Sole constructor for Stat enum values
     * @param deps the set of stat values, other then this one, which are a distributed 
     *        dependency and must be computed and returned by each individual shards in 
     *        order to compute <i>this</i> stat over the entire distributed result set.
     * @param selfDep indicates that when computing this stat across a distributed result 
     *        set, each shard must compute this stat <i>in addition to</i> any other 
     *        distributed dependencies.
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
     * Given a String, returns the corresponding Stat enum value if any, otherwise returns null.
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
    
    /** 
     * Called when the name of a stat is found as a local param on this {@link StatsField}
     * @return true if the user is requesting this stat, else false
     */
    boolean parseParams(StatsField sf) {
      return sf.localParams.getBool(this.name(), false);
    }
    
  }

  /**
   * the equivalent stats if "calcdistinct" is specified
   * @see Stat#countDistinct
   * @see Stat#distinctValues
   */
  private static final EnumSet<Stat> CALCDISTINCT_PSUEDO_STAT = EnumSet.of(Stat.countDistinct, Stat.distinctValues);

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
  private final List<Double> percentilesList= new ArrayList<Double>();
  private final boolean isShard;
  
  private double tdigestCompression = 100.0D;
  private HllOptions hllOpts;
  
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

        // NOTE we could use QParser.getParser(...) here, but that would redundantly
        // reparse everything.  ( TODO: refactor a common method in QParser ?)
        QParserPlugin qplug = rb.req.getCore().getQueryPlugin(parserName);
        if (qplug == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid query parser '" + parserName +
              (originalParam == null? "'": "' for query '" + originalParam + "'"));
        }
        QParser qp = qplug.createParser(localParams.get(QueryParsing.V),
                                         localParams, params, rb.req);

        // figure out what type of query we are dealing, get the most direct ValueSource
        vs = extractValueSource(qp.parse());

        // if this ValueSource directly corresponds to a SchemaField, act as if
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

    if (null != schemaField && !schemaField.getType().isPointField()
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
        if (stat.parseParams(this)) {
          statsInResponse.add(stat);
        }
      }
    }

    // if no individual stat setting use the default set
    if ( ! ( statSpecifiedByLocalParam
             // calcdistinct (as a local param) is a psuedo-stat, prevents default set
             || localParams.getBool("calcdistinct", false) ) ) {
      statsInResponse.addAll(DEFAULT_STATS);
    }

    // calcDistinct is a psuedo-stat with optional top level param default behavior
    // if not overridden by the specific individual stats
    if (localParams.getBool("calcdistinct", topLevelCalcDistinct)) {
      for (Stat stat : CALCDISTINCT_PSUEDO_STAT) {
        // assume true, but don't include if specific stat overrides
        if (localParams.getBool(stat.name(), true)) {
          statsInResponse.add(stat);
        }
      }
    }

    for (Stat stat : statsInResponse) {
      statsToCalculate.addAll(stat.getDistribDeps());
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

  public List<Double> getPercentilesList() {
    return percentilesList;
  }
  
  public boolean getIsShard() {
    return isShard;
  }
  
  public double getTdigestCompression() {
    return tdigestCompression;
  }

  public HllOptions getHllOptions() {
    return hllOpts;
  }

  /**
   * Helper Struct for parsing and encapsulating all of the options relaed to building a {@link HLL}
   *
   * @see Stat#cardinality
   * @lucene.internal
   */
  public static final class HllOptions {
    final HashFunction hasher;
    
    // NOTE: this explanation linked to from the java-hll jdocs...
    // https://github.com/aggregateknowledge/postgresql-hll/blob/master/README.markdown#explanation-of-parameters-and-tuning
    // ..if i'm understanding the regwidth chart correctly, a value of 6 should be a enough
    // to support any max cardinality given that we're always dealing with hashes and 
    // the cardinality of the set of all long values is 2**64 == 1.9e19
    //
    // But i guess that assumes a *perfect* hash and high log2m? ... if the hash algo is imperfect 
    // and/or log2m is low (ie: user is less concerned about accuracy), then many diff hash values 
    // might fall in the same register (ie: bucket) and having a wider register to count more of 
    // them may be useful

    final int log2m;  
    final int regwidth;
    
    final static String ERR = "cardinality must be specified as 'true' (for default tunning) or decimal number between 0 and 1 to adjust accuracy vs memory usage (large number is more memory and more accuracy)";

    private HllOptions(int log2m, int regwidth, HashFunction hasher) {
      this.log2m = log2m;
      this.regwidth = regwidth;
      this.hasher = hasher;
    }
    /** 
     * Creates an HllOptions based on the (local) params specified (if appropriate).
     *
     * @param localParams the LocalParams for this {@link StatsField}
     * @param field the field corresponding to this {@link StatsField}, may be null if these stats are over a value source
     * @return the {@link HllOptions} to use based on the params, or null if no {@link HLL} should be computed
     * @throws SolrException if there are invalid options
     */
    public static HllOptions parseHllOptions(SolrParams localParams, SchemaField field) 
      throws SolrException {

      String cardinalityOpt = localParams.get(Stat.cardinality.name());
      if (StringUtils.isBlank(cardinalityOpt)) {
        return null;
      }

      final NumberType hashableNumType = getHashableNumericType(field);

      // some sane defaults
      int log2m = 13;   // roughly equivalent to "cardinality='0.33'"
      int regwidth = 6; // with decent hash, this is plenty for all valid long hashes

      if (NumberType.FLOAT.equals(hashableNumType) || NumberType.INTEGER.equals(hashableNumType)) {
        // for 32bit values, we can adjust our default regwidth down a bit
        regwidth--;

        // NOTE: EnumField uses LegacyNumericType.INT, and in theory we could be super conservative
        // with it, but there's no point - just let the EXPLICIT HLL handle it
      }

      // TODO: we could attempt additional reductions in the default regwidth based on index
      // statistics -- but thta doesn't seem worth the effort.  for tiny indexes, the 
      // EXPLICIT and SPARSE HLL representations have us nicely covered, and in general we don't 
      // want to be too aggresive about lowering regwidth or we could really poor results if 
      // log2m is also low and  there is heavy hashkey collision

      try {
        // NFE will short out here if it's not a number
        final double accuracyOpt = Double.parseDouble(cardinalityOpt);

        // if a float between 0 and 1 is specified, treat it as a prefrence of accuracy
        // - 0 means accuracy is not a concern, save RAM
        // - 1 means be as accurate as possible, using as much RAM as needed.

        if (accuracyOpt < 0D || 1.0D < accuracyOpt) {
          throw new SolrException(ErrorCode.BAD_REQUEST, ERR);
        }

        // use accuracyOpt as a scaling factor between min & max legal log2m values
        log2m = HLL.MINIMUM_LOG2M_PARAM
          + (int) Math.round(accuracyOpt * (HLL.MAXIMUM_LOG2M_PARAM - HLL.MINIMUM_LOG2M_PARAM));

        // use accuracyOpt as a scaling factor for regwidth as well, BUT...
        // be more conservative -- HLL.MIN_REGWIDTH_PARAM is too absurdly low to be useful
        // use previously computed (hashableNumType) default regwidth -1 as lower bound for scaling
        final int MIN_HUERISTIC_REGWIDTH = regwidth-1;
        regwidth = MIN_HUERISTIC_REGWIDTH
          + (int) Math.round(accuracyOpt * (HLL.MAXIMUM_REGWIDTH_PARAM - MIN_HUERISTIC_REGWIDTH));

      } catch (NumberFormatException nfe) {
        // param value isn't a number -- let's check for simple true/false
        if (! localParams.getBool(Stat.cardinality.name(), false)) {
          return null;
        }
      }

      // let explicit params override both the default and/or any accuracy specification
      log2m = localParams.getInt("hllLog2m", log2m);
      regwidth = localParams.getInt("hllRegwidth", regwidth);

      // validate legal values
      if (log2m < HLL.MINIMUM_LOG2M_PARAM || HLL.MAXIMUM_LOG2M_PARAM < log2m) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "hllLog2m must be at least " + 
                                HLL.MINIMUM_LOG2M_PARAM + " and at most " + HLL.MAXIMUM_LOG2M_PARAM
                                + " (" + log2m +")");
      }
      if (regwidth < HLL.MINIMUM_REGWIDTH_PARAM || HLL.MAXIMUM_REGWIDTH_PARAM < regwidth) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "hllRegwidth must be at least " + 
                                HLL.MINIMUM_REGWIDTH_PARAM + " and at most " + HLL.MAXIMUM_REGWIDTH_PARAM);
      }
      
      HashFunction hasher = localParams.getBool("hllPreHashed", false) ? null : Hashing.murmur3_128();

      if (null == hasher) {
        // if this is a function, or a non Long field, pre-hashed is invalid
        // NOTE: we ignore hashableNumType - it's LONG for non numerics like Strings
        if (null == field || !(NumberType.LONG.equals(field.getType().getNumberType()) || NumberType.DATE.equals(field.getType().getNumberType()))) { 
          throw new SolrException(ErrorCode.BAD_REQUEST, "hllPreHashed is only supported with Long based fields");
        }
      }

      // if we're still here, then we need an HLL...
      return new HllOptions(log2m, regwidth, hasher);
    }
    /** @see HLL */
    public int getLog2m() {
      return log2m;
    }
    /** @see HLL */
    public int getRegwidth() {
      return regwidth;
    }
    /** May be null if user has indicated that field values are pre-hashed */
    public HashFunction getHasher() {
      return hasher;
    }
    public HLL newHLL() {
      // Although it (in theory) saves memory for "medium" size sets, the SPARSE type seems to have
      // some nasty impacts on response time as it gets larger - particularly in distrib requests.
      // Merging large SPARSE HLLs is much much slower then merging FULL HLLs with the same num docs
      //
      // TODO: add more tunning options for this.
      return new HLL(getLog2m(), getRegwidth(), -1 /* auto explict threshold */,
                     false /* no sparse representation */, HLLType.EMPTY);
                     
    }
  }

  /**
   * Returns the effective {@link NumberType} for the field for the purposes of hash values.
   * ie: If the field has an explict NumberType that is returned; If the field has no explicit
   * NumberType then {@link NumberType#LONG} is returned;  If field is null, then
   * {@link NumberType#FLOAT} is assumed for ValueSource.
   */
  private static NumberType getHashableNumericType(SchemaField field) {
    if (null == field) {
      return NumberType.FLOAT;
    }
    final NumberType result = field.getType().getNumberType();
    return null == result ? NumberType.LONG : result;
  }
}
