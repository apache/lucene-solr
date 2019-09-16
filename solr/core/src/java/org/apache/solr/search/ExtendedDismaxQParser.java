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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ProductFloatFunction;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.parser.SolrQueryParserBase.MagicFieldName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.ExtendedDismaxQParser.ExtendedSolrQueryParser.Alias;
import org.apache.solr.util.SolrPluginUtils;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * Query parser that generates DisjunctionMaxQueries based on user configuration.
 * See Wiki page http://wiki.apache.org/solr/ExtendedDisMax
 */
public class ExtendedDismaxQParser extends QParser {

  /**
   * A field we can't ever find in any schema, so we can safely tell
   * DisjunctionMaxQueryParser to use it as our defaultField, and
   * map aliases from it to any field in our schema.
   */
  private static String IMPOSSIBLE_FIELD_NAME = "\uFFFC\uFFFC\uFFFC";

  /** shorten the class references for utilities */
  private static class U extends SolrPluginUtils {
    /* :NOOP */
  }
  
  /** shorten the class references for utilities */
  private static interface DMP extends DisMaxParams {
    /**
     * User fields. The fields that can be used by the end user to create field-specific queries.
     */
    public static String UF = "uf";
    
    /**
     * Lowercase Operators. If set to true, 'or' and 'and' will be considered OR and AND, otherwise
     * lowercase operators will be considered terms to search for.
     */
    public static String LOWERCASE_OPS = "lowercaseOperators";

    /**
     * Multiplicative boost. Boost functions which scores are going to be multiplied to the score
     * of the main query (instead of just added, like with bf)
     */
    public static String MULT_BOOST = "boost";

    /**
     * If set to true, stopwords are removed from the query.
     */
    public static String STOPWORDS = "stopwords";
  }
  
  private ExtendedDismaxConfiguration config;
  private Query parsedUserQuery;
  private Query altUserQuery;
  private List<Query> boostQueries;
  private boolean parsed = false;
  
  
  public ExtendedDismaxQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
    config = this.createConfiguration(qstr,localParams,params,req);
  }
  
  @Override
  public Query parse() throws SyntaxError {

    parsed = true;
    
    /* the main query we will execute.  we disable the coord because
     * this query is an artificial construct
     */
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    
    /* * * Main User Query * * */
    parsedUserQuery = null;
    String userQuery = getString();
    altUserQuery = null;
    if( userQuery == null || userQuery.trim().length() == 0 ) {
      // If no query is specified, we may have an alternate
      if (config.altQ != null) {
        QParser altQParser = subQuery(config.altQ, null);
        altUserQuery = altQParser.getQuery();
        query.add( altUserQuery , BooleanClause.Occur.MUST );
      } else {
        return null;
        // throw new SyntaxError("missing query string" );
      }
    } else {
      // There is a valid query string
      ExtendedSolrQueryParser up = createEdismaxQueryParser(this, IMPOSSIBLE_FIELD_NAME);
      up.addAlias(IMPOSSIBLE_FIELD_NAME, config.tiebreaker, config.queryFields);
      addAliasesFromRequest(up, config.tiebreaker);
      validateQueryFields(up);
      up.setPhraseSlop(config.qslop);     // slop for explicit user phrase queries
      up.setAllowLeadingWildcard(true);
      up.setAllowSubQueryParsing(config.userFields.isAllowed(MagicFieldName.QUERY.field));
      
      // defer escaping and only do if lucene parsing fails, or we need phrases
      // parsing fails.  Need to sloppy phrase queries anyway though.
      List<Clause> clauses = splitIntoClauses(userQuery, false);
      
      // Always rebuild mainUserQuery from clauses to catch modifications from splitIntoClauses
      // This was necessary for userFields modifications to get propagated into the query.
      // Convert lower or mixed case operators to uppercase if we saw them.
      // only do this for the lucene query part and not for phrase query boosting
      // since some fields might not be case insensitive.
      // We don't use a regex for this because it might change and AND or OR in
      // a phrase query in a case sensitive field.
      String mainUserQuery = rebuildUserQuery(clauses, config.lowercaseOperators);
      
      // but always for unstructured implicit bqs created by getFieldQuery
      up.minShouldMatch = config.minShouldMatch;

      up.setSplitOnWhitespace(config.splitOnWhitespace);
      
      parsedUserQuery = parseOriginalQuery(up, mainUserQuery, clauses, config);
      
      if (parsedUserQuery == null) {
        parsedUserQuery = parseEscapedQuery(up, escapeUserQuery(clauses), config);
      }
      
      query.add(parsedUserQuery, BooleanClause.Occur.MUST);
      
      addPhraseFieldQueries(query, clauses, config);
      
    }
    
    /* * * Boosting Query * * */
    boostQueries = getBoostQueries();
    for(Query f : boostQueries) {
      query.add(f, BooleanClause.Occur.SHOULD);
    }
    
    /* * * Boosting Functions * * */
    List<Query> boostFunctions = getBoostFunctions();
    for(Query f : boostFunctions) {
      query.add(f, BooleanClause.Occur.SHOULD);
    }
    
    //
    // create a boosted query (scores multiplied by boosts)
    //
    Query topQuery = QueryUtils.build(query, this);
    List<ValueSource> boosts = getMultiplicativeBoosts();
    if (boosts.size()>1) {
      ValueSource prod = new ProductFloatFunction(boosts.toArray(new ValueSource[boosts.size()]));
      topQuery = FunctionScoreQuery.boostByValue(topQuery, prod.asDoubleValuesSource());
    } else if (boosts.size() == 1) {
      topQuery = FunctionScoreQuery.boostByValue(topQuery, boosts.get(0).asDoubleValuesSource());
    }
    
    return topQuery;
  }
  
  /**
   * Validate query field names. Must be explicitly defined in the schema or match a dynamic field pattern.
   * Checks source field(s) represented by a field alias
   * 
   * @param up parser used
   * @throws SyntaxError for invalid field name
   */
  protected void validateQueryFields(ExtendedSolrQueryParser up) throws SyntaxError {
    List<String> flds = new ArrayList<>(config.queryFields.keySet().size());
    for (String fieldName : config.queryFields.keySet()) {
      buildQueryFieldList(fieldName, up.getAlias(fieldName), flds, up);
    }
    
    checkFieldsInSchema(flds);
  }
  
  /**
   * Build list of source (non-alias) query field names. Recursive through aliases.
   * 
   * @param fieldName query field name
   * @param alias field alias
   * @param flds list of query field names
   * @param up parser used
   * @throws SyntaxError for invalid field name
   */
  private void buildQueryFieldList(String fieldName, Alias alias, List<String> flds, ExtendedSolrQueryParser up) throws SyntaxError {
    if (null == alias) {
        flds.add(fieldName);
        return;
    }

    up.validateCyclicAliasing(fieldName);
    flds.addAll(getFieldsFromAlias(up, alias));
  }
  
  /**
   * Return list of source (non-alias) field names from an alias
   * 
   * @param up parser used
   * @param a field alias
   * @return list of source fields
   * @throws SyntaxError for invalid field name
   */
  private List<String> getFieldsFromAlias(ExtendedSolrQueryParser up, Alias a) throws SyntaxError {
    List<String> lst = new ArrayList<>();
    for (String s : a.fields.keySet()) {
      buildQueryFieldList(s, up.getAlias(s), lst, up);
    }

    return lst;
  }
  
  /**
   * Verify field name exists in schema, explicit or dynamic field pattern
   * 
   * @param fieldName source field name to verify
   * @throws SyntaxError for invalid field name
   */
  private void checkFieldInSchema(String fieldName) throws SyntaxError {
    try {
        config.schema.getField(fieldName);
    } catch (SolrException se) {
        throw new SyntaxError("Query Field '" + fieldName + "' is not a valid field name", se);
    }
  }

  /**
   * Verify list of source field names
   * 
   * @param flds list of source field names to verify
   * @throws SyntaxError for invalid field name
   */
  private void checkFieldsInSchema(List<String> flds) throws SyntaxError {
    for (String fieldName : flds) {
        checkFieldInSchema(fieldName);
    }
  }
  
  /**
   * Adds shingled phrase queries to all the fields specified in the pf, pf2 anf pf3 parameters
   * 
   */
  protected void addPhraseFieldQueries(BooleanQuery.Builder query, List<Clause> clauses,
      ExtendedDismaxConfiguration config) throws SyntaxError {

    // sloppy phrase queries for proximity
    List<FieldParams> allPhraseFields = config.getAllPhraseFields();
    
    if (allPhraseFields.size() > 0) {
      // find non-field clauses
      List<Clause> normalClauses = new ArrayList<>(clauses.size());
      for (Clause clause : clauses) {
        if (clause.field != null || clause.isPhrase) continue;
        // check for keywords "AND,OR,TO"
        if (clause.isBareWord()) {
          String s = clause.val;
          // avoid putting explicit operators in the phrase query
          if ("OR".equals(s) || "AND".equals(s) || "NOT".equals(s) || "TO".equals(s)) continue;
        }
        normalClauses.add(clause);
      }

      // create a map of {wordGram, [phraseField]}
      Multimap<Integer, FieldParams> phraseFieldsByWordGram = Multimaps.index(allPhraseFields, FieldParams::getWordGrams);

      // for each {wordGram, [phraseField]} entry, create and add shingled field queries to the main user query
      for (Map.Entry<Integer, Collection<FieldParams>> phraseFieldsByWordGramEntry : phraseFieldsByWordGram.asMap().entrySet()) {

        // group the fields within this wordGram collection by their associated slop (it's possible that the same
        // field appears multiple times for the same wordGram count but with different slop values. In this case, we
        // should take the *sum* of those phrase queries, rather than the max across them).
        Multimap<Integer, FieldParams> phraseFieldsBySlop = Multimaps.index(phraseFieldsByWordGramEntry.getValue(), FieldParams::getSlop);
        for (Map.Entry<Integer, Collection<FieldParams>> phraseFieldsBySlopEntry : phraseFieldsBySlop.asMap().entrySet()) {
          addShingledPhraseQueries(query, normalClauses, phraseFieldsBySlopEntry.getValue(),
              phraseFieldsByWordGramEntry.getKey(), config.tiebreaker, phraseFieldsBySlopEntry.getKey());
        }
      }
    }
  }

  /**
   * Creates an instance of ExtendedDismaxConfiguration. It will contain all
   * the necessary parameters to parse the query
   */
  protected ExtendedDismaxConfiguration createConfiguration(String qstr,
      SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new ExtendedDismaxConfiguration(localParams,params,req);
  }
  
  /**
   * Creates an instance of ExtendedSolrQueryParser, the query parser that's going to be used
   * to parse the query.
   */
  protected ExtendedSolrQueryParser createEdismaxQueryParser(QParser qParser, String field) {
    return new ExtendedSolrQueryParser(qParser, field);
  }
  
  /**
   * Parses an escaped version of the user's query.  This method is called 
   * in the event that the original query encounters exceptions during parsing.
   *
   * @param up parser used
   * @param escapedUserQuery query that is parsed, should already be escaped so that no trivial parse errors are encountered
   * @param config Configuration options for this parse request
   * @return the resulting query (flattened if needed) with "min should match" rules applied as specified in the config.
   * @see #parseOriginalQuery
   * @see SolrPluginUtils#flattenBooleanQuery
   */
  protected Query parseEscapedQuery(ExtendedSolrQueryParser up,
      String escapedUserQuery, ExtendedDismaxConfiguration config) throws SyntaxError {
    Query query = up.parse(escapedUserQuery);
    
    if (query instanceof BooleanQuery) {
      BooleanQuery.Builder t = new BooleanQuery.Builder();
      SolrPluginUtils.flattenBooleanQuery(t, (BooleanQuery)query);
      SolrPluginUtils.setMinShouldMatch(t, config.minShouldMatch, config.mmAutoRelax);
      query = QueryUtils.build(t, this);
    }
    return query;
  }
  
  /**
   * Parses the user's original query.  This method attempts to cleanly parse the specified query string using the specified parser, any Exceptions are ignored resulting in null being returned.
   *
   * @param up parser used
   * @param mainUserQuery query string that is parsed
   * @param clauses used to dictate "min should match" logic
   * @param config Configuration options for this parse request
   * @return the resulting query with "min should match" rules applied as specified in the config.
   * @see #parseEscapedQuery
   */
   protected Query parseOriginalQuery(ExtendedSolrQueryParser up,
      String mainUserQuery, List<Clause> clauses, ExtendedDismaxConfiguration config) {
    
    Query query = null;
    try {
      up.setRemoveStopFilter(!config.stopwords);
      up.exceptions = true;
      query = up.parse(mainUserQuery);
      
      if (shouldRemoveStopFilter(config, query)) {
        // if the query was all stop words, remove none of them
        up.setRemoveStopFilter(true);
        query = up.parse(mainUserQuery);          
      }
    } catch (Exception e) {
      // ignore failure and reparse later after escaping reserved chars
      up.exceptions = false;
    }
    
    if(query == null) {
      return null;
    }
    // For correct lucene queries, turn off mm processing if no explicit mm spec was provided
    // and there were explicit operators (except for AND).
    if (query instanceof BooleanQuery) {
      // config.minShouldMatch holds the value of mm which MIGHT have come from the user,
      // but could also have been derived from q.op.
      String mmSpec = config.minShouldMatch;

      if (foundOperators(clauses, config.lowercaseOperators)) {
        mmSpec = params.get(DisMaxParams.MM, "0%"); // Use provided mm spec if present, otherwise turn off mm processing
      }
      query = SolrPluginUtils.setMinShouldMatch((BooleanQuery)query, mmSpec, config.mmAutoRelax);
    }
    return query;
  }

  /**
   * Determines if query should be re-parsed removing the stop filter.
   * @return true if there are stopwords configured and the parsed query was empty
   *         false in any other case.
   */
  protected boolean shouldRemoveStopFilter(ExtendedDismaxConfiguration config,
      Query query) {
    return config.stopwords && isEmpty(query);
  }
  
  private String escapeUserQuery(List<Clause> clauses) {
    StringBuilder sb = new StringBuilder();
    for (Clause clause : clauses) {
      
      boolean doQuote = clause.isPhrase;
      
      String s=clause.val;
      if (!clause.isPhrase && ("OR".equals(s) || "AND".equals(s) || "NOT".equals(s))) {
        doQuote=true;
      }
      
      if (clause.must != 0) {
        sb.append(clause.must);
      }
      if (clause.field != null) {
        sb.append(clause.field);
        sb.append(':');
      }
      if (doQuote) {
        sb.append('"');
      }
      sb.append(clause.val);
      if (doQuote) {
        sb.append('"');
      }
      if (clause.field != null) {
        // Add the default user field boost, if any
        Float boost = config.userFields.getBoost(clause.field);
        if(boost != null)
          sb.append("^").append(boost);
      }
      sb.append(' ');
    }
    return sb.toString();
  }

  /**
   * Returns true if at least one of the clauses is/has an explicit operator (except for AND)
   */
  private boolean foundOperators(List<Clause> clauses, boolean lowercaseOperators) {
    for (Clause clause : clauses) {
      if (clause.must == '+') return true;
      if (clause.must == '-') return true;
      if (clause.isBareWord()) {
        String s = clause.val;
        if ("OR".equals(s)) {
          return true;
        } else if ("NOT".equals(s)) {
          return true;
        } else if (lowercaseOperators && "or".equals(s)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Generates a query string from the raw clauses, uppercasing 
   * 'and' and 'or' as needed.
   * @param clauses the clauses of the query string to be rebuilt
   * @param lowercaseOperators if true, lowercase 'and' and 'or' clauses will 
   *        be recognized as operators and uppercased in the final query string.
   * @return the generated query string.
   */
  protected String rebuildUserQuery(List<Clause> clauses, boolean lowercaseOperators) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<clauses.size(); i++) {
      Clause clause = clauses.get(i);
      String s = clause.raw;
      // and and or won't be operators at the start or end
      if (lowercaseOperators && i>0 && i+1<clauses.size()) {
        if ("AND".equalsIgnoreCase(s)) {
          s="AND";
        } else if ("OR".equalsIgnoreCase(s)) {
          s="OR";
        }
      }
      sb.append(s);
      sb.append(' ');
    }
    return sb.toString();
  }
  
  /**
   * Parses all multiplicative boosts
   */
  protected List<ValueSource> getMultiplicativeBoosts() throws SyntaxError {
    List<ValueSource> boosts = new ArrayList<>();
    if (config.hasMultiplicativeBoosts()) {
      for (String boostStr : config.multBoosts) {
        if (boostStr==null || boostStr.length()==0) continue;
        Query boost = subQuery(boostStr, FunctionQParserPlugin.NAME).getQuery();
        ValueSource vs;
        if (boost instanceof FunctionQuery) {
          vs = ((FunctionQuery)boost).getValueSource();
        } else {
          vs = new QueryValueSource(boost, 1.0f);
        }
        boosts.add(vs);
      }
    }
    return boosts;
  }
  
  /**
   * Parses all function queries
   */
  protected List<Query> getBoostFunctions() throws SyntaxError {
    List<Query> boostFunctions = new LinkedList<>();
    if (config.hasBoostFunctions()) {
      for (String boostFunc : config.boostFuncs) {
        if(null == boostFunc || "".equals(boostFunc)) continue;
        Map<String,Float> ff = SolrPluginUtils.parseFieldBoosts(boostFunc);
        for (Map.Entry<String, Float> entry : ff.entrySet()) {
          Query fq = subQuery(entry.getKey(), FunctionQParserPlugin.NAME).getQuery();
          Float b = entry.getValue();
          if (null != b && b.floatValue() != 1f) {
            fq = new BoostQuery(fq, b);
          }
          boostFunctions.add(fq);
        }
      }
    }
    return boostFunctions;
  }
  
  /**
   * Parses all boost queries
   */
  protected List<Query> getBoostQueries() throws SyntaxError {
    List<Query> boostQueries = new LinkedList<>();
    if (config.hasBoostParams()) {
      for (String qs : config.boostParams) {
        if (qs.trim().length()==0) continue;
        Query q = subQuery(qs, null).getQuery();
        boostQueries.add(q);
      }
    }
    return boostQueries;
  }
  
  /**
   * Extracts all the aliased fields from the requests and adds them to up
   */
  private void addAliasesFromRequest(ExtendedSolrQueryParser up, float tiebreaker) {
    Iterator<String> it = config.solrParams.getParameterNamesIterator();
    while(it.hasNext()) {
      String param = it.next();
      if(param.startsWith("f.") && param.endsWith(".qf")) {
        // Add the alias
        String fname = param.substring(2,param.length()-3);
        String qfReplacement = config.solrParams.get(param);
        Map<String,Float> parsedQf = SolrPluginUtils.parseFieldBoosts(qfReplacement);
        if(parsedQf.size() == 0)
          return;
        up.addAlias(fname, tiebreaker, parsedQf);
      }
    }
  }
  
  /**
   * Modifies the main query by adding a new optional Query consisting
   * of shingled phrase queries across the specified clauses using the 
   * specified field =&gt; boost mappings.
   *
   * @param mainQuery Where the phrase boosting queries will be added
   * @param clauses Clauses that will be used to construct the phrases
   * @param fields Field =&gt; boost mappings for the phrase queries
   * @param shingleSize how big the phrases should be, 0 means a single phrase
   * @param tiebreaker tie breaker value for the DisjunctionMaxQueries
   */
  protected void addShingledPhraseQueries(final BooleanQuery.Builder mainQuery, 
      final List<Clause> clauses,
      final Collection<FieldParams> fields,
      int shingleSize,
      final float tiebreaker,
      final int slop)
          throws SyntaxError {
    
    if (null == fields || fields.isEmpty() || 
        null == clauses || clauses.size() < shingleSize ) 
      return;
    
    if (0 == shingleSize) shingleSize = clauses.size();
    
    final int lastClauseIndex = shingleSize-1;
    
    StringBuilder userPhraseQuery = new StringBuilder();
    for (int i=0; i < clauses.size() - lastClauseIndex; i++) {
      userPhraseQuery.append('"');
      for (int j=0; j <= lastClauseIndex; j++) {
        userPhraseQuery.append(clauses.get(i + j).val);
        userPhraseQuery.append(' ');
      }
      userPhraseQuery.append('"');
      userPhraseQuery.append(' ');
    }
    
    /* for parsing sloppy phrases using DisjunctionMaxQueries */
    ExtendedSolrQueryParser pp = createEdismaxQueryParser(this, IMPOSSIBLE_FIELD_NAME);

    pp.addAlias(IMPOSSIBLE_FIELD_NAME, tiebreaker, getFieldBoosts(fields));
    pp.setPhraseSlop(slop);
    pp.setRemoveStopFilter(true);  // remove stop filter and keep stopwords
    pp.setSplitOnWhitespace(config.splitOnWhitespace);
    
    /* :TODO: reevaluate using makeDismax=true vs false...
     * 
     * The DismaxQueryParser always used DisjunctionMaxQueries for the 
     * pf boost, for the same reasons it used them for the qf fields.
     * When Yonik first wrote the ExtendedDismaxQParserPlugin, he added
     * the "makeDismax=false" property to use BooleanQueries instead, but 
     * when asked why his response was "I honestly don't recall" ...
     *
     * https://issues.apache.org/jira/browse/SOLR-1553?focusedCommentId=12793813#action_12793813
     *
     * so for now, we continue to use dismax style queries because it 
     * seems the most logical and is back compatible, but we should 
     * try to figure out what Yonik was thinking at the time (because he 
     * rarely does things for no reason)
     */
    pp.makeDismax = true; 
    
    
    // minClauseSize is independent of the shingleSize because of stop words
    // (if they are removed from the middle, so be it, but we need at least 
    // two or there shouldn't be a boost)
    pp.minClauseSize = 2;  
    
    // TODO: perhaps we shouldn't use synonyms either...
    
    Query phrase = pp.parse(userPhraseQuery.toString());
    if (phrase != null) {
      mainQuery.add(phrase, BooleanClause.Occur.SHOULD);
    }
  }

  /**
   * @return a {fieldName, fieldBoost} map for the given fields.
   */
  private Map<String, Float> getFieldBoosts(Collection<FieldParams> fields) {
    Map<String, Float> fieldBoostMap = new LinkedHashMap<>(fields.size());

    for (FieldParams field : fields) {
      fieldBoostMap.put(field.getField(), field.getBoost());
    }

    return fieldBoostMap;
  }

  @Override
  public String[] getDefaultHighlightFields() {
    return config.queryFields.keySet().toArray(new String[0]);
  }
  
  @Override
  public Query getHighlightQuery() throws SyntaxError {
    if (!parsed)
      parse();
    return parsedUserQuery == null ? altUserQuery : parsedUserQuery;
  }
  
  @Override
  public void addDebugInfo(NamedList<Object> debugInfo) {
    super.addDebugInfo(debugInfo);
    debugInfo.add("altquerystring", altUserQuery);
    if (null != boostQueries) {
      debugInfo.add("boost_queries", config.boostParams);
      debugInfo.add("parsed_boost_queries",
          QueryParsing.toString(boostQueries, getReq().getSchema()));
    }
    debugInfo.add("boostfuncs", getReq().getParams().getParams(DisMaxParams.BF));
  }

  protected static class Clause {
    
    boolean isBareWord() {
      return must==0 && !isPhrase;
    }
    
    protected String field;
    protected String rawField;  // if the clause is +(foo:bar) then rawField=(foo
    protected boolean isPhrase;
    protected boolean hasWhitespace;
    protected boolean hasSpecialSyntax;
    protected boolean syntaxError;
    protected char must;   // + or -
    protected String val;  // the field value (minus the field name, +/-, quotes)
    protected String raw;  // the raw clause w/o leading/trailing whitespace
  }
  
  public List<Clause> splitIntoClauses(String s, boolean ignoreQuote) {
    ArrayList<Clause> lst = new ArrayList<>(4);
    Clause clause;
    
    int pos=0;
    int end=s.length();
    char ch=0;
    int start;
    boolean disallowUserField;
    while (pos < end) {
      clause = new Clause();
      disallowUserField = true;
      
      ch = s.charAt(pos);
      
      while (Character.isWhitespace(ch)) {
        if (++pos >= end) break;
        ch = s.charAt(pos);
      }
      
      start = pos;      
      
      if ((ch=='+' || ch=='-') && (pos+1)<end) {
        clause.must = ch;
        pos++;
      }
      
      clause.field = getFieldName(s, pos, end);
      if(clause.field != null && !config.userFields.isAllowed(clause.field)) {
        clause.field = null;
      }
      if (clause.field != null) {
        disallowUserField = false;
        int colon = s.indexOf(':',pos);
        clause.rawField = s.substring(pos, colon);
        pos += colon - pos; // skip the field name
        pos++;  // skip the ':'
      }
      
      if (pos>=end) break;
      
      
      char inString=0;
      
      ch = s.charAt(pos);
      if (!ignoreQuote && ch=='"') {
        clause.isPhrase = true;
        inString = '"';
        pos++;
      }
      
      StringBuilder sb = new StringBuilder();
      while (pos < end) {
        ch = s.charAt(pos++);
        if (ch=='\\') {    // skip escaped chars, but leave escaped
          sb.append(ch);
          if (pos >= end) {
            sb.append(ch); // double backslash if we are at the end of the string
            break;
          }
          ch = s.charAt(pos++);
          sb.append(ch);
          continue;
        } else if (inString != 0 && ch == inString) {
          inString=0;
          break;
        } else if (Character.isWhitespace(ch)) {
          clause.hasWhitespace=true;
          if (inString == 0) {
            // end of the token if we aren't in a string, backing
            // up the position.
            pos--;
            break;
          }
        }
        
        if (inString == 0) {
          switch (ch) {
            case '!':
            case '(':
            case ')':
            case ':':
            case '^':
            case '[':
            case ']':
            case '{':
            case '}':
            case '~':
            case '*':
            case '?':
            case '"':
            case '+':
            case '-':
            case '\\':
            case '|':
            case '&':
            case '/':
              clause.hasSpecialSyntax = true;
              sb.append('\\');
          }
        } else if (ch=='"') {
          // only char we need to escape in a string is double quote
          sb.append('\\');
        }
        sb.append(ch);
      }
      clause.val = sb.toString();
      
      if (clause.isPhrase) {
        if (inString != 0) {
          // detected bad quote balancing... retry
          // parsing with quotes like any other char
          return splitIntoClauses(s, true);
        }
        
        // special syntax in a string isn't special
        clause.hasSpecialSyntax = false;        
      } else {
        // an empty clause... must be just a + or - on its own
        if (clause.val.length() == 0) {
          clause.syntaxError = true;
          if (clause.must != 0) {
            clause.val="\\"+clause.must;
            clause.must = 0;
            clause.hasSpecialSyntax = true;
          } else {
            // uh.. this shouldn't happen.
            clause=null;
          }
        }
      }
      
      if (clause != null) {
        if(disallowUserField) {
          clause.raw = s.substring(start, pos);
          // escape colons, except for "match all" query
          if(!"*:*".equals(clause.raw)) {
            clause.raw = clause.raw.replaceAll("([^\\\\]):", "$1\\\\:");
          }
        } else {
          clause.raw = s.substring(start, pos);
          // Add default userField boost if no explicit boost exists
          if(config.userFields.isAllowed(clause.field) && !clause.raw.contains("^")) {
            Float boost = config.userFields.getBoost(clause.field);
            if(boost != null)
              clause.raw += "^" + boost;
          }
        }
        lst.add(clause);
      }
    }
    
    return lst;
  }
  
  /** 
   * returns a field name or legal field alias from the current 
   * position of the string 
   */
  public String getFieldName(String s, int pos, int end) {
    if (pos >= end) return null;
    int p=pos;
    int colon = s.indexOf(':',pos);
    // make sure there is space after the colon, but not whitespace
    if (colon<=pos || colon+1>=end || Character.isWhitespace(s.charAt(colon+1))) return null;
    char ch = s.charAt(p++);
    while ((ch=='(' || ch=='+' || ch=='-') && (pos<end)) {
      ch = s.charAt(p++);
      pos++;
    }
    if (!Character.isJavaIdentifierPart(ch)) return null;
    while (p<colon) {
      ch = s.charAt(p++);
      if (!(Character.isJavaIdentifierPart(ch) || ch=='-' || ch=='.')) return null;
    }
    String fname = s.substring(pos, p);
    boolean isInSchema = getReq().getSchema().getFieldTypeNoEx(fname) != null;
    boolean isAlias = config.solrParams.get("f."+fname+".qf") != null;
    boolean isMagic = (null != MagicFieldName.get(fname));
    
    return (isInSchema || isAlias || isMagic) ? fname : null;
  }
  
  public static List<String> split(String s, boolean ignoreQuote) {
    ArrayList<String> lst = new ArrayList<>(4);
    int pos=0, start=0, end=s.length();
    char inString=0;
    char ch=0;
    while (pos < end) {
      char prevChar=ch;
      ch = s.charAt(pos++);
      if (ch=='\\') {    // skip escaped chars
        pos++;
      } else if (inString != 0 && ch==inString) {
        inString=0;
      } else if (!ignoreQuote && ch=='"') {
        // If char is directly preceeded by a number or letter
        // then don't treat it as the start of a string.
        if (!Character.isLetterOrDigit(prevChar)) {
          inString=ch;
        }
      } else if (Character.isWhitespace(ch) && inString==0) {
        lst.add(s.substring(start,pos-1));
        start=pos;
      }
    }
    if (start < end) {
      lst.add(s.substring(start,end));
    }
    
    if (inString != 0) {
      // unbalanced quote... ignore them
      return split(s, true);
    }
    
    return lst;
  }
  
  enum QType {
    FIELD,
    PHRASE,
    PREFIX,
    WILDCARD,
    FUZZY,
    RANGE
  }
  
  
  static final RuntimeException unknownField = new RuntimeException("UnknownField");
  static {
    unknownField.fillInStackTrace();
  }
  
  /**
   * A subclass of SolrQueryParser that supports aliasing fields for
   * constructing DisjunctionMaxQueries.
   */
  public static class ExtendedSolrQueryParser extends SolrQueryParser {
    
    /** A simple container for storing alias info
     */
    protected static class Alias {
      public float tie;
      public Map<String,Float> fields;
    }
    
    boolean makeDismax=true;
    boolean allowWildcard=true;
    int minClauseSize = 0;    // minimum number of clauses per phrase query...
    // used when constructing boosting part of query via sloppy phrases
    boolean exceptions;  //  allow exceptions to be thrown (for example on a missing field)
    
    private Map<String, Analyzer> nonStopFilterAnalyzerPerField;
    private boolean removeStopFilter;
    String minShouldMatch; // for inner boolean queries produced from a single fieldQuery
    
    /**
     * Where we store a map from field name we expect to see in our query
     * string, to Alias object containing the fields to use in our
     * DisjunctionMaxQuery and the tiebreaker to use.
     */
    protected Map<String,Alias> aliases = new HashMap<>(3);
    
    private QType type;
    private String field;
    private String val;
    private String val2;
    private List<String> vals;
    private boolean bool;
    private boolean bool2;
    private float flt;
    private int slop;
    
    public ExtendedSolrQueryParser(QParser parser, String defaultField) {
      super(parser, defaultField);
      // Respect the q.op parameter before mm will be applied later
      SolrParams defaultParams = SolrParams.wrapDefaults(parser.getLocalParams(), parser.getParams());
      QueryParser.Operator defaultOp = QueryParsing.parseOP(defaultParams.get(QueryParsing.OP));
      setDefaultOperator(defaultOp);
    }
    
    public void setRemoveStopFilter(boolean remove) {
      removeStopFilter = remove;
    }
    
    @Override
    protected Query getBooleanQuery(List<BooleanClause> clauses) throws SyntaxError {
      Query q = super.getBooleanQuery(clauses);
      if (q != null) {
        q = QueryUtils.makeQueryable(q);
      }
      return q;
    }
    
    /**
     * Add an alias to this query parser.
     *
     * @param field the field name that should trigger alias mapping
     * @param fieldBoosts the mapping from fieldname to boost value that
     *                    should be used to build up the clauses of the
     *                    DisjunctionMaxQuery.
     * @param tiebreaker to the tiebreaker to be used in the
     *                   DisjunctionMaxQuery
     * @see SolrPluginUtils#parseFieldBoosts
     */
    public void addAlias(String field, float tiebreaker,
        Map<String,Float> fieldBoosts) {
      Alias a = new Alias();
      a.tie = tiebreaker;
      a.fields = fieldBoosts;
      aliases.put(field, a);
    }
    
    /**
     * Returns the aliases found for a field.
     * Returns null if there are no aliases for the field
     * @return Alias
     */
    protected Alias getAlias(String field) {
      return aliases.get(field);
    }
    
    @Override
    protected Query getFieldQuery(String field, String val, boolean quoted, boolean raw) throws SyntaxError {
      this.type = quoted ? QType.PHRASE : QType.FIELD;
      this.field = field;
      this.val = val;
      this.vals = null;
      this.slop = getPhraseSlop(); // unspecified
      return getAliasedQuery();
    }
    
    @Override
    protected Query getFieldQuery(String field, String val, int slop) throws SyntaxError {
      this.type = QType.PHRASE;
      this.field = field;
      this.val = val;
      this.vals = null;
      this.slop = slop;
      return getAliasedQuery();
    }

    @Override
    protected Query getFieldQuery(String field, List<String> queryTerms, boolean raw) throws SyntaxError {
      this.type = QType.FIELD;
      this.field = field;
      this.val = null;
      this.vals = queryTerms;
      this.slop = getPhraseSlop();
      return getAliasedMultiTermQuery();
    }

    @Override
    protected Query getPrefixQuery(String field, String val) throws SyntaxError {
      if (val.equals("") && field.equals("*")) {
        return new MatchAllDocsQuery();
      }
      this.type = QType.PREFIX;
      this.field = field;
      this.val = val;
      this.vals = null;
      return getAliasedQuery();
    }
    
    @Override
    protected Query newFieldQuery(Analyzer analyzer, String field, String queryText, 
                                  boolean quoted, boolean fieldAutoGenPhraseQueries, boolean enableGraphQueries,
                                  SynonymQueryStyle synonymQueryStyle)
        throws SyntaxError {
      Analyzer actualAnalyzer;
      if (removeStopFilter) {
        if (nonStopFilterAnalyzerPerField == null) {
          nonStopFilterAnalyzerPerField = new HashMap<>();
        }
        actualAnalyzer = nonStopFilterAnalyzerPerField.get(field);
        if (actualAnalyzer == null) {
          actualAnalyzer = noStopwordFilterAnalyzer(field);
        }
      } else {
        actualAnalyzer = parser.getReq().getSchema().getFieldType(field).getQueryAnalyzer();
      }
      return super.newFieldQuery(actualAnalyzer, field, queryText, quoted, fieldAutoGenPhraseQueries, enableGraphQueries, synonymQueryStyle);
    }
    
    @Override
    protected Query getRangeQuery(String field, String a, String b, boolean startInclusive, boolean endInclusive) throws SyntaxError {
      this.type = QType.RANGE;
      this.field = field;
      this.val = a;
      this.val2 = b;
      this.vals = null;
      this.bool = startInclusive;
      this.bool2 = endInclusive;
      return getAliasedQuery();
    }
    
    @Override
    protected Query getWildcardQuery(String field, String val) throws SyntaxError {
      if (val.equals("*")) {
        if (field.equals("*") || getExplicitField() == null) {
          return new MatchAllDocsQuery();
        } else{
          return getPrefixQuery(field,"");
        }
      }
      this.type = QType.WILDCARD;
      this.field = field;
      this.val = val;
      this.vals = null;
      return getAliasedQuery();
    }
    
    @Override
    protected Query getFuzzyQuery(String field, String val, float minSimilarity) throws SyntaxError {
      this.type = QType.FUZZY;
      this.field = field;
      this.val = val;
      this.vals = null;
      this.flt = minSimilarity;
      return getAliasedQuery();
    }
    
    /**
     * Delegates to the super class unless the field has been specified
     * as an alias -- in which case we recurse on each of
     * the aliased fields, and the results are composed into a
     * DisjunctionMaxQuery.  (so yes: aliases which point at other
     * aliases should work)
     */
    protected Query getAliasedQuery() throws SyntaxError {
      Alias a = aliases.get(field);
      this.validateCyclicAliasing(field);
      if (a != null) {
        List<Query> lst = getQueries(a);
        if (lst == null || lst.size()==0)
          return getQuery();
        // make a DisjunctionMaxQuery in this case too... it will stop
        // the "mm" processing from making everything required in the case
        // that the query expanded to multiple clauses.
        // DisMaxQuery.rewrite() removes itself if there is just a single clause anyway.
        // if (lst.size()==1) return lst.get(0);
        
        if (makeDismax) {
          DisjunctionMaxQuery q = new DisjunctionMaxQuery(lst, a.tie);
          return q;
        } else {
          BooleanQuery.Builder q = new BooleanQuery.Builder();
          for (Query sub : lst) {
            q.add(sub, BooleanClause.Occur.SHOULD);
          }
          return QueryUtils.build(q, parser);
        }
      } else {
        
        // verify that a fielded query is actually on a field that exists... if not,
        // then throw an exception to get us out of here, and we'll treat it like a
        // literal when we try the escape+re-parse.
        if (exceptions) {
          FieldType ft = schema.getFieldTypeNoEx(field);
          if (ft == null && null == MagicFieldName.get(field)) {
            throw unknownField;
          }
        }
        
        return getQuery();
      }
    }

    /**
     * Delegates to the super class unless the field has been specified
     * as an alias -- in which case we recurse on each of
     * the aliased fields, and the results are composed into a
     * DisjunctionMaxQuery.  (so yes: aliases which point at other
     * aliases should work)
     */
    protected Query getAliasedMultiTermQuery() throws SyntaxError {
      Alias a = aliases.get(field);
      this.validateCyclicAliasing(field);
      if (a != null) {
        List<Query> lst = getMultiTermQueries(a);
        if (lst == null || lst.size() == 0) {
          return getQuery();
        }
        
        // make a DisjunctionMaxQuery in this case too... it will stop
        // the "mm" processing from making everything required in the case
        // that the query expanded to multiple clauses.
        // DisMaxQuery.rewrite() removes itself if there is just a single clause anyway.
        // if (lst.size()==1) return lst.get(0);
        if (makeDismax) {
          Query firstQuery = lst.get(0);
          if ((firstQuery instanceof BooleanQuery
              || (firstQuery instanceof BoostQuery && ((BoostQuery)firstQuery).getQuery() instanceof BooleanQuery))
              && allSameQueryStructure(lst)) {
            BooleanQuery.Builder q = new BooleanQuery.Builder();
            List<Query> subs = new ArrayList<>(lst.size());
            BooleanQuery firstBooleanQuery = firstQuery instanceof BoostQuery
                ? (BooleanQuery)((BoostQuery)firstQuery).getQuery() : (BooleanQuery)firstQuery;
            for (int c = 0 ; c < firstBooleanQuery.clauses().size() ; ++c) {
              subs.clear();
              // Make a dismax query for each clause position in the boolean per-field queries.
              for (int n = 0 ; n < lst.size() ; ++n) {
                if (lst.get(n) instanceof BoostQuery) {
                  BoostQuery boostQuery = (BoostQuery)lst.get(n);
                  BooleanQuery booleanQuery = (BooleanQuery)boostQuery.getQuery();
                  subs.add(new BoostQuery(booleanQuery.clauses().get(c).getQuery(), boostQuery.getBoost()));
                } else {
                  subs.add(((BooleanQuery)lst.get(n)).clauses().get(c).getQuery());
                }
              }
              q.add(newBooleanClause(new DisjunctionMaxQuery(subs, a.tie), BooleanClause.Occur.SHOULD));
            }
            return QueryUtils.build(q, parser);
          } else {
            return new DisjunctionMaxQuery(lst, a.tie); 
          }
        } else {
          BooleanQuery.Builder q = new BooleanQuery.Builder();
          for (Query sub : lst) {
            q.add(sub, BooleanClause.Occur.SHOULD);
          }
          return QueryUtils.build(q, parser);
        }
      } else {
        // verify that a fielded query is actually on a field that exists... if not,
        // then throw an exception to get us out of here, and we'll treat it like a
        // literal when we try the escape+re-parse.
        if (exceptions) {
          FieldType ft = schema.getFieldTypeNoEx(field);
          if (ft == null && null == MagicFieldName.get(field)) {
            throw unknownField;
          }
        }
        return getQuery();
      }
    }

    /**
     * Recursively examines the given query list for identical structure in all queries.
     * Boosts on BoostQuery-s are ignored, and the contained queries are instead used as the basis for comparison.
     **/
    private boolean allSameQueryStructure(List<Query> lst) {
      boolean allSame = true;
      Query firstQuery = lst.get(0);
      if (firstQuery instanceof BoostQuery) {
        firstQuery = ((BoostQuery)firstQuery).getQuery(); // ignore boost; compare contained query
      }
      for (int n = 1 ; n < lst.size(); ++n) {
        Query nthQuery = lst.get(n);
        if (nthQuery instanceof BoostQuery) {
          nthQuery = ((BoostQuery)nthQuery).getQuery();
        }
        if (nthQuery.getClass() != firstQuery.getClass()) {
          allSame = false;
          break;
        }
        if (firstQuery instanceof BooleanQuery) {
          List<BooleanClause> firstBooleanClauses = ((BooleanQuery)firstQuery).clauses();
          List<BooleanClause> nthBooleanClauses = ((BooleanQuery)nthQuery).clauses();
          if (firstBooleanClauses.size() != nthBooleanClauses.size()) {
            allSame = false;
            break;
          }
          for (int c = 0 ; c < firstBooleanClauses.size() ; ++c) {
            if (nthBooleanClauses.get(c).getQuery().getClass() != firstBooleanClauses.get(c).getQuery().getClass()
                || nthBooleanClauses.get(c).getOccur() != firstBooleanClauses.get(c).getOccur()) {
              allSame = false;
              break;
            }
            if (firstBooleanClauses.get(c).getQuery() instanceof BooleanQuery && ! allSameQueryStructure
                (Arrays.asList(firstBooleanClauses.get(c).getQuery(), nthBooleanClauses.get(c).getQuery()))) {
              allSame = false;
              break;
            }
          }
        }
      }
      return allSame;
    }

    @Override
    protected void addMultiTermClause(List<BooleanClause> clauses, Query q) {
      // We might have been passed a null query; the terms might have been filtered away by the analyzer.
      if (q == null) {
        return;
      }
      
      boolean required = operator == AND_OPERATOR;
      BooleanClause.Occur occur = required ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;  
      
      if (q instanceof BooleanQuery) {
        boolean allOptionalDisMaxQueries = true;
        for (BooleanClause c : ((BooleanQuery)q).clauses()) {
          if (c.getOccur() != BooleanClause.Occur.SHOULD || ! (c.getQuery() instanceof DisjunctionMaxQuery)) {
            allOptionalDisMaxQueries = false;
            break;
          }
        }
        if (allOptionalDisMaxQueries) {
          // getAliasedMultiTermQuery() constructed a BooleanQuery containing only SHOULD DisjunctionMaxQuery-s.
          // Unwrap the query and add a clause for each contained DisMax query.
          for (BooleanClause c : ((BooleanQuery)q).clauses()) {
            clauses.add(newBooleanClause(c.getQuery(), occur));
          }
          return;
        }
      }
      clauses.add(newBooleanClause(q, occur));
    }

    /**
     * Validate there is no cyclic referencing in the aliasing
     */
    private void validateCyclicAliasing(String field) throws SyntaxError {
      Set<String> set = new HashSet<>();
      set.add(field);
      if(validateField(field, set)) {
        throw new SyntaxError("Field aliases lead to a cycle");
      }
    }
    
    private boolean validateField(String field, Set<String> set) {
      if(this.getAlias(field) == null) {
        return false;
      }
      boolean hascycle = false;
      for(String referencedField:this.getAlias(field).fields.keySet()) {
        if(!set.add(referencedField)) {
          hascycle = true;
        } else {
          if(validateField(referencedField, set)) {
            hascycle = true;
          }
          set.remove(referencedField);
        }
      }
      return hascycle;
    }
    
    protected List<Query> getQueries(Alias a) throws SyntaxError {
      if (a == null) return null;
      if (a.fields.size()==0) return null;
      List<Query> lst= new ArrayList<>(4);
      
      for (String f : a.fields.keySet()) {
        this.field = f;
        Query sub = getAliasedQuery();
        if (sub != null) {
          Float boost = a.fields.get(f);
          if (boost != null && boost.floatValue() != 1f) {
            sub = new BoostQuery(sub, boost);
          }
          lst.add(sub);
        }
      }
      return lst;
    }

    protected List<Query> getMultiTermQueries(Alias a) throws SyntaxError {
      if (a == null) return null;
      if (a.fields.size()==0) return null;
      List<Query> lst= new ArrayList<>(4);

      for (String f : a.fields.keySet()) {
        this.field = f;
        Query sub = getAliasedMultiTermQuery();
        if (sub != null) {
          Float boost = a.fields.get(f);
          if (boost != null && boost.floatValue() != 1f) {
            sub = new BoostQuery(sub, boost);
          }
          lst.add(sub);
        }
      }
      return lst;
    }

    private Query getQuery() {
      try {
        
        switch (type) {
          case FIELD:  // fallthrough
          case PHRASE:
            Query query;
            if (val == null) {
              query = super.getFieldQuery(field, vals, false);
            } else {
              query = super.getFieldQuery(field, val, type == QType.PHRASE, false);
            }
            // Boolean query on a whitespace-separated string
            // If these were synonyms we would have a SynonymQuery
            if (query instanceof BooleanQuery) {
              if (type == QType.FIELD) { // Don't set mm for boolean query containing phrase queries
                BooleanQuery bq = (BooleanQuery) query;
                query = SolrPluginUtils.setMinShouldMatch(bq, minShouldMatch, false);
              }
            } else if (query instanceof PhraseQuery) {
              PhraseQuery pq = (PhraseQuery)query;
              if (minClauseSize > 1 && pq.getTerms().length < minClauseSize) return null;
              PhraseQuery.Builder builder = new PhraseQuery.Builder();
              Term[] terms = pq.getTerms();
              int[] positions = pq.getPositions();
              for (int i = 0; i < terms.length; ++i) {
                builder.add(terms[i], positions[i]);
              }
              builder.setSlop(slop);
              query = builder.build();
            } else if (query instanceof MultiPhraseQuery) {
              MultiPhraseQuery mpq = (MultiPhraseQuery)query;
              if (minClauseSize > 1 && mpq.getTermArrays().length < minClauseSize) return null;
              if (slop != mpq.getSlop()) {
                query = new MultiPhraseQuery.Builder(mpq).setSlop(slop).build();
              }
            } else if (query instanceof SpanQuery) {
              return query;
            } else if (minClauseSize > 1) {
              // if it's not a type of phrase query, it doesn't meet the minClauseSize requirements
              return null;
            }
            return query;
          case PREFIX: return super.getPrefixQuery(field, val);
          case WILDCARD: return super.getWildcardQuery(field, val);
          case FUZZY: return super.getFuzzyQuery(field, val, flt);
          case RANGE: return super.getRangeQuery(field, val, val2, bool, bool2);
        }
        return null;
        
      } catch (Exception e) {
        // an exception here is due to the field query not being compatible with the input text
        // for example, passing a string to a numeric field.
        return null;
      }
    }

    private Analyzer noStopwordFilterAnalyzer(String fieldName) {
      FieldType ft = parser.getReq().getSchema().getFieldType(fieldName);
      Analyzer qa = ft.getQueryAnalyzer();
      if (!(qa instanceof TokenizerChain)) {
        return qa;
      }
      
      TokenizerChain tcq = (TokenizerChain) qa;
      Analyzer ia = ft.getIndexAnalyzer();
      if (ia == qa || !(ia instanceof TokenizerChain)) {
        return qa;
      }
      TokenizerChain tci = (TokenizerChain) ia;
      
      // make sure that there isn't a stop filter in the indexer
      for (TokenFilterFactory tf : tci.getTokenFilterFactories()) {
        if (tf instanceof StopFilterFactory) {
          return qa;
        }
      }
      
      // now if there is a stop filter in the query analyzer, remove it
      int stopIdx = -1;
      TokenFilterFactory[] facs = tcq.getTokenFilterFactories();
      
      for (int i = 0; i < facs.length; i++) {
        TokenFilterFactory tf = facs[i];
        if (tf instanceof StopFilterFactory) {
          stopIdx = i;
          break;
        }
      }
      
      if (stopIdx == -1) {
        // no stop filter exists
        return qa;
      }
      
      TokenFilterFactory[] newtf = new TokenFilterFactory[facs.length - 1];
      for (int i = 0, j = 0; i < facs.length; i++) {
        if (i == stopIdx) continue;
        newtf[j++] = facs[i];
      }
      
      TokenizerChain newa = new TokenizerChain(tcq.getCharFilterFactories(), tcq.getTokenizerFactory(), newtf);
      newa.setPositionIncrementGap(tcq.getPositionIncrementGap(fieldName));
      return newa;
    }
  }
  
  static boolean isEmpty(Query q) {
    if (q==null) return true;
    if (q instanceof BooleanQuery && ((BooleanQuery)q).clauses().size()==0) return true;
    return false;
  }
  
  /**
   * Class that encapsulates the input from userFields parameter and can answer whether
   * a field allowed or disallowed as fielded query in the query string
   */
  static class UserFields {
    private Map<String,Float> userFieldsMap;
    private DynamicField[] dynamicUserFields;
    private DynamicField[] negativeDynamicUserFields;
    
    UserFields(Map<String, Float> ufm) {
      userFieldsMap = ufm;
      if (0 == userFieldsMap.size()) {
        userFieldsMap.put("*", null);
      }
      
      // Process dynamic patterns in userFields
      ArrayList<DynamicField> dynUserFields = new ArrayList<>();
      ArrayList<DynamicField> negDynUserFields = new ArrayList<>();
      for(String f : userFieldsMap.keySet()) {
        if(f.contains("*")) {
          if(f.startsWith("-"))
            negDynUserFields.add(new DynamicField(f.substring(1)));
          else
            dynUserFields.add(new DynamicField(f));
        }
      }
      // unless "_query_" was expressly allowed, we forbid it.
      if (!userFieldsMap.containsKey(MagicFieldName.QUERY.field)) {
        userFieldsMap.put("-" + MagicFieldName.QUERY.field, null);
      }
      Collections.sort(dynUserFields);
      dynamicUserFields = dynUserFields.toArray(new DynamicField[dynUserFields.size()]);
      Collections.sort(negDynUserFields);
      negativeDynamicUserFields = negDynUserFields.toArray(new DynamicField[negDynUserFields.size()]);
    }
    
    /**
     * Is the given field name allowed according to UserFields spec given in the uf parameter?
     * @param fname the field name to examine
     * @return true if the fielded queries are allowed on this field
     */
    public boolean isAllowed(String fname) {
      boolean res = ((userFieldsMap.containsKey(fname) || isDynField(fname, false)) && 
          !userFieldsMap.containsKey("-"+fname) &&
          !isDynField(fname, true));
      return res;
    }
    
    private boolean isDynField(String field, boolean neg) {
      return getDynFieldForName(field, neg) == null ? false : true;
    }
    
    private String getDynFieldForName(String f, boolean neg) {
      for( DynamicField df : neg?negativeDynamicUserFields:dynamicUserFields ) {
        if( df.matches( f ) ) return df.wildcard;
      }
      return null;
    }
    
    /**
     * Finds the default user field boost associated with the given field.
     * This is parsed from the uf parameter, and may be specified as wildcards, e.g. *name^2.0 or *^3.0
     * @param field the field to find boost for
     * @return the float boost value associated with the given field or a wildcard matching the field
     */
    public Float getBoost(String field) {
      return (userFieldsMap.containsKey(field)) ?
          userFieldsMap.get(field) : // Exact field
            userFieldsMap.get(getDynFieldForName(field, false)); // Dynamic field
    }
  }
  
  /* Represents a dynamic field, for easier matching, inspired by same class in IndexSchema */
  static class DynamicField implements Comparable<DynamicField> {
    final static int STARTS_WITH=1;
    final static int ENDS_WITH=2;
    final static int CATCHALL=3;
    
    final String wildcard;
    final int type;
    
    final String str;
    
    protected DynamicField(String wildcard) {
      this.wildcard = wildcard;
      if (wildcard.equals("*")) {
        type=CATCHALL;
        str=null;
      }
      else if (wildcard.startsWith("*")) {
        type=ENDS_WITH;
        str=wildcard.substring(1);
      }
      else if (wildcard.endsWith("*")) {
        type=STARTS_WITH;
        str=wildcard.substring(0,wildcard.length()-1);
      }
      else {
        throw new RuntimeException("dynamic field name must start or end with *");
      }
    }
    
    /*
     * Returns true if the regex wildcard for this DynamicField would match the input field name
     */
    public boolean matches(String name) {
      if (type==CATCHALL) return true;
      else if (type==STARTS_WITH && name.startsWith(str)) return true;
      else if (type==ENDS_WITH && name.endsWith(str)) return true;
      else return false;
    }
    
    /**
     * Sort order is based on length of regex.  Longest comes first.
     * @param other The object to compare to.
     * @return a negative integer, zero, or a positive integer
     * as this object is less than, equal to, or greater than
     * the specified object.
     */
    @Override
    public int compareTo(DynamicField other) {
      return other.wildcard.length() - wildcard.length();
    }
    
    @Override
    public String toString() {
      return this.wildcard;
    }
  }
  
  /**
   * Simple container for configuration information used when parsing queries
   */
  public static class ExtendedDismaxConfiguration {
    
    /**
     * The field names specified by 'qf' that (most) clauses will 
     * be queried against 
     */
    protected Map<String,Float> queryFields;
    
    /** 
     * The field names specified by 'uf' that users are 
     * allowed to include literally in their query string.  The Float
     * boost values will be applied automatically to any clause using that 
     * field name. '*' will be treated as an alias for any 
     * field that exists in the schema. Wildcards are allowed to
     * express dynamicFields.
     */
    protected UserFields userFields;
    
    protected String[] boostParams;
    protected String[] multBoosts;
    protected SolrParams solrParams;
    protected String minShouldMatch;
    
    protected List<FieldParams> allPhraseFields;
    
    protected float tiebreaker;
    
    protected int qslop;
    
    protected boolean stopwords;

    protected boolean mmAutoRelax;
    
    protected String altQ;
    
    protected boolean lowercaseOperators;
    
    protected  String[] boostFuncs;

    protected boolean splitOnWhitespace;
    
    protected IndexSchema schema;

    public ExtendedDismaxConfiguration(SolrParams localParams,
        SolrParams params, SolrQueryRequest req) {
      solrParams = SolrParams.wrapDefaults(localParams, params);
      schema = req.getSchema();
      minShouldMatch = DisMaxQParser.parseMinShouldMatch(schema, solrParams); // req.getSearcher() here causes searcher refcount imbalance
      userFields = new UserFields(U.parseFieldBoosts(solrParams.getParams(DMP.UF)));
      try {
        queryFields = DisMaxQParser.parseQueryFields(schema, solrParams);  // req.getSearcher() here causes searcher refcount imbalance
      } catch (SyntaxError e) {
        throw new RuntimeException(e);
      }
      // Phrase slop array
      int pslop[] = new int[4];
      pslop[0] = solrParams.getInt(DisMaxParams.PS, 0);
      pslop[2] = solrParams.getInt(DisMaxParams.PS2, pslop[0]);
      pslop[3] = solrParams.getInt(DisMaxParams.PS3, pslop[0]);
      
      List<FieldParams> phraseFields = U.parseFieldBoostsAndSlop(solrParams.getParams(DMP.PF),0,pslop[0]);
      List<FieldParams> phraseFields2 = U.parseFieldBoostsAndSlop(solrParams.getParams(DMP.PF2),2,pslop[2]);
      List<FieldParams> phraseFields3 = U.parseFieldBoostsAndSlop(solrParams.getParams(DMP.PF3),3,pslop[3]);
      
      allPhraseFields = new ArrayList<>(phraseFields.size() + phraseFields2.size() + phraseFields3.size());
      allPhraseFields.addAll(phraseFields);
      allPhraseFields.addAll(phraseFields2);
      allPhraseFields.addAll(phraseFields3);
      
      tiebreaker = solrParams.getFloat(DisMaxParams.TIE, 0.0f);
      
      qslop = solrParams.getInt(DisMaxParams.QS, 0);
      
      stopwords = solrParams.getBool(DMP.STOPWORDS, true);

      mmAutoRelax = solrParams.getBool(DMP.MM_AUTORELAX, false);
      
      altQ = solrParams.get( DisMaxParams.ALTQ );

      lowercaseOperators = solrParams.getBool(DMP.LOWERCASE_OPS, false);
      
      /* * * Boosting Query * * */
      boostParams = solrParams.getParams(DisMaxParams.BQ);
      
      boostFuncs = solrParams.getParams(DisMaxParams.BF);
      
      multBoosts = solrParams.getParams(DMP.MULT_BOOST);

      splitOnWhitespace = solrParams.getBool(QueryParsing.SPLIT_ON_WHITESPACE, SolrQueryParser.DEFAULT_SPLIT_ON_WHITESPACE);
    }
    /**
     * 
     * @return true if there are valid multiplicative boost queries
     */
    public boolean hasMultiplicativeBoosts() {
      return multBoosts!=null && multBoosts.length>0;
    }
    
    /**
     * 
     * @return true if there are valid boost functions
     */
    public boolean hasBoostFunctions() {
      return null != boostFuncs && 0 != boostFuncs.length;
    }
    /**
     * 
     * @return true if there are valid boost params
     */
    public boolean hasBoostParams() {
      return boostParams!=null && boostParams.length>0;
    }
    
    public List<FieldParams> getAllPhraseFields() {
      return allPhraseFields;
    }
  }
  
}
