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
package org.apache.solr.util;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.RequestParams;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.json.RequestUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.FieldParams;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.search.SortSpecParsing;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import static java.util.Collections.singletonList;
import static org.apache.solr.core.PluginInfo.APPENDS;
import static org.apache.solr.core.PluginInfo.DEFAULTS;
import static org.apache.solr.core.PluginInfo.INVARIANTS;
import static org.apache.solr.core.RequestParams.USEPARAM;

/**
 * Utilities that may be of use to RequestHandlers.
 */
public class SolrPluginUtils {


  /**
   * Map containing all the possible purposes codes of a request as key and
   * the corresponding readable purpose as value
   */
  private static final Map<Integer, String> purposes;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static {
      Map<Integer, String> map = new TreeMap<>();
      map.put(ShardRequest.PURPOSE_PRIVATE, "PRIVATE");
      map.put(ShardRequest.PURPOSE_GET_TOP_IDS, "GET_TOP_IDS");
      map.put(ShardRequest.PURPOSE_REFINE_TOP_IDS, "REFINE_TOP_IDS");
      map.put(ShardRequest.PURPOSE_GET_FACETS, "GET_FACETS");
      map.put(ShardRequest.PURPOSE_REFINE_FACETS, "REFINE_FACETS");
      map.put(ShardRequest.PURPOSE_GET_FIELDS, "GET_FIELDS");
      map.put(ShardRequest.PURPOSE_GET_HIGHLIGHTS, "GET_HIGHLIGHTS");
      map.put(ShardRequest.PURPOSE_GET_DEBUG, "GET_DEBUG");
      map.put(ShardRequest.PURPOSE_GET_STATS, "GET_STATS");
      map.put(ShardRequest.PURPOSE_GET_TERMS, "GET_TERMS");
      map.put(ShardRequest.PURPOSE_GET_TOP_GROUPS, "GET_TOP_GROUPS");
      map.put(ShardRequest.PURPOSE_GET_MLT_RESULTS, "GET_MLT_RESULTS");
      map.put(ShardRequest.PURPOSE_REFINE_PIVOT_FACETS, "REFINE_PIVOT_FACETS");
      map.put(ShardRequest.PURPOSE_SET_TERM_STATS, "SET_TERM_STATS");
      map.put(ShardRequest.PURPOSE_GET_TERM_STATS, "GET_TERM_STATS");
    purposes = Collections.unmodifiableMap(map);
  }

  private static final MapSolrParams maskUseParams = new MapSolrParams(ImmutableMap.<String, String>builder()
      .put(USEPARAM, "")
      .build());

  /**
   * Set default-ish params on a SolrQueryRequest.
   *
   * RequestHandlers can use this method to ensure their defaults and
   * overrides are visible to other components such as the response writer
   *
   * @param req The request whose params we are interested i
   * @param defaults values to be used if no values are specified in the request params
   * @param appends values to be appended to those from the request (or defaults) when dealing with multi-val params, or treated as another layer of defaults for singl-val params.
   * @param invariants values which will be used instead of any request, or default values, regardless of context.
   */
  public static void setDefaults(SolrQueryRequest req, SolrParams defaults, SolrParams appends, SolrParams invariants) {
    setDefaults(null, req, defaults, appends, invariants);
  }

  public static void setDefaults(SolrRequestHandler handler, SolrQueryRequest req, SolrParams defaults,
                                 SolrParams appends, SolrParams invariants) {
    String useParams = (String) req.getContext().get(USEPARAM);
    if(useParams != null) {
      RequestParams rp = req.getCore().getSolrConfig().getRequestParams();
      defaults = applyParamSet(rp, defaults, useParams, DEFAULTS);
      appends = applyParamSet(rp, appends, useParams, APPENDS);
      invariants = applyParamSet(rp, invariants, useParams, INVARIANTS);
    }
    useParams = req.getParams().get(USEPARAM);
    if (useParams != null && !useParams.isEmpty()) {
      RequestParams rp = req.getCore().getSolrConfig().getRequestParams();
      // now that we have expanded the request macro useParams with the actual values
      // it makes no sense to keep it visible now on.
      // distrib request sends all params to the nodes down the line and
      // if it sends the useParams to other nodes , they will expand them as well.
      // which is not desirable. At the same time, because we send the useParams
      // value as an empty string to other nodes we get the desired benefit of
      // overriding the useParams specified in the requestHandler directly
      req.setParams(SolrParams.wrapDefaults(maskUseParams, req.getParams()));
      defaults = applyParamSet(rp, defaults, useParams, DEFAULTS);
      appends = applyParamSet(rp, appends, useParams, APPENDS);
      invariants = applyParamSet(rp, invariants, useParams, INVARIANTS);
    }
    RequestUtil.processParams(handler, req, defaults, appends, invariants);
  }

  private static SolrParams applyParamSet(RequestParams requestParams,
                                          SolrParams defaults, String paramSets, String type) {
    if (paramSets == null) return defaults;
    List<String> paramSetList = paramSets.indexOf(',') == -1 ? singletonList(paramSets) : StrUtils.splitSmart(paramSets, ',');
    for (String name : paramSetList) {
      RequestParams.VersionedParams params = requestParams.getParams(name, type);
      if (params == null) return defaults;
      if (type.equals(DEFAULTS)) {
        defaults = SolrParams.wrapDefaults(params, defaults);
      } else if (type.equals(INVARIANTS)) {
        defaults = SolrParams.wrapAppended(params, defaults);
      } else {
        defaults = SolrParams.wrapAppended(params, defaults);
      }
    }
    return defaults;
  }



  /**
   * SolrIndexSearch.numDocs(Query,Query) freaks out if the filtering
   * query is null, so we use this workarround.
   */
  public static int numDocs(SolrIndexSearcher s, Query q, Query f)
    throws IOException {

    return (null == f) ? s.getDocSet(q).size() : s.numDocs(q,f);

  }

  private static final Pattern SPLIT_PATTERN = Pattern.compile("[\\s,]+"); // space or comma

  /** Split a value between spaces and/or commas.  No need to trim anything. */
  public static String[] split(String value) {
    // TODO consider moving / adapting this into a new StrUtils.splitSmart variant?
    // TODO deprecate; it's only used by two callers?
    return SPLIT_PATTERN.split(value.trim());
  }

  /**
   * Pre-fetch documents into the index searcher's document cache.
   *
   * This is an entirely optional step which you might want to perform for
   * the following reasons:
   *
   * <ul>
   *     <li>Locates the document-retrieval costs in one spot, which helps
   *     detailed performance measurement</li>
   *
   *     <li>Determines a priori what fields will be needed to be fetched by
   *     various subtasks, like response writing and highlighting.  This
   *     minimizes the chance that many needed fields will be loaded lazily.
   *     (it is more efficient to load all the field we require normally).</li>
   * </ul>
   *
   * If lazy field loading is disabled, this method does nothing.
   */
  public static void optimizePreFetchDocs(ResponseBuilder rb,
                                          DocList docs,
                                          Query query,
                                          SolrQueryRequest req,
                                          SolrQueryResponse res) throws IOException {
    SolrIndexSearcher searcher = req.getSearcher();
    if(!searcher.getDocFetcher().isLazyFieldLoadingEnabled()) {
      // nothing to do
      return;
    }

    ReturnFields returnFields = res.getReturnFields();
    if(returnFields.getLuceneFieldNames() != null) {
      Set<String> fieldFilter = returnFields.getLuceneFieldNames();

      if (rb.doHighlights) {
        // copy return fields list
        fieldFilter = new HashSet<>(fieldFilter);
        // add highlight fields

        SolrHighlighter highlighter = HighlightComponent.getHighlighter(req.getCore());
        for (String field: highlighter.getHighlightFields(query, req, null))
          fieldFilter.add(field);

        // fetch unique key if one exists.
        SchemaField keyField = searcher.getSchema().getUniqueKeyField();
        if(null != keyField)
          fieldFilter.add(keyField.getName());
      }

      // get documents
      DocIterator iter = docs.iterator();
      for (int i=0; i<docs.size(); i++) {
        searcher.doc(iter.nextDoc(), fieldFilter);
      }

    }

  }


  public static Set<String> getDebugInterests(String[] params, ResponseBuilder rb){
    Set<String> debugInterests = new HashSet<>();
    if (params != null) {
      for (int i = 0; i < params.length; i++) {
        if (params[i].equalsIgnoreCase("all") || params[i].equalsIgnoreCase("true")){
          rb.setDebug(true);
          break;
          //still might add others
        } else if (params[i].equals(CommonParams.TIMING)){
          rb.setDebugTimings(true);
        } else if (params[i].equals(CommonParams.QUERY)){
          rb.setDebugQuery(true);
        } else if (params[i].equals(CommonParams.RESULTS)){
          rb.setDebugResults(true);
        } else if (params[i].equals(CommonParams.TRACK)){
          rb.setDebugTrack(true);
        }
      }
    }
    return debugInterests;
  }
  /**
   * <p>
   * Returns a NamedList containing many "standard" pieces of debugging
   * information.
   * </p>
   *
   * <ul>
   * <li>rawquerystring - the 'q' param exactly as specified by the client
   * </li>
   * <li>querystring - the 'q' param after any preprocessing done by the plugin
   * </li>
   * <li>parsedquery - the main query executed formated by the Solr
   *     QueryParsing utils class (which knows about field types)
   * </li>
   * <li>parsedquery_toString - the main query executed formatted by its
   *     own toString method (in case it has internal state Solr
   *     doesn't know about)
   * </li>
   * <li>explain - the list of score explanations for each document in
   *     results against query.
   * </li>
   * <li>otherQuery - the query string specified in 'explainOther' query param.
   * </li>
   * <li>explainOther - the list of score explanations for each document in
   *     results against 'otherQuery'
   * </li>
   * </ul>
   *
   * @param req the request we are dealing with
   * @param userQuery the users query as a string, after any basic
   *                  preprocessing has been done
   * @param query the query built from the userQuery
   *              (and perhaps other clauses) that identifies the main
   *              result set of the response.
   * @param results the main result set of the response
   * @return The debug info
   * @throws java.io.IOException if there was an IO error
   */
  @SuppressWarnings({"rawtypes"})
  public static NamedList doStandardDebug(
          SolrQueryRequest req,
          String userQuery,
          Query query,
          DocList results,
          boolean dbgQuery,
          boolean dbgResults)
          throws IOException
  {
    NamedList dbg = new SimpleOrderedMap();
    doStandardQueryDebug(req, userQuery, query, dbgQuery, dbg);
    doStandardResultsDebug(req, query, results, dbgResults, dbg);
    return dbg;
  }


  @SuppressWarnings({"unchecked"})
  public static void doStandardQueryDebug(
          SolrQueryRequest req,
          String userQuery,
          Query query,
          boolean dbgQuery,
          @SuppressWarnings({"rawtypes"})NamedList dbg)
  {
    if (dbgQuery) {
      /* userQuery may have been pre-processed .. expose that */
      dbg.add("rawquerystring", req.getParams().get(CommonParams.Q));
      dbg.add("querystring", userQuery);

     /* QueryParsing.toString isn't perfect, use it to see converted
      * values, use regular toString to see any attributes of the
      * underlying Query it may have missed.
      */
      dbg.add("parsedquery", QueryParsing.toString(query, req.getSchema()));
      dbg.add("parsedquery_toString", query.toString());
    }
  }

  @SuppressWarnings({"unchecked"})
  public static void doStandardResultsDebug(
          SolrQueryRequest req,
          Query query,
          DocList results,
          boolean dbgResults,
          @SuppressWarnings({"rawtypes"})NamedList dbg) throws IOException
  {
    if (dbgResults) {
      SolrIndexSearcher searcher = req.getSearcher();
      IndexSchema schema = searcher.getSchema();
      boolean explainStruct = req.getParams().getBool(CommonParams.EXPLAIN_STRUCT, false);

      if (results != null) {
        NamedList<Explanation> explain = getExplanations(query, results, searcher, schema);
        dbg.add("explain", explainStruct
            ? explanationsToNamedLists(explain)
            : explanationsToStrings(explain));
      }

      String otherQueryS = req.getParams().get(CommonParams.EXPLAIN_OTHER);
      if (otherQueryS != null && otherQueryS.length() > 0) {
        DocList otherResults = doSimpleQuery(otherQueryS, req, 0, 10);
        dbg.add("otherQuery", otherQueryS);
        NamedList<Explanation> explainO = getExplanations(query, otherResults, searcher, schema);
        dbg.add("explainOther", explainStruct
                ? explanationsToNamedLists(explainO)
                : explanationsToStrings(explainO));
      }
    }
  }

  public static NamedList<Object> explanationToNamedList(Explanation e) {
    NamedList<Object> out = new SimpleOrderedMap<>();

    out.add("match", e.isMatch());
    out.add("value", e.getValue());
    out.add("description", e.getDescription());

    Explanation[] details = e.getDetails();

    // short circut out
    if (0 == details.length) return out;

    List<NamedList<Object>> kids
      = new ArrayList<>(details.length);
    for (Explanation d : details) {
      kids.add(explanationToNamedList(d));
    }
    out.add("details", kids);

    return out;
  }

  public static NamedList<NamedList<Object>> explanationsToNamedLists
    (NamedList<Explanation> explanations) {

    NamedList<NamedList<Object>> out
      = new SimpleOrderedMap<>();
    for (Map.Entry<String,Explanation> entry : explanations) {
      out.add(entry.getKey(), explanationToNamedList(entry.getValue()));
    }
    return out;
  }

  /**
   * Generates an NamedList of Explanations for each item in a list of docs.
   *
   * @param query The Query you want explanations in the context of
   * @param docs The Documents you want explained relative that query
   */
  public static NamedList<Explanation> getExplanations
    (Query query,
     DocList docs,
     SolrIndexSearcher searcher,
     IndexSchema schema) throws IOException {

    NamedList<Explanation> explainList = new SimpleOrderedMap<>();
    DocIterator iterator = docs.iterator();
    for (int i=0; i<docs.size(); i++) {
      int id = iterator.nextDoc();

      Document doc = searcher.doc(id);
      String strid = schema.printableUniqueKey(doc);

      explainList.add(strid, searcher.explain(query, id) );
    }
    return explainList;
  }

  private static NamedList<String> explanationsToStrings
    (NamedList<Explanation> explanations) {

    NamedList<String> out = new SimpleOrderedMap<>();
    for (Map.Entry<String,Explanation> entry : explanations) {
      out.add(entry.getKey(), "\n"+entry.getValue().toString());
    }
    return out;
  }


  /**
   * Executes a basic query
   */
  public static DocList doSimpleQuery(String sreq,
                                      SolrQueryRequest req,
                                      int start, int limit) throws IOException {
    List<String> commands = StrUtils.splitSmart(sreq,';');

    String qs = commands.size() >= 1 ? commands.get(0) : "";
    try {
    Query query = QParser.getParser(qs, req).getQuery();

    // If the first non-query, non-filter command is a simple sort on an indexed field, then
    // we can use the Lucene sort ability.
    Sort sort = null;
    if (commands.size() >= 2) {
      sort = SortSpecParsing.parseSortSpec(commands.get(1), req).getSort();
    }

    DocList results = req.getSearcher().getDocList(query,(DocSet)null, sort, start, limit);
    return results;
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing query: " + qs);
    }

  }
  private static final Pattern whitespacePattern = Pattern.compile("\\s+");
  private static final Pattern caratPattern = Pattern.compile("\\^");
  private static final Pattern tildePattern = Pattern.compile("[~]");

  /**
   * Given a string containing fieldNames and boost info,
   * converts it to a Map from field name to boost info.
   *
   * <p>
   * Doesn't care if boost info is negative, you're on your own.
   * </p>
   * <p>
   * Doesn't care if boost info is missing, again: you're on your own.
   * </p>
   *
   * @param in a String like "fieldOne^2.3 fieldTwo fieldThree^-0.4"
   * @return Map of fieldOne =&gt; 2.3, fieldTwo =&gt; null, fieldThree =&gt; -0.4
   */
  public static Map<String,Float> parseFieldBoosts(String in) {
    return parseFieldBoosts(new String[]{in});
  }
  /**
   * Like <code>parseFieldBoosts(String)</code>, but parses all the strings
   * in the provided array (which may be null).
   *
   * @param fieldLists an array of Strings eg. <code>{"fieldOne^2.3", "fieldTwo", fieldThree^-0.4}</code>
   * @return Map of fieldOne =&gt; 2.3, fieldTwo =&gt; null, fieldThree =&gt; -0.4
   */
  public static Map<String,Float> parseFieldBoosts(String[] fieldLists) {
    if (null == fieldLists || 0 == fieldLists.length) {
      return new HashMap<>();
    }
    Map<String, Float> out = new HashMap<>(7);
    for (String in : fieldLists) {
      if (null == in) {
        continue;
      }
      in = in.trim();
      if(in.length()==0) {
        continue;
      }

      String[] bb = whitespacePattern.split(in);
      for (String s : bb) {
        String[] bbb = caratPattern.split(s);
        out.put(bbb[0], 1 == bbb.length ? null : Float.valueOf(bbb[1]));
      }
    }
    return out;
  }
  /**

  /**
   * Like {@link #parseFieldBoosts}, but allows for an optional slop value prefixed by "~".
   *
   * @param fieldLists - an array of Strings eg. <code>{"fieldOne^2.3", "fieldTwo", fieldThree~5^-0.4}</code>
   * @param wordGrams - (0=all words, 2,3 = shingle size)
   * @param defaultSlop - the default slop for this param
   * @return - FieldParams containing the fieldname,boost,slop,and shingle size
   */
  public static List<FieldParams> parseFieldBoostsAndSlop(String[] fieldLists,int wordGrams,int defaultSlop) {
    if (null == fieldLists || 0 == fieldLists.length) {
        return new ArrayList<>();
    }
    List<FieldParams> out = new ArrayList<>();
    for (String in : fieldLists) {
      if (null == in) {
        continue;
      }
      in = in.trim();
      if(in.length()==0) {
        continue;
      }
      String[] fieldConfigs = whitespacePattern.split(in);
      for (String s : fieldConfigs) {
        String[] fieldAndSlopVsBoost = caratPattern.split(s);
        String[] fieldVsSlop = tildePattern.split(fieldAndSlopVsBoost[0]);
        String field = fieldVsSlop[0];
        int slop  = (2 == fieldVsSlop.length) ? Integer.parseInt(fieldVsSlop[1]) : defaultSlop;
        float boost = (1 == fieldAndSlopVsBoost.length) ? 1  : Float.parseFloat(fieldAndSlopVsBoost[1]);
        FieldParams fp = new FieldParams(field,wordGrams,slop,boost);
        out.add(fp);
      }
    }
    return out;
  }

  /**
   * Checks the number of optional clauses in the query, and compares it
   * with the specification string to determine the proper value to use.
   * <p>
   * If mmAutoRelax=true, we'll perform auto relaxation of mm if tokens
   * are removed from some but not all DisMax clauses, as can happen when
   * stopwords or punctuation tokens are removed in analysis.
   * </p>
   * <p>
   * Details about the specification format can be found
   * <a href="doc-files/min-should-match.html">here</a>
   * </p>
   *
   * <p>A few important notes...</p>
   * <ul>
   * <li>
   * If the calculations based on the specification determine that no
   * optional clauses are needed, BooleanQuerysetMinMumberShouldMatch
   * will never be called, but the usual rules about BooleanQueries
   * still apply at search time (a BooleanQuery containing no required
   * clauses must still match at least one optional clause)
   * <li>
   * <li>
   * No matter what number the calculation arrives at,
   * BooleanQuery.setMinShouldMatch() will never be called with a
   * value greater then the number of optional clauses (or less then 1)
   * </li>
   * </ul>
   *
   * <p>:TODO: should optimize the case where number is same
   * as clauses to just make them all "required"
   * </p>
   *
   * @param q The query as a BooleanQuery.Builder
   * @param spec The mm spec
   * @param mmAutoRelax whether to perform auto relaxation of mm if tokens are removed from some but not all DisMax clauses
   */
  public static void setMinShouldMatch(BooleanQuery.Builder q, String spec, boolean mmAutoRelax) {

    int optionalClauses = 0;
    int maxDisjunctsSize = 0;
    int optionalDismaxClauses = 0;
    for (BooleanClause c : q.build().clauses()) {
      if (c.getOccur() == Occur.SHOULD) {
        if (mmAutoRelax && c.getQuery() instanceof DisjunctionMaxQuery) {
          int numDisjuncts = ((DisjunctionMaxQuery)c.getQuery()).getDisjuncts().size();
          if (numDisjuncts>maxDisjunctsSize) {
            maxDisjunctsSize = numDisjuncts;
            optionalDismaxClauses = 1;
          }
          else if (numDisjuncts == maxDisjunctsSize) {
            optionalDismaxClauses++;
          }
        } else {
          optionalClauses++;
        }
      }
    }

    int msm = calculateMinShouldMatch(optionalClauses + optionalDismaxClauses, spec);
    if (0 < msm) {
      q.setMinimumNumberShouldMatch(msm);
    }
  }

  public static void setMinShouldMatch(BooleanQuery.Builder q, String spec) {
    setMinShouldMatch(q, spec, false);
  }

  public static BooleanQuery setMinShouldMatch(BooleanQuery q, String spec) {
    return setMinShouldMatch(q, spec, false);
  }

  public static BooleanQuery setMinShouldMatch(BooleanQuery q, String spec, boolean mmAutoRelax) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (BooleanClause clause : q) {
      builder.add(clause);
    }
    setMinShouldMatch(builder, spec, mmAutoRelax);
    return builder.build();
  }

  // private static Pattern spaceAroundLessThanPattern = Pattern.compile("\\s*<\\s*");
  private static Pattern spaceAroundLessThanPattern = Pattern.compile("(\\s+<\\s*)|(\\s*<\\s+)");
  private static Pattern spacePattern = Pattern.compile(" ");
  private static Pattern lessThanPattern = Pattern.compile("<");

  /**
   * helper exposed for UnitTests
   * @see #setMinShouldMatch
   */
  static int calculateMinShouldMatch(int optionalClauseCount, String spec) {

    int result = optionalClauseCount;
    spec = spec.trim();

    if (-1 < spec.indexOf("<")) {
      /* we have conditional spec(s) */
      spec = spaceAroundLessThanPattern.matcher(spec).replaceAll("<");
      for (String s : spacePattern.split(spec)) {
        String[] parts = lessThanPattern.split(s,0);
        if (parts.length < 2) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Invalid 'mm' spec: '" + s + "'. Expecting values before and after '<'");
        }
        int upperBound = checkedParseInt(parts[0], "Invalid 'mm' spec. Expecting an integer.");
        if (optionalClauseCount <= upperBound) {
          return result;
        } else {
          result = calculateMinShouldMatch
            (optionalClauseCount, parts[1]);
        }
      }
      return result;
    }

    /* otherwise, simple expresion */

    if (-1 < spec.indexOf('%')) {
      /* percentage - assume the % was the last char.  If not, let Integer.parseInt fail. */
      spec = spec.substring(0,spec.length()-1);
      int percent = checkedParseInt(spec,
          "Invalid 'mm' spec. Expecting an integer.");
      float calc = (result * percent) * (1/100f);
      result = calc < 0 ? result + (int)calc : (int)calc;
    } else {
      int calc = checkedParseInt(spec, "Invalid 'mm' spec. Expecting an integer.");
      result = calc < 0 ? result + calc : calc;
    }

    return (optionalClauseCount < result ?
            optionalClauseCount : (result < 0 ? 0 : result));

  }

  /**
   * Wrapper of {@link Integer#parseInt(String)} that wraps any {@link NumberFormatException} in a
   * {@link SolrException} with HTTP 400 Bad Request status.
   *
   * @param input the string to parse
   * @param errorMessage the error message for any SolrException
   * @return the integer value of {@code input}
   * @throws SolrException when parseInt throws NumberFormatException
   */
  private static int checkedParseInt(String input, String errorMessage) {
    int percent;
    try {
      percent = Integer.parseInt(input);
    } catch (NumberFormatException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, errorMessage, e);
    }
    return percent;
  }


  /**
   * Recursively walks the "from" query pulling out sub-queries and
   * adding them to the "to" query.
   *
   * <p>
   * Boosts are multiplied as needed.  Sub-BooleanQueryies which are not
   * optional will not be flattened.  From will be mangled durring the walk,
   * so do not attempt to reuse it.
   * </p>
   */
  public static void flattenBooleanQuery(BooleanQuery.Builder to, BooleanQuery from) {
    flattenBooleanQuery(to, from, 1f);
  }

  private static void flattenBooleanQuery(BooleanQuery.Builder to, BooleanQuery from, float fromBoost) {

    for (BooleanClause clause : from.clauses()) {

      Query cq = clause.getQuery();
      float boost = fromBoost;
      while (cq instanceof BoostQuery) {
        BoostQuery bq = (BoostQuery) cq;
        cq = bq.getQuery();
        boost *= bq.getBoost();
      }

      if (cq instanceof BooleanQuery
          && !clause.isRequired()
          && !clause.isProhibited()) {

        /* we can recurse */
        flattenBooleanQuery(to, (BooleanQuery)cq, boost);

      } else {
        to.add(clause);
      }
    }
  }

  /**
   * Escapes all special characters except '"', '-', and '+'
   */
  public static CharSequence partialEscape(CharSequence s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' || c == '!' || c == '(' || c == ')' ||
          c == ':'  || c == '^' || c == '[' || c == ']' || c == '/' ||
          c == '{'  || c == '}' || c == '~' || c == '*' || c == '?'
          ) {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb;
  }

  // Pattern to detect dangling operator(s) at end of query
  // \s+[-+\s]+$
  private final static Pattern DANGLING_OP_PATTERN = Pattern.compile( "\\s+[-+\\s]+$" );
  // Pattern to detect consecutive + and/or - operators
  // \s+[+-](?:\s*[+-]+)+
  private final static Pattern CONSECUTIVE_OP_PATTERN = Pattern.compile( "\\s+[+-](?:\\s*[+-]+)+" );
  protected static final String UNKNOWN_VALUE = "Unknown";

  /**
   * Strips operators that are used illegally, otherwise returns its
   * input.  Some examples of illegal user queries are: "chocolate +-
   * chip", "chocolate - - chip", and "chocolate chip -".
   */
  public static CharSequence stripIllegalOperators(CharSequence s) {
    String temp = CONSECUTIVE_OP_PATTERN.matcher( s ).replaceAll( " " );
    return DANGLING_OP_PATTERN.matcher( temp ).replaceAll( "" );
  }

  /**
   * Returns its input if there is an even (ie: balanced) number of
   * '"' characters -- otherwise returns a String in which all '"'
   * characters are striped out.
   */
  public static CharSequence stripUnbalancedQuotes(CharSequence s) {
    int count = 0;
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == '\"') { count++; }
    }
    if (0 == (count & 1)) {
      return s;
    }
    return s.toString().replace("\"","");
  }

  /**
   * Adds to {@code dest} all the not-null elements of {@code entries} that have non-null names
   *
   * @param entries The array of entries to be added to the {@link NamedList} {@code dest}
   * @param dest The {@link NamedList} instance where the not-null elements of entries are added
   * @return Returns The {@code dest} input object
   */
  public static <T> NamedList<T> removeNulls(Map.Entry<String, T>[] entries, NamedList<T> dest) {
    for (int i=0; i<entries.length; i++) {
      Map.Entry<String, T> entry = entries[i];
      if (entry != null) {
        String key = entry.getKey();
        if (key != null) {
          dest.add(key, entry.getValue());
        }
      }
    }
    return dest;
  }

  /** Copies the given {@code namedList} assumed to have doc uniqueKey keyed data into {@code destArr}
   * at the position of the document in the response.  destArr is assumed to be the same size as
   * {@code resultIds} is.  {@code resultIds} comes from {@link ResponseBuilder#resultIds}.  If the doc key
   * isn't in {@code resultIds} then it is ignored.
   * Note: most likely you will call {@link #removeNulls(Map.Entry[], NamedList)} sometime after calling this. */
  public static void copyNamedListIntoArrayByDocPosInResponse(@SuppressWarnings({"rawtypes"})NamedList namedList, Map<Object, ShardDoc> resultIds,
                                                              Map.Entry<String, Object>[] destArr) {
    assert resultIds.size() == destArr.length;
    for (int i = 0; i < namedList.size(); i++) {
      String id = namedList.getName(i);
      // TODO: lookup won't work for non-string ids... String vs Float
      ShardDoc sdoc = resultIds.get(id);
      if (sdoc != null) { // maybe null when rb.onePassDistributedQuery
        int idx = sdoc.positionInResponse;
        destArr[idx] = new NamedList.NamedListEntry<>(id, namedList.getVal(i));
      }
    }
  }

  /**
   * A subclass of SolrQueryParser that supports aliasing fields for
   * constructing DisjunctionMaxQueries.
   */
  public static class DisjunctionMaxQueryParser extends SolrQueryParser {

    /** A simple container for storing alias info
     * @see #aliases
     */
    protected static class Alias {
      public float tie;
      public Map<String,Float> fields;
    }

    /**
     * Where we store a map from field name we expect to see in our query
     * string, to Alias object containing the fields to use in our
     * DisjunctionMaxQuery and the tiebreaker to use.
     */
    protected Map<String,Alias> aliases = new HashMap<>(3);
    public DisjunctionMaxQueryParser(QParser qp, String defaultField) {
      super(qp,defaultField);
      // don't trust that our parent class won't ever change its default
      setDefaultOperator(QueryParser.Operator.OR);
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
     * Delegates to the super class unless the field has been specified
     * as an alias -- in which case we recurse on each of
     * the aliased fields, and the results are composed into a
     * DisjunctionMaxQuery.  (so yes: aliases which point at other
     * aliases should work)
     */
    @Override
    protected Query getFieldQuery(String field, String queryText, boolean quoted, boolean raw)
        throws SyntaxError {

      if (aliases.containsKey(field)) {

        Alias a = aliases.get(field);

        List<Query> disjuncts = new ArrayList<>();
        for (Map.Entry<String, Float> entry : a.fields.entrySet()) {

          Query sub = getFieldQuery(entry.getKey(),queryText,quoted, false);
          if (null != sub) {
            if (null != entry.getValue()) {
              sub = new BoostQuery(sub, entry.getValue());
            }
            disjuncts.add(sub);
          }
        }
        return disjuncts.isEmpty()
            ? null
            : new DisjunctionMaxQuery(disjuncts, a.tie);

      } else {
        try {
          return super.getFieldQuery(field, queryText, quoted, raw);
        } catch (Exception e) {
          return null;
        }
      }
    }

  }

  /**
   * Determines the correct Sort based on the request parameter "sort"
   *
   * @return null if no sort is specified.
   */
  public static Sort getSort(SolrQueryRequest req) {

    String sort = req.getParams().get(CommonParams.SORT);
    if (null == sort || sort.equals("")) {
      return null;
    }

    SolrException sortE = null;
    Sort ss = null;
    try {
      ss = SortSpecParsing.parseSortSpec(sort, req).getSort();
    } catch (SolrException e) {
      sortE = e;
    }

    if ((null == ss) || (null != sortE)) {
      /* we definitely had some sort of sort string from the user,
       * but no SortSpec came out of it
       */
      log.warn("Invalid sort '{}' was specified, ignoring", sort, sortE);
      return null;
    }

    return ss;
  }

  /** Turns an array of query strings into a List of Query objects.
   *
   * @return null if no queries are generated
   */
  public static List<Query> parseQueryStrings(SolrQueryRequest req,
                                              String[] queries) throws SyntaxError {
    if (null == queries || 0 == queries.length) return null;
    List<Query> out = new ArrayList<>(queries.length);
    for (String q : queries) {
      if (null != q && 0 != q.trim().length()) {
        out.add(QParser.getParser(q, req).getQuery());
      }
    }
    return out;
  }

  public static void invokeSetters(Object bean, Iterable<Map.Entry<String,Object>> initArgs) {
    invokeSetters(bean, initArgs, false);
  }

  public static void invokeSetters(Object bean, Iterable<Map.Entry<String,Object>> initArgs, boolean lenient) {
    if (initArgs == null) return;
    final Class<?> clazz = bean.getClass();
    for (Map.Entry<String,Object> entry : initArgs) {
      String key = entry.getKey();
      String setterName = "set" + String.valueOf(Character.toUpperCase(key.charAt(0))) + key.substring(1);
      try {
        final Object val = entry.getValue();
        final Method method = findSetter(clazz, setterName, key, val.getClass(), lenient);
        if (method != null) {
          method.invoke(bean, val);
        }
      } catch (InvocationTargetException | IllegalAccessException e1) {
        if (lenient) {
          continue;
        }
        throw new RuntimeException("Error invoking setter " + setterName + " on class : " + clazz.getName(), e1);
      }
      catch (AssertionError ae) {
        throw new RuntimeException("Error invoking setter " + setterName + " on class : " + clazz.getName()+
            ". This might be a case of SOLR-12207", ae);
      }
    }
  }

  private static Method findSetter(Class<?> clazz, String setterName, String key, Class<?> paramClazz, boolean lenient) {
    BeanInfo beanInfo;
    try {
      beanInfo = Introspector.getBeanInfo(clazz);
    } catch (IntrospectionException ie) {
      if (lenient) {
        return null;
      }
      throw new RuntimeException("Error getting bean info for class : " + clazz.getName(), ie);
    }
    for (final boolean matchParamClazz: new boolean[]{true, false}) {
      for (final MethodDescriptor desc : beanInfo.getMethodDescriptors()) {
        final Method m = desc.getMethod();
        final Class<?> p[] = m.getParameterTypes();
        if (m.getName().equals(setterName) && p.length == 1 &&
            (!matchParamClazz || paramClazz.equals(p[0]))) {
          return m;
        }
      }
    }
    if (lenient) {
      return null;
    }
    throw new RuntimeException("No setter corrresponding to '" + key + "' in " + clazz.getName());
  }

   /**
   * Given the integer purpose of a request generates a readable value corresponding
   * the request purposes (there can be more than one on a single request). If
   * there is a purpose parameter present that's not known this method will
   * return {@value #UNKNOWN_VALUE}
   * @param reqPurpose Numeric request purpose
   * @return a comma separated list of purposes or {@value #UNKNOWN_VALUE}
   */
  public static String getRequestPurpose(Integer reqPurpose) {
      if (reqPurpose != null) {
          StringBuilder builder = new StringBuilder();
          for (Map.Entry<Integer, String>entry : purposes.entrySet()) {
              if ((reqPurpose & entry.getKey()) != 0) {
                  builder.append(entry.getValue()).append(',');
              }
          }
          if (builder.length() == 0) {
              return UNKNOWN_VALUE;
          }
          builder.setLength(builder.length() - 1);
          return builder.toString();
      }
      return UNKNOWN_VALUE;
  }

  private static final String[] purposeUnknown = new String[] { UNKNOWN_VALUE };

  /**
   * Given the integer purpose of a request generates a readable value corresponding
   * the request purposes (there can be more than one on a single request). If
   * there is a purpose parameter present that's not known this method will
   * return a 1-element array containing {@value #UNKNOWN_VALUE}
   * @param reqPurpose Numeric request purpose
   * @return an array of purpose names.
   */
  public static String[] getRequestPurposeNames(Integer reqPurpose) {
    if (reqPurpose != null) {
      int valid = 0;
      for (Map.Entry<Integer, String>entry : purposes.entrySet()) {
        if ((reqPurpose & entry.getKey()) != 0) {
          valid++;
        }
      }
      if (valid == 0) {
        return purposeUnknown;
      } else {
        String[] result = new String[valid];
        int i = 0;
        for (Map.Entry<Integer, String>entry : purposes.entrySet()) {
          if ((reqPurpose & entry.getKey()) != 0) {
            result[i] = entry.getValue();
            i++;
          }
        }
        return result;
      }
    }
    return purposeUnknown;
  }

}







