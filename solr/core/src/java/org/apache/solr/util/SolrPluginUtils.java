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

package org.apache.solr.util;

import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;
import org.apache.solr.update.DocumentBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

/**
 * <p>Utilities that may be of use to RequestHandlers.</p>
 *
 * <p>
 * Many of these functions have code that was stolen/mutated from
 * StandardRequestHandler.
 * </p>
 *
 * <p>:TODO: refactor StandardRequestHandler to use these utilities</p>
 *
 * <p>:TODO: Many "standard" functionality methods are not cognisant of
 * default parameter settings.
 */
public class SolrPluginUtils {

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
  public static void setDefaults(SolrQueryRequest req, SolrParams defaults,
                                 SolrParams appends, SolrParams invariants) {

      SolrParams p = req.getParams();
      p = SolrParams.wrapDefaults(p, defaults);
      p = SolrParams.wrapAppended(p, appends);
      p = SolrParams.wrapDefaults(invariants, p);

      req.setParams(p);
  }



  /**
   * SolrIndexSearch.numDocs(Query,Query) freaks out if the filtering
   * query is null, so we use this workarround.
   */
  public static int numDocs(SolrIndexSearcher s, Query q, Query f)
    throws IOException {

    return (null == f) ? s.getDocSet(q).size() : s.numDocs(q,f);

  }






  private final static Pattern splitList=Pattern.compile(",| ");

  /** Split a value that may contain a comma, space of bar separated list. */
  public static String[] split(String value){
     return splitList.split(value.trim(), 0);
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
    if(!searcher.enableLazyFieldLoading) {
      // nothing to do
      return;
    }

    ReturnFields returnFields = res.getReturnFields();
    if(returnFields.getLuceneFieldNames() != null) {
      Set<String> fieldFilter = returnFields.getLuceneFieldNames();

      if (rb.doHighlights) {
        // copy return fields list
        fieldFilter = new HashSet<String>(fieldFilter);
        // add highlight fields

        SolrHighlighter highlighter = HighlightComponent.getHighlighter(req.getCore());
        for (String field: highlighter.getHighlightFields(query, req, null))
          fieldFilter.add(field);

        // fetch unique key if one exists.
        SchemaField keyField = req.getSearcher().getSchema().getUniqueKeyField();
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
    Set<String> debugInterests = new HashSet<String>();
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
   * <li>parsedquery_toString - the main query executed formated by it's
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
  public static NamedList doStandardDebug(SolrQueryRequest req,
                                          String userQuery,
                                          Query query,
                                          DocList results, boolean dbgQuery, boolean dbgResults)
    throws IOException {

    NamedList dbg = null;

    dbg = new SimpleOrderedMap();

    SolrIndexSearcher searcher = req.getSearcher();
    IndexSchema schema = req.getSchema();

    boolean explainStruct
            = req.getParams().getBool(CommonParams.EXPLAIN_STRUCT, false);

    if (dbgQuery) {
      /* userQuery may have been pre-processed .. expose that */
      dbg.add("rawquerystring", req.getParams().get(CommonParams.Q));
      dbg.add("querystring", userQuery);

      /* QueryParsing.toString isn't perfect, use it to see converted
      * values, use regular toString to see any attributes of the
      * underlying Query it may have missed.
      */
      dbg.add("parsedquery", QueryParsing.toString(query, schema));
      dbg.add("parsedquery_toString", query.toString());
    }

    if (dbgResults) {
      NamedList<Explanation> explain
              = getExplanations(query, results, searcher, schema);
      dbg.add("explain", explainStruct ?
              explanationsToNamedLists(explain) :
              explanationsToStrings(explain));

      String otherQueryS = req.getParams().get(CommonParams.EXPLAIN_OTHER);
      if (otherQueryS != null && otherQueryS.length() > 0) {
        DocList otherResults = doSimpleQuery
                (otherQueryS, req, 0, 10);
        dbg.add("otherQuery", otherQueryS);
        NamedList<Explanation> explainO
                = getExplanations(query, otherResults, searcher, schema);
        dbg.add("explainOther", explainStruct ?
                explanationsToNamedLists(explainO) :
                explanationsToStrings(explainO));
      }
    }


    return dbg;
  }

  public static NamedList<Object> explanationToNamedList(Explanation e) {
    NamedList<Object> out = new SimpleOrderedMap<Object>();

    out.add("match", e.isMatch());
    out.add("value", e.getValue());
    out.add("description", e.getDescription());

    Explanation[] details = e.getDetails();

    // short circut out
    if (null == details || 0 == details.length) return out;

    List<NamedList<Object>> kids
      = new ArrayList<NamedList<Object>>(details.length);
    for (Explanation d : details) {
      kids.add(explanationToNamedList(d));
    }
    out.add("details", kids);

    return out;
  }

  public static NamedList<NamedList<Object>> explanationsToNamedLists
    (NamedList<Explanation> explanations) {

    NamedList<NamedList<Object>> out
      = new SimpleOrderedMap<NamedList<Object>>();
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

    NamedList<Explanation> explainList = new SimpleOrderedMap<Explanation>();
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

    NamedList<String> out = new SimpleOrderedMap<String>();
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
    Query query = QParser.getParser(qs, null, req).getQuery();

    // If the first non-query, non-filter command is a simple sort on an indexed field, then
    // we can use the Lucene sort ability.
    Sort sort = null;
    if (commands.size() >= 2) {
      sort = QueryParsing.parseSort(commands.get(1), req);
    }

    DocList results = req.getSearcher().getDocList(query,(DocSet)null, sort, start, limit);
    return results;
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing query: " + qs);
    }

  }

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
      return new HashMap<String,Float>();
    }
    Map<String, Float> out = new HashMap<String,Float>(7);
    for (String in : fieldLists) {
      if (null == in || "".equals(in.trim()))
        continue;
      String[] bb = in.trim().split("\\s+");
      for (String s : bb) {
        String[] bbb = s.split("\\^");
        out.put(bbb[0], 1 == bbb.length ? null : Float.valueOf(bbb[1]));
      }
    }
    return out;
  }

  /**
   * Checks the number of optional clauses in the query, and compares it
   * with the specification string to determine the proper value to use.
   *
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
   */
  public static void setMinShouldMatch(BooleanQuery q, String spec) {

    int optionalClauses = 0;
    for (BooleanClause c : q.clauses()) {
      if (c.getOccur() == Occur.SHOULD) {
        optionalClauses++;
      }
    }

    int msm = calculateMinShouldMatch(optionalClauses, spec);
    if (0 < msm) {
      q.setMinimumNumberShouldMatch(msm);
    }
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
        int upperBound = Integer.parseInt(parts[0]);
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
      int percent = Integer.parseInt(spec);
      float calc = (result * percent) * (1/100f);
      result = calc < 0 ? result + (int)calc : (int)calc;
    } else {
      int calc = Integer.parseInt(spec);
      result = calc < 0 ? result + calc : calc;
    }

    return (optionalClauseCount < result ?
            optionalClauseCount : (result < 0 ? 0 : result));

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
  public static void flattenBooleanQuery(BooleanQuery to, BooleanQuery from) {

    for (BooleanClause clause : from.clauses()) {

      Query cq = clause.getQuery();
      cq.setBoost(cq.getBoost() * from.getBoost());

      if (cq instanceof BooleanQuery
          && !clause.isRequired()
          && !clause.isProhibited()) {

        /* we can recurse */
        flattenBooleanQuery(to, (BooleanQuery)cq);

      } else {
        to.add(clause);
      }
    }
  }

  /**
   * Escapes all special characters except '"', '-', and '+'
   *
   * @see QueryParser#escape
   */
  public static CharSequence partialEscape(CharSequence s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' || c == '!' || c == '(' || c == ')' ||
          c == ':'  || c == '^' || c == '[' || c == ']' ||
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

  /**
   * Strips operators that are used illegally, otherwise reuturns it's
   * input.  Some examples of illegal user queries are: "chocolate +-
   * chip", "chocolate - - chip", and "chocolate chip -".
   */
  public static CharSequence stripIllegalOperators(CharSequence s) {
    String temp = CONSECUTIVE_OP_PATTERN.matcher( s ).replaceAll( " " );
    return DANGLING_OP_PATTERN.matcher( temp ).replaceAll( "" );
  }

  /**
   * Returns it's input if there is an even (ie: balanced) number of
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

  public static NamedList removeNulls(NamedList nl) {
    for (int i=0; i<nl.size(); i++) {
      if (nl.getName(i)==null) {
        NamedList newList = nl instanceof SimpleOrderedMap ? new SimpleOrderedMap() : new NamedList();
        for (int j=0; j<nl.size(); j++) {
          String n = nl.getName(j);
          if (n != null) {
            newList.add(n, nl.getVal(j));
          }
        }
        return newList;
      }
    }
    return nl;
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
    protected Map<String,Alias> aliases = new HashMap<String,Alias>(3);
    public DisjunctionMaxQueryParser(QParser qp, String defaultField) {
      super(qp,defaultField);
      // don't trust that our parent class won't ever change it's default
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
    protected Query getFieldQuery(String field, String queryText, boolean quoted)
      throws ParseException {

      if (aliases.containsKey(field)) {

        Alias a = aliases.get(field);
        DisjunctionMaxQuery q = new DisjunctionMaxQuery(a.tie);

        /* we might not get any valid queries from delegation,
         * in which case we should return null
         */
        boolean ok = false;

        for (String f : a.fields.keySet()) {

          Query sub = getFieldQuery(f,queryText,quoted);
          if (null != sub) {
            if (null != a.fields.get(f)) {
              sub.setBoost(a.fields.get(f));
            }
            q.add(sub);
            ok = true;
          }
        }
        return ok ? q : null;

      } else {
        try {
          return super.getFieldQuery(field, queryText, quoted);
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
      ss = QueryParsing.parseSort(sort, req);
    } catch (SolrException e) {
      sortE = e;
    }

    if ((null == ss) || (null != sortE)) {
      /* we definitely had some sort of sort string from the user,
       * but no SortSpec came out of it
       */
      SolrCore.log.warn("Invalid sort \""+sort+"\" was specified, ignoring", sortE);
      return null;
    }

    return ss;
  }

  /** Turns an array of query strings into a List of Query objects.
   *
   * @return null if no queries are generated
   */
  public static List<Query> parseQueryStrings(SolrQueryRequest req,
                                              String[] queries) throws ParseException {
    if (null == queries || 0 == queries.length) return null;
    List<Query> out = new ArrayList<Query>(queries.length);
    for (String q : queries) {
      if (null != q && 0 != q.trim().length()) {
        out.add(QParser.getParser(q, null, req).getQuery());
      }
    }
    return out;
  }

  /**
   * A CacheRegenerator that can be used whenever the items in the cache
   * are not dependant on the current searcher.
   *
   * <p>
   * Flat out copies the oldKey=&gt;oldVal pair into the newCache
   * </p>
   */
  public static class IdentityRegenerator implements CacheRegenerator {
    public boolean regenerateItem(SolrIndexSearcher newSearcher,
                                  SolrCache newCache,
                                  SolrCache oldCache,
                                  Object oldKey,
                                  Object oldVal)
      throws IOException {

      newCache.put(oldKey,oldVal);
      return true;
    }
  }

  /**
   * Convert a DocList to a SolrDocumentList
   *
   * The optional param "ids" is populated with the lucene document id
   * for each SolrDocument.
   *
   * @param docs The {@link org.apache.solr.search.DocList} to convert
   * @param searcher The {@link org.apache.solr.search.SolrIndexSearcher} to use to load the docs from the Lucene index
   * @param fields The names of the Fields to load
   * @param ids A map to store the ids of the docs
   * @return The new {@link org.apache.solr.common.SolrDocumentList} containing all the loaded docs
   * @throws java.io.IOException if there was a problem loading the docs
   * @since solr 1.4
   */
  public static SolrDocumentList docListToSolrDocumentList(
      DocList docs,
      SolrIndexSearcher searcher,
      Set<String> fields,
      Map<SolrDocument, Integer> ids ) throws IOException
  {
    DocumentBuilder db = new DocumentBuilder(searcher.getSchema());
    SolrDocumentList list = new SolrDocumentList();
    list.setNumFound(docs.matches());
    list.setMaxScore(docs.maxScore());
    list.setStart(docs.offset());

    DocIterator dit = docs.iterator();

    while (dit.hasNext()) {
      int docid = dit.nextDoc();

      Document luceneDoc = searcher.doc(docid, fields);
      SolrDocument doc = new SolrDocument();
      db.loadStoredFields(doc, luceneDoc);

      // this may be removed if XMLWriter gets patched to
      // include score from doc iterator in solrdoclist
      if (docs.hasScores()) {
        doc.addField("score", dit.score());
      } else {
        doc.addField("score", 0.0f);
      }

      list.add( doc );

      if( ids != null ) {
        ids.put( doc, new Integer(docid) );
      }
    }
    return list;
  }


  public static void invokeSetters(Object bean, NamedList initArgs) {
    if (initArgs == null) return;
    Class clazz = bean.getClass();
    Method[] methods = clazz.getMethods();
    Iterator<Map.Entry<String, Object>> iterator = initArgs.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Object> entry = iterator.next();
      String key = entry.getKey();
      String setterName = "set" + String.valueOf(Character.toUpperCase(key.charAt(0))) + key.substring(1);
      Method method = null;
      try {
        for (Method m : methods) {
          if (m.getName().equals(setterName) && m.getParameterTypes().length == 1) {
            method = m;
            break;
          }
        }
        if (method == null) {
          throw new RuntimeException("no setter corrresponding to '" + key + "' in " + clazz.getName());
        }
        Class pClazz = method.getParameterTypes()[0];
        Object val = entry.getValue();
        method.invoke(bean, val);
      } catch (InvocationTargetException e1) {
        throw new RuntimeException("Error invoking setter " + setterName + " on class : " + clazz.getName(), e1);
      } catch (IllegalAccessException e1) {
        throw new RuntimeException("Error invoking setter " + setterName + " on class : " + clazz.getName(), e1);
      }
    }
  }
  
}







