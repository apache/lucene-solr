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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.highlight.*;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.request.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.DefaultSolrParams;
import org.apache.solr.request.AppendedSolrParams;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.logging.Level;
import java.util.regex.Pattern;
    
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
   * Set defaults on a SolrQueryRequest.
   *
   * RequestHandlers can use this method to ensure their defaults are
   * visible to other components such as the response writer
   */
  public static void setDefaults(SolrQueryRequest req, SolrParams defaults) {
    setDefaults(req, defaults, null, null);
  }
  
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
      if (defaults != null) {
        p = new DefaultSolrParams(p,defaults);
      }
      if (appends != null) {
        p = new AppendedSolrParams(p,appends);
      }
      if (invariants != null) {
        p = new DefaultSolrParams(invariants,p);
      }
      req.setParams(p);
  }


  /** standard param for field list */
  @Deprecated
  public static String FL = SolrParams.FL;

  /**
   * SolrIndexSearch.numDocs(Query,Query) freaks out if the filtering
   * query is null, so we use this workarround.
   */
  public static int numDocs(SolrIndexSearcher s, Query q, Query f)
    throws IOException {

    return (null == f) ? s.getDocSet(q).size() : s.numDocs(q,f);
        
  }
    
  /**
   * Returns the param, or the default if it's empty or not specified.
   * @deprecated use SolrParam.get(String,String)
   */
  public static String getParam(SolrQueryRequest req,
                                String param, String def) {
        
    String v = req.getParam(param);
    // Note: parameters passed but given only white-space value are
    // considered equvalent to passing nothing for that parameter.
    if (null == v || "".equals(v.trim())) {
      return def;
    }
    return v;
  }
    
  /**
   * Treats the param value as a Number, returns the default if nothing is
   * there or if it's not a number.
   * @deprecated use SolrParam.getFloat(String,float)
   */
  public static Number getNumberParam(SolrQueryRequest req,
                                      String param, Number def) {
        
    Number r = def;
    String v = req.getParam(param);
    if (null == v || "".equals(v.trim())) {
      return r;
    }
    try {
      r = new Float(v);
    } catch (NumberFormatException e) {
      /* :NOOP" */
    }
    return r;
  }
        
  /**
   * Treats parameter value as a boolean.  The string 'false' is false; 
   * any other non-empty string is true.
   * @deprecated use SolrParam.getBool(String,boolean)
   */
  public static boolean getBooleanParam(SolrQueryRequest req,
                                       String param, boolean def) {        
    String v = req.getParam(param);
    if (null == v || "".equals(v.trim())) {
      return def;
    }
    return !"false".equals(v.trim());
  }
    
  private final static Pattern splitList=Pattern.compile(",| ");
  
  /** Split a value that may contain a comma, space of bar separated list. */
  public static String[] split(String value){
     return splitList.split(value.trim(), 0);
  }

  /**
   * Assumes the standard query param of "fl" to specify the return fields
   * @see #setReturnFields(String,SolrQueryResponse)
   */
  public static int setReturnFields(SolrQueryRequest req,
                                    SolrQueryResponse res) {

    return setReturnFields(req.getParam(FL), res);
  }

  /**
   * Given a space seperated list of field names, sets the field list on the
   * SolrQueryResponse.
   *
   * @return bitfield of SolrIndexSearcher flags that need to be set
   */
  public static int setReturnFields(String fl,
                                    SolrQueryResponse res) {
    int flags = 0;
    if (fl != null) {
      // TODO - this could become more efficient if widely used.
      // TODO - should field order be maintained?
      String[] flst = split(fl);
      if (flst.length > 0 && !(flst.length==1 && flst[0].length()==0)) {
        Set<String> set = new HashSet<String>();
        for (String fname : flst) {
          if("score".equalsIgnoreCase(fname))
            flags |= SolrIndexSearcher.GET_SCORES;
          set.add(fname);
        }
        res.setReturnFields(set);
      }
    }
    return flags;
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
  public static void optimizePreFetchDocs(DocList docs,
                                          Query query,
                                          SolrQueryRequest req,
                                          SolrQueryResponse res) throws IOException {
    SolrIndexSearcher searcher = req.getSearcher();
    if(!searcher.enableLazyFieldLoading) {
      // nothing to do
      return;
    }

    Set<String> fieldFilter = null;
    Set<String> returnFields = res.getReturnFields();
    if(returnFields != null) {
      // copy return fields list
      fieldFilter = new HashSet<String>(returnFields);
      // add highlight fields
      if(HighlightingUtils.isHighlightingEnabled(req)) {
        for(String field: HighlightingUtils.getHighlightFields(query, req, null)) 
          fieldFilter.add(field);        
      }
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
   * <li>expain - the list of score explanations for each document in
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
   * @deprecated Use doStandardDebug(SolrQueryRequest,String,Query,DocList) with setDefaults
   */
  public static NamedList doStandardDebug(SolrQueryRequest req,
                                          String userQuery,
                                          Query query,
                                          DocList results,
                                          CommonParams params)
    throws IOException {
        
    String debug = getParam(req, SolrParams.DEBUG_QUERY, params.debugQuery);

    NamedList dbg = null;
    if (debug!=null) {
      dbg = new SimpleOrderedMap();

      /* userQuery may have been pre-processes .. expose that */
      dbg.add("rawquerystring", req.getQueryString());
      dbg.add("querystring", userQuery);

      /* QueryParsing.toString isn't perfect, use it to see converted
       * values, use regular toString to see any attributes of the
       * underlying Query it may have missed.
       */
      dbg.add("parsedquery",QueryParsing.toString(query, req.getSchema()));
      dbg.add("parsedquery_toString", query.toString());
            
      dbg.add("explain", getExplainList
              (query, results, req.getSearcher(), req.getSchema()));
      String otherQueryS = req.getParam("explainOther");
      if (otherQueryS != null && otherQueryS.length() > 0) {
        DocList otherResults = doSimpleQuery
          (otherQueryS,req.getSearcher(), req.getSchema(),0,10);
        dbg.add("otherQuery",otherQueryS);
        dbg.add("explainOther", getExplainList
                (query, otherResults,
                 req.getSearcher(),
                 req.getSchema()));
      }
    }

    return dbg;
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
   * <li>expain - the list of score explanations for each document in
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
   */
  public static NamedList doStandardDebug(SolrQueryRequest req,
                                          String userQuery,
                                          Query query,
                                          DocList results)
    throws IOException {

    String debug = req.getParam(SolrParams.DEBUG_QUERY);

    NamedList dbg = null;
    if (debug!=null) {
      dbg = new SimpleOrderedMap();

      /* userQuery may have been pre-processes .. expose that */
      dbg.add("rawquerystring", req.getQueryString());
      dbg.add("querystring", userQuery);

      /* QueryParsing.toString isn't perfect, use it to see converted
       * values, use regular toString to see any attributes of the
       * underlying Query it may have missed.
       */
      dbg.add("parsedquery",QueryParsing.toString(query, req.getSchema()));
      dbg.add("parsedquery_toString", query.toString());

      dbg.add("explain", getExplainList
              (query, results, req.getSearcher(), req.getSchema()));
      String otherQueryS = req.getParam("explainOther");
      if (otherQueryS != null && otherQueryS.length() > 0) {
        DocList otherResults = doSimpleQuery
          (otherQueryS,req.getSearcher(), req.getSchema(),0,10);
        dbg.add("otherQuery",otherQueryS);
        dbg.add("explainOther", getExplainList
                (query, otherResults,
                 req.getSearcher(),
                 req.getSchema()));
      }
    }

    return dbg;
  }



  /**
   * Generates an list of Explanations for each item in a list of docs.
   *
   * @param query The Query you want explanations in the context of
   * @param docs The Documents you want explained relative that query
   */
  public static NamedList getExplainList(Query query, DocList docs,
                                         SolrIndexSearcher searcher,
                                         IndexSchema schema)
    throws IOException {
        
    NamedList explainList = new SimpleOrderedMap();
    DocIterator iterator = docs.iterator();
    for (int i=0; i<docs.size(); i++) {
      int id = iterator.nextDoc();

      Explanation explain = searcher.explain(query, id);

      Document doc = searcher.doc(id);
      String strid = schema.printableUniqueKey(doc);
      String docname = "";
      if (strid != null) docname="id="+strid+",";
      docname = docname + "internal_docid="+id;

      explainList.add(docname, "\n" +explain.toString());
    }
    return explainList;
  }

  /**
   * Executes a basic query in lucene syntax
   */
  public static DocList doSimpleQuery(String sreq,
                                      SolrIndexSearcher searcher,
                                      IndexSchema schema,
                                      int start, int limit) throws IOException {
    List<String> commands = StrUtils.splitSmart(sreq,';');

    String qs = commands.size() >= 1 ? commands.get(0) : "";
    Query query = QueryParsing.parseQuery(qs, schema);

    // If the first non-query, non-filter command is a simple sort on an indexed field, then
    // we can use the Lucene sort ability.
    Sort sort = null;
    if (commands.size() >= 2) {
      QueryParsing.SortSpec sortSpec = QueryParsing.parseSort(commands.get(1), schema);
      if (sortSpec != null) {
        sort = sortSpec.getSort();
        if (sortSpec.getCount() >= 0) {
          limit = sortSpec.getCount();
        }
      }
    }

    DocList results = searcher.getDocList(query,(DocSet)null, sort, start, limit);
    return results;
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

    if (null == in || "".equals(in.trim())) {
      return new HashMap<String,Float>();
    }
        
    String[] bb = in.trim().split("\\s+");
    Map<String, Float> out = new HashMap<String,Float>(7);
    for (String s : bb) {
      String[] bbb = s.split("\\^");
      out.put(bbb[0], 1 == bbb.length ? null : Float.valueOf(bbb[1]));
    }
    return out;
  }

  /**
   * Given a string containing functions with optional boosts, returns
   * an array of Queries representing those functions with the specified
   * boosts.
   * <p>
   * NOTE: intra-function whitespace is not allowed.
   * </p>
   * @see #parseFieldBoosts
   */
  public static List<Query> parseFuncs(IndexSchema s, String in)
    throws ParseException {
  
    Map<String,Float> ff = parseFieldBoosts(in);
    List<Query> funcs = new ArrayList<Query>(ff.keySet().size());
    for (String f : ff.keySet()) {
      Query fq = QueryParsing.parseFunction(f, s);
      Float b = ff.get(f);
      if (null != b) {
        fq.setBoost(b);
      }
      funcs.add(fq);
    }
    return funcs;
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
    for (BooleanClause c : q.getClauses()) {
      if (c.getOccur() == Occur.SHOULD) {
        optionalClauses++;
      }
    }

    int msm = calculateMinShouldMatch(optionalClauses, spec);
    if (0 < msm) {
      q.setMinimumNumberShouldMatch(msm);
    }
  }

  /**
   * helper exposed for UnitTests
   * @see #setMinShouldMatch
   */
  static int calculateMinShouldMatch(int optionalClauseCount, String spec) {

    int result = optionalClauseCount;
        

    if (-1 < spec.indexOf("<")) {
      /* we have conditional spec(s) */
            
      for (String s : spec.trim().split(" ")) {
        String[] parts = s.split("<");
        int upperBound = (new Integer(parts[0])).intValue();
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

    if (-1 < spec.indexOf("%")) {
      /* percentage */
      int percent = new Integer(spec.replace("%","")).intValue();
      float calc = (result * percent) / 100f;
      result = calc < 0 ? result + (int)calc : (int)calc;
    } else {
      int calc = (new Integer(spec)).intValue();
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

    BooleanClause[] c = from.getClauses();
    for (int i = 0; i < c.length; i++) {
            
      Query ci = c[i].getQuery();
      ci.setBoost(ci.getBoost() * from.getBoost());
            
      if (ci instanceof BooleanQuery
          && !c[i].isRequired()
          && !c[i].isProhibited()) {
                
        /* we can recurse */
        flattenBooleanQuery(to, (BooleanQuery)ci);
                
      } else {
        to.add(c[i]);
      }
    }
  }

  /**
   * Escapes all special characters except '"', '-', and '+'
   *
   * @see QueryParser#escape
   */
  public static CharSequence partialEscape(CharSequence s) {
    StringBuffer sb = new StringBuffer();
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
        
    public DisjunctionMaxQueryParser(IndexSchema s, String defaultField) {
      super(s,defaultField);
    }
    public DisjunctionMaxQueryParser(IndexSchema s) {
      this(s,null);
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
    protected Query getFieldQuery(String field, String queryText)
      throws ParseException {
            
      if (aliases.containsKey(field)) {
                
        Alias a = aliases.get(field);
        DisjunctionMaxQuery q = new DisjunctionMaxQuery(a.tie);

        /* we might not get any valid queries from delegation,
         * in which we should return null
         */
        boolean ok = false;
                
        for (String f : a.fields.keySet()) {

          Query sub = getFieldQuery(f,queryText);
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
        return super.getFieldQuery(field, queryText);
      }
    }
        
  }

  /**
   * Determines the correct Sort based on the request parameter "sort"
   *
   * @return null if no sort is specified.
   */
  public static Sort getSort(SolrQueryRequest req) {

    String sort = req.getParam("sort");
    if (null == sort || sort.equals("")) {
      return null;
    }

    SolrException sortE = null;
    QueryParsing.SortSpec ss = null;
    try {
      ss = QueryParsing.parseSort(sort, req.getSchema());
    } catch (SolrException e) {
      sortE = e;
    }

    if ((null == ss) || (null != sortE)) {
      /* we definitely had some sort of sort string from the user,
       * but no SortSpec came out of it
       */
      SolrCore.log.log(Level.WARNING,"Invalid sort \""+sort+"\" was specified, ignoring", sortE);
      return null;
    }
        
    return ss.getSort();
  }

  /**
   * Builds a list of Query objects that should be used to filter results
   * @see SolrParams#FQ
   * @return null if no filter queries
   */
  public static List<Query> parseFilterQueries(SolrQueryRequest req) throws ParseException {
    String[] in = req.getParams().getParams(SolrParams.FQ);
    
    if (null == in || 0 == in.length) return null;

    List<Query> out = new LinkedList<Query>();
    SolrIndexSearcher s = req.getSearcher();
    /* Ignore SolrParams.DF - could have init param FQs assuming the
     * schema default with query param DF intented to only affect Q.
     * If user doesn't want schema default, they should be explicit in the FQ.
     */
    SolrQueryParser qp = new SolrQueryParser(s.getSchema(), null);
    for (String q : in) {
      if (null != q && 0 != q.trim().length()) {
        out.add(qp.parse(q));
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

}

/** 
 * Helper class which creates a single TokenStream out of values from a 
 * multi-valued field.
 */
class MultiValueTokenStream extends TokenStream {
  private String fieldName;
  private String[] values;
  private Analyzer analyzer;
  private int curIndex;                  // next index into the values array
  private int curOffset;                 // offset into concatenated string
  private TokenStream currentStream;     // tokenStream currently being iterated
  private boolean orderTokenOffsets;

  /** Constructs a TokenStream for consecutively-analyzed field values
   *
   * @param fieldName name of the field
   * @param values array of field data
   * @param analyzer analyzer instance
   */
  public MultiValueTokenStream(String fieldName, String[] values, 
                               Analyzer analyzer, boolean orderTokenOffsets) {
    this.fieldName = fieldName;
    this.values = values;
    this.analyzer = analyzer;
    curIndex = -1;
    curOffset = 0;
    currentStream = null;
    this.orderTokenOffsets=orderTokenOffsets;
  }

  /** Returns the next token in the stream, or null at EOS. */
  public Token next() throws IOException {
    int extra = 0;
    if(currentStream == null) {
      curIndex++;        
      if(curIndex < values.length) {
        currentStream = analyzer.tokenStream(fieldName, 
                                             new StringReader(values[curIndex]));
        if (orderTokenOffsets) currentStream = new TokenOrderingFilter(currentStream,10);
        // add extra space between multiple values
        if(curIndex > 0) 
          extra = analyzer.getPositionIncrementGap(fieldName);
      } else {
        return null;
      }
    }
    Token nextToken = currentStream.next();
    if(nextToken == null) {
      curOffset += values[curIndex].length();
      currentStream = null;
      return next();
    }
    // create an modified token which is the offset into the concatenated
    // string of all values
    Token offsetToken = new Token(nextToken.termText(), 
                                  nextToken.startOffset() + curOffset,
                                  nextToken.endOffset() + curOffset);
    offsetToken.setPositionIncrement(nextToken.getPositionIncrement() + extra*10);
    return offsetToken;
  }

  /**
   * Returns all values as a single String into which the Tokens index with
   * their offsets.
   */
  public String asSingleValue() {
    StringBuilder sb = new StringBuilder();
    for(String str : values)
      sb.append(str);
    return sb.toString();
  }

}

/**
 * A simple modification of SimpleFragmenter which additionally creates new
 * fragments when an unusually-large position increment is encountered
 * (this behaves much better in the presence of multi-valued fields).
 */
class GapFragmenter extends SimpleFragmenter {
  public static final int INCREMENT_THRESHOLD = 50;
  protected int fragOffsetAccum = 0;
  
  public GapFragmenter() {
  }
  
  public GapFragmenter(int fragsize) {
     super(fragsize);
  }
  
  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.TextFragmenter#start(java.lang.String)
   */
  public void start(String originalText) {
    fragOffsetAccum = 0;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.TextFragmenter#isNewFragment(org.apache.lucene.analysis.Token)
   */
  public boolean isNewFragment(Token token) {
    boolean isNewFrag = 
      token.endOffset() >= fragOffsetAccum + getFragmentSize() ||
      token.getPositionIncrement() > INCREMENT_THRESHOLD;
    if(isNewFrag) {
        fragOffsetAccum += token.endOffset() - fragOffsetAccum;
    }
    return isNewFrag;
  }
}


/** Orders Tokens in a window first by their startOffset ascending.
 * endOffset is currently ignored.
 * This is meant to work around fickleness in the highlighter only.  It
 * can mess up token positions and should not be used for indexing or querying.
 */
class TokenOrderingFilter extends TokenFilter {
  private final int windowSize;
  private final LinkedList<Token> queue = new LinkedList<Token>();
  private boolean done=false;

  protected TokenOrderingFilter(TokenStream input, int windowSize) {
    super(input);
    this.windowSize = windowSize;
  }

  public Token next() throws IOException {
    while (!done && queue.size() < windowSize) {
      Token newTok = input.next();
      if (newTok==null) {
        done=true;
        break;
      }

      // reverse iterating for better efficiency since we know the
      // list is already sorted, and most token start offsets will be too.
      ListIterator<Token> iter = queue.listIterator(queue.size());
      while(iter.hasPrevious()) {
        if (newTok.startOffset() >= iter.previous().startOffset()) {
          // insertion will be before what next() would return (what
          // we just compared against), so move back one so the insertion
          // will be after.
          iter.next();
          break;
        }
      }
      iter.add(newTok);
    }

    return queue.isEmpty() ? null : queue.removeFirst();
  }
}
