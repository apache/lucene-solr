/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrException;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.CacheRegenerator;

import org.apache.solr.request.StandardRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;

import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;

import org.apache.solr.util.StrUtils;
import org.apache.solr.util.NamedList;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.ConstantScoreRangeQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.analysis.Analyzer;

import org.xmlpull.v1.XmlPullParserException;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.io.IOException;
import java.net.URL;
    
/**
 * <p>Utilities that may be of use to RequestHandlers.</p>
 *
 * <p>
 * Many of these functions have code that was stolen/mutated from
 * StandardRequestHandler.
 * </p>
 *
 * <p>:TODO: refactor StandardRequestHandler to use these utilities</p>
 */
public class SolrPluginUtils {
    
  /** standard param for field list */
  public static String FL = CommonParams.FL;

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
   */
  public static String getParam(SolrQueryRequest req,
                                String param, String def) {
        
    String v = req.getParam(param);
    if (null == v || "".equals(v.trim())) {
      return def;
    }
    return v;
  }
    
  /**
   * Treats the param value as a Number, returns the default if nothing is
   * there or if it's not a number.
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
        

    
  private final static Pattern splitList=Pattern.compile(",| ");

  /**
   * Assumes the standard query param of "fl" to specify the return fields
   * @see #setReturnFields(String,SolrQueryResponse)
   */
  public static void setReturnFields(SolrQueryRequest req,
                                     SolrQueryResponse res) {

    setReturnFields(req.getParam(FL), res);
  }

  /**
   * Given a space seperated list of field names, sets the field list on the
   * SolrQueryResponse.
   */
  public static void setReturnFields(String fl,
                                     SolrQueryResponse res) {

    if (fl != null) {
      // TODO - this could become more efficient if widely used.
      // TODO - should field order be maintained?
      String[] flst = splitList.split(fl.trim(),0);
      if (flst.length > 0 && !(flst.length==1 && flst[0].length()==0)) {
        Set<String> set = new HashSet<String>();
        for (String fname : flst) set.add(fname);
        res.setReturnFields(set);
      }
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
   * @param results the main result set of hte response
   */
  public static NamedList doStandardDebug(SolrQueryRequest req,
                                          String userQuery,
                                          Query query,
                                          DocList results)
    throws IOException {
        
        
    String debug = req.getParam("debugQuery");

    NamedList dbg = null;
    if (debug!=null) {
      dbg = new NamedList();          

      /* userQuery may have been pre-processes .. expose that */
      dbg.add("rawquerystring",req.getQueryString());
      dbg.add("querystring",userQuery);

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
        
    NamedList explainList = new NamedList();
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
  public static void flatenBooleanQuery(BooleanQuery to, BooleanQuery from) {

    BooleanClause[] c = from.getClauses();
    for (int i = 0; i < c.length; i++) {
            
      Query ci = c[i].getQuery();
      ci.setBoost(ci.getBoost() * from.getBoost());
            
      if (ci instanceof BooleanQuery
          && !c[i].isRequired()
          && !c[i].isProhibited()) {
                
        /* we can recurse */
        flatenBooleanQuery(to, (BooleanQuery)ci);
                
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
   * A collection on common params, both for Plugin initialization and
   * for Requests.
   */
  public static class CommonParams {

    /** query and init param for tiebreaker value */
    public static String TIE = "tie";
    /** query and init param for query fields */
    public static String QF = "qf";
    /** query and init param for phrase boost fields */
    public static String PF = "pf";
    /** query and init param for MinShouldMatch specification */
    public static String MM = "mm";
    /** query and init param for Phrase Slop value */
    public static String PS = "ps";
    /** query and init param for boosting query */
    public static String BQ = "bq";
    /** query and init param for boosting functions */
    public static String BF = "bf";
    /** query and init param for filtering query */
    public static String FQ = "fq";
    /** query and init param for field list */
    public static String FL = "fl";
    /** query and init param for field list */
    public static String GEN = "gen";
        
    /** the default tie breaker to use in DisjunctionMaxQueries */
    public float tiebreaker = 0.0f;
    /** the default query fields to be used */
    public String qf = null;
    /** the default phrase boosting fields to be used */
    public String pf = null;
    /** the default min should match to be used */
    public String mm = "100%";
    /** the default phrase slop to be used */
    public int pslop = 0;
    /** the default boosting query to be used */
    public String bq = null;
    /** the default boosting functions to be used */
    public String bf = null;
    /** the default filtering query to be used */
    public String fq = null;
    /** the default field list to be used */
    public String fl = null;

    public CommonParams() {
      /* :NOOP: */
    }

    /** @see #setValues */
    public CommonParams(NamedList args) {
      this();
      setValues(args);
    }

    /**
     * Sets the params using values from a NamedList, usefull in the
     * init method for your handler.
     *
     * <p>
     * If any param is not of the expected type, a severe error is
     * logged,and the param is skipped.
     * </p>
     *
     * <p>
     * If any param is not of in the NamedList, it is skipped and the
     * old value is left alone.
     * </p>
     *
     */
    public void setValues(NamedList args) {

      Object tmp;

      tmp = args.get(TIE);
      if (null != tmp) {
        if (tmp instanceof Float) {
          tiebreaker = ((Float)tmp).floatValue();
        } else {
          SolrCore.log.severe("init param is not a float: " + TIE);
        }
      }

      tmp = args.get(QF);
      if (null != tmp) {
        if (tmp instanceof String) {
          qf = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + QF);
        }
      }

      tmp = args.get(PF);
      if (null != tmp) {
        if (tmp instanceof String) {
          pf = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + PF);
        }
      }

        
      tmp = args.get(MM);
      if (null != tmp) {
        if (tmp instanceof String) {
          mm = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + MM);
        }
      }
        
      tmp = args.get(PS);
      if (null != tmp) {
        if (tmp instanceof Integer) {
          pslop = ((Integer)tmp).intValue();
        } else {
          SolrCore.log.severe("init param is not an int: " + PS);
        }
      }

      tmp = args.get(BQ);
      if (null != tmp) {
        if (tmp instanceof String) {
          bq = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + BQ);
        }
      }
 
      tmp = args.get(BF);
      if (null != tmp) {
        if (tmp instanceof String) {
          bf = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + BF);
        }
      }
 
      tmp = args.get(FQ);
      if (null != tmp) {
        if (tmp instanceof String) {
          fq = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + FQ);
        }
      }
        
      tmp = args.get(FL);
      if (null != tmp) {
        if (tmp instanceof String) {
          fl = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + FL);
        }
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
