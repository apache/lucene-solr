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
package org.apache.solr.search.facet;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.*;
import org.apache.solr.search.join.GraphQuery;
import org.apache.solr.search.join.GraphQueryParser;

import java.io.IOException;
import java.util.*;

import static org.apache.solr.common.params.CommonParams.SORT;
import static org.apache.solr.search.facet.FacetRequest.RefineMethod.NONE;


public abstract class FacetRequest {

  public static enum SortDirection {
    asc(-1) ,
    desc(1);

    private final int multiplier;
    private SortDirection(int multiplier) {
      this.multiplier = multiplier;
    }

    // asc==-1, desc==1
    public int getMultiplier() {
      return multiplier;
    }
  }

  public static enum RefineMethod {
    NONE,
    SIMPLE;
    // NONE is distinct from null since we may want to know if refinement was explicitly turned off.
    public static FacetRequest.RefineMethod fromObj(Object method) {
      if (method == null) return null;
      if (method instanceof  Boolean) {
        return ((Boolean)method) ? SIMPLE : NONE;
      }
      if ("simple".equals(method)) {
        return SIMPLE;
      } else if ("none".equals(method)) {
        return NONE;
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown RefineMethod method " + method);
      }
    }
  }


  protected Map<String,AggValueSource> facetStats;  // per-bucket statistics
  protected Map<String,FacetRequest> subFacets;     // per-bucket sub-facets
  protected boolean processEmpty;
  protected Domain domain;

  // domain changes
  public static class Domain {
    /** 
     * An explicit query domain, <em>ignoring all parent context</em>, expressed in JSON query format.
     * Mutually exclusive to {@link #excludeTags}
     */
    public List<Object> explicitQueries; // list of symbolic filters (JSON query format)
    /**
     * Specifies query/filter tags that should be excluded to re-compute the domain from the parent context.
     * Mutually exclusive to {@link #explicitQueries}
     */
    public List<String> excludeTags;
    public JoinField joinField;
    public GraphField graphField;
    public boolean toParent;
    public boolean toChildren;
    public String parents; // identifies the parent filter... the full set of parent documents for any block join operation
    public List<Object> filters; // list of symbolic filters (JSON query format)

    // True if a starting set of documents can be mapped onto a different set of documents not originally in the starting set.
    public boolean canTransformDomain() {
      return toParent || toChildren
        || (explicitQueries != null) || (excludeTags != null) || (joinField != null);
    }

    // Can this domain become non-empty if the input domain is empty?  This does not check any sub-facets (see canProduceFromEmpty for that)
    public boolean canBecomeNonEmpty() {
      return (explicitQueries != null) || (excludeTags != null);
    }

    /** Are we doing a query time join across other documents */
    public static class JoinField {
      public final String from;
      public final String to;

      private JoinField(String from, String to) {
        assert null != from;
        assert null != to;

        this.from = from;
        this.to = to;
      }

      /**
       * Given a <code>Domain</code>, and a (JSON) map specifying the configuration for that Domain,
       * validates if a '<code>join</code>' is specified, and if so creates a <code>JoinField</code>
       * and sets it on the <code>Domain</code>.
       *
       * (params must not be null)
       */
      public static void createJoinField(FacetRequest.Domain domain, Map<String,Object> domainMap) {
        assert null != domain;
        assert null != domainMap;

        final Object queryJoin = domainMap.get("join");
        if (null != queryJoin) {
          // TODO: maybe allow simple string (instead of map) to mean "self join on this field name" ?
          if (! (queryJoin instanceof Map)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'join' domain change requires a map containing the 'from' and 'to' fields");
          }
          final Map<String,String> join = (Map<String,String>) queryJoin;
          if (! (join.containsKey("from") && join.containsKey("to") &&
              null != join.get("from") && null != join.get("to")) ) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'join' domain change requires non-null 'from' and 'to' field names");
          }
          if (2 != join.size()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'join' domain change contains unexpected keys, only 'from' and 'to' supported: "
                    + join.toString());
          }
          domain.joinField = new JoinField(join.get("from"), join.get("to"));
        }
      }

      /**
       * Creates a Query that can be used to recompute the new "base" for this domain, relative to the
       * current base of the FacetContext.
       */
      public Query createDomainQuery(FacetContext fcontext) throws IOException {
        // NOTE: this code lives here, instead of in FacetProcessor.handleJoin, in order to minimize
        // the number of classes that have to know about the number of possible settings on the join
        // (ie: if we add a score mode, or some other modifier to how the joins are done)

        final SolrConstantScoreQuery fromQuery = new SolrConstantScoreQuery(fcontext.base.getTopFilter());
        // this shouldn't matter once we're wrapped in a join query, but just in case it ever does...
        fromQuery.setCache(false);

        return JoinQParserPlugin.createJoinQuery(fromQuery, this.from, this.to);
      }


    }

    /** Are we doing a query time graph across other documents */
    public static class GraphField {
      public final SolrParams localParams;

      private GraphField(SolrParams localParams) {
        assert null != localParams;

        this.localParams = localParams;
      }

      /**
       * Given a <code>Domain</code>, and a (JSON) map specifying the configuration for that Domain,
       * validates if a '<code>graph</code>' is specified, and if so creates a <code>GraphField</code>
       * and sets it on the <code>Domain</code>.
       *
       * (params must not be null)
       */
      public static void createGraphField(FacetRequest.Domain domain, Map<String,Object> domainMap) {
        assert null != domain;
        assert null != domainMap;
        
        final Object queryGraph = domainMap.get("graph");
        if (null != queryGraph) {
          if (! (queryGraph instanceof Map)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                    "'graph' domain change requires a map containing the 'from' and 'to' fields");
          }
          final Map<String,String> graph = (Map<String,String>) queryGraph;
          if (! (graph.containsKey("from") && graph.containsKey("to") &&
                 null != graph.get("from") && null != graph.get("to")) ) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                    "'graph' domain change requires non-null 'from' and 'to' field names");
          }

          NamedList<String> graphParams = new NamedList<>();
          graphParams.addAll(graph);
          SolrParams localParams = SolrParams.toSolrParams(graphParams);
          domain.graphField = new GraphField(localParams);
        }
      }

      /**
       * Creates a Query that can be used to recompute the new "base" for this domain, relative to the
       * current base of the FacetContext.
       */
      public Query createDomainQuery(FacetContext fcontext) throws IOException {
        final SolrConstantScoreQuery fromQuery = new SolrConstantScoreQuery(fcontext.base.getTopFilter());
        // this shouldn't matter once we're wrapped in a join query, but just in case it ever does...
        fromQuery.setCache(false);

        GraphQueryParser graphParser = new GraphQueryParser(null, localParams, null, fcontext.req);
        try {
          GraphQuery graphQuery = (GraphQuery)graphParser.parse();
          graphQuery.setQ(fromQuery);
          return graphQuery;
        } catch (SyntaxError syntaxError) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
        }
      }


    }
    
  }

  public FacetRequest() {
    facetStats = new LinkedHashMap<>();
    subFacets = new LinkedHashMap<>();
  }

  public Map<String, AggValueSource> getFacetStats() {
    return facetStats;
  }

  public Map<String, FacetRequest> getSubFacets() {
    return subFacets;
  }

  /** Returns null if unset */
  public RefineMethod getRefineMethod() {
    return null;
  }

  public boolean doRefine() {
    return !(getRefineMethod()==null || getRefineMethod()==NONE);
  }

  /** Returns true if this facet can return just some of the facet buckets that match all the criteria.
   * This is normally true only for facets with a limit.
   */
  public boolean returnsPartial() {
    // TODO: should the default impl check processEmpty ?
    return false;
  }

  /** Returns true if this facet, or any sub-facets can produce results from an empty domain. */
  public boolean canProduceFromEmpty() {
    if (domain != null && domain.canBecomeNonEmpty()) return true;
    for (FacetRequest freq : subFacets.values()) {
      if (freq.canProduceFromEmpty()) return true;
    }
    return false;
  }

  public void addStat(String key, AggValueSource stat) {
    facetStats.put(key, stat);
  }

  public void addSubFacet(String key, FacetRequest facetRequest) {
    subFacets.put(key, facetRequest);
  }

  @Override
  public String toString() {
    Map<String, Object> descr = getFacetDescription();
    String s = "facet request: { ";
    for (String key : descr.keySet()) {
      s += key + ":" + descr.get(key) + ",";
    }
    s += "}";
    return s;
  }
  
  public abstract FacetProcessor createFacetProcessor(FacetContext fcontext);

  public abstract FacetMerger createFacetMerger(Object prototype);
  
  public abstract Map<String, Object> getFacetDescription();
}


class FacetContext {
  // Context info for actually executing a local facet command
  public static final int IS_SHARD=0x01;
  public static final int IS_REFINEMENT=0x02;
  public static final int SKIP_FACET=0x04;  // refinement: skip calculating this immediate facet, but proceed to specific sub-facets based on facetInfo

  FacetProcessor processor;
  Map<String,Object> facetInfo; // refinement info for this node
  QueryContext qcontext;
  SolrQueryRequest req;  // TODO: replace with params?
  SolrIndexSearcher searcher;
  Query filter;  // TODO: keep track of as a DocSet or as a Query?
  DocSet base;
  FacetContext parent;
  int flags;
  FacetDebugInfo debugInfo;
  
  public void setDebugInfo(FacetDebugInfo debugInfo) {
    this.debugInfo = debugInfo;
  }
  
  public FacetDebugInfo getDebugInfo() {
    return debugInfo;
  }
  
  public boolean isShard() {
    return (flags & IS_SHARD) != 0;
  }

  /**
   * @param filter The filter for the bucket that resulted in this context/domain.  Can be null if this is the root context.
   * @param domain The resulting set of documents for this facet.
   */
  public FacetContext sub(Query filter, DocSet domain) {
    FacetContext ctx = new FacetContext();
    ctx.parent = this;
    ctx.base = domain;
    ctx.filter = filter;

    // carry over from parent
    ctx.flags = flags;
    ctx.qcontext = qcontext;
    ctx.req = req;
    ctx.searcher = searcher;

    return ctx;
  }
}


abstract class FacetParser<FacetRequestT extends FacetRequest> {
  protected FacetRequestT facet;
  protected FacetParser parent;
  protected String key;

  public FacetParser(FacetParser parent,String key) {
    this.parent = parent;
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  public String getPathStr() {
    if (parent == null) {
      return "/" + key;
    }
    return parent.getKey() + "/" + key;
  }

  protected RuntimeException err(String msg) {
    return new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg + " , path="+getPathStr());
  }

  public abstract FacetRequest parse(Object o) throws SyntaxError;

  // TODO: put the FacetRequest on the parser object?
  public void parseSubs(Object o) throws SyntaxError {
    if (o==null) return;
    if (o instanceof Map) {
      Map<String,Object> m = (Map<String, Object>) o;
      for (Map.Entry<String,Object> entry : m.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        if ("processEmpty".equals(key)) {
          facet.processEmpty = getBoolean(m, "processEmpty", false);
          continue;
        }

        // "my_prices" : { "range" : { "field":...
        // key="my_prices", value={"range":..

        Object parsedValue = parseFacetOrStat(key, value);

        // TODO: have parseFacetOrStat directly add instead of return?
        if (parsedValue instanceof FacetRequest) {
          facet.addSubFacet(key, (FacetRequest)parsedValue);
        } else if (parsedValue instanceof AggValueSource) {
          facet.addStat(key, (AggValueSource)parsedValue);
        } else {
          throw err("Unknown facet type key=" + key + " class=" + (parsedValue == null ? "null" : parsedValue.getClass().getName()));
        }
      }
    } else {
      // facet : my_field?
      throw err("Expected map for facet/stat");
    }
  }

  public Object parseFacetOrStat(String key, Object o) throws SyntaxError {

    if (o instanceof String) {
      return parseStringFacetOrStat(key, (String)o);
    }

    if (!(o instanceof Map)) {
      throw err("expected Map but got " + o);
    }

    // The type can be in a one element map, or inside the args as the "type" field
    // { "query" : "foo:bar" }
    // { "range" : { "field":... } }
    // { "type"  : range, field : myfield, ... }
    Map<String,Object> m = (Map<String,Object>)o;
    String type;
    Object args;

    if (m.size() == 1) {
      Map.Entry<String,Object> entry = m.entrySet().iterator().next();
      type = entry.getKey();
      args = entry.getValue();
      // throw err("expected facet/stat type name, like {range:{... but got " + m);
    } else {
      // type should be inside the map as a parameter
      Object typeObj = m.get("type");
      if (!(typeObj instanceof String)) {
          throw err("expected facet/stat type name, like {type:range, field:price, ...} but got " + typeObj);
      }
      type = (String)typeObj;
      args = m;
    }

    return parseFacetOrStat(key, type, args);
  }

  public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
    // TODO: a place to register all these facet types?

    if ("field".equals(type) || "terms".equals(type)) {
      return parseFieldFacet(key, args);
    } else if ("query".equals(type)) {
      return parseQueryFacet(key, args);
    } else if ("range".equals(type)) {
      return parseRangeFacet(key, args);
    }

    AggValueSource stat = parseStat(key, type, args);
    if (stat == null) {
      throw err("Unknown facet or stat. key=" + key + " type=" + type + " args=" + args);
    }
    return stat;
  }



  FacetField parseFieldFacet(String key, Object args) throws SyntaxError {
    FacetFieldParser parser = new FacetFieldParser(this, key);
    return parser.parse(args);
  }

  FacetQuery parseQueryFacet(String key, Object args) throws SyntaxError {
    FacetQueryParser parser = new FacetQueryParser(this, key);
    return parser.parse(args);
  }

  FacetRange parseRangeFacet(String key, Object args) throws SyntaxError {
    FacetRangeParser parser = new FacetRangeParser(this, key);
    return parser.parse(args);
  }

  public Object parseStringFacetOrStat(String key, String s) throws SyntaxError {
    // "avg(myfield)"
    return parseStringStat(key, s);
    // TODO - simple string representation of facets
  }

  // parses avg(x)
  private AggValueSource parseStringStat(String key, String stat) throws SyntaxError {
    FunctionQParser parser = (FunctionQParser)QParser.getParser(stat, FunctionQParserPlugin.NAME, getSolrRequest());
    AggValueSource agg = parser.parseAgg(FunctionQParser.FLAG_DEFAULT);
    return agg;
  }

  public AggValueSource parseStat(String key, String type, Object args) throws SyntaxError {
    return null;
  }


  private FacetRequest.Domain getDomain() {
    if (facet.domain == null) {
      facet.domain = new FacetRequest.Domain();
    }
    return facet.domain;
  }

  protected void parseCommonParams(Object o) {
    if (o instanceof Map) {
      Map<String,Object> m = (Map<String,Object>)o;
      List<String> excludeTags = getStringList(m, "excludeTags");
      if (excludeTags != null) {
        getDomain().excludeTags = excludeTags;
      }

      Map<String,Object> domainMap = (Map<String,Object>) m.get("domain");
      if (domainMap != null) {
        FacetRequest.Domain domain = getDomain();

        excludeTags = getStringList(domainMap, "excludeTags");
        if (excludeTags != null) {
          domain.excludeTags = excludeTags;
        }

        if (domainMap.containsKey("query")) {
          domain.explicitQueries = parseJSONQueryStruct(domainMap.get("query"));
          if (null == domain.explicitQueries) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                    "'query' domain can not be null or empty");
          } else if (null != domain.excludeTags) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                    "'query' domain can not be combined with 'excludeTags'");
          }
        }
        
        String blockParent = (String)domainMap.get("blockParent");
        String blockChildren = (String)domainMap.get("blockChildren");

        if (blockParent != null) {
          domain.toParent = true;
          domain.parents = blockParent;
        } else if (blockChildren != null) {
          domain.toChildren = true;
          domain.parents = blockChildren;
        }
          
        FacetRequest.Domain.JoinField.createJoinField(domain, domainMap);
        FacetRequest.Domain.GraphField.createGraphField(domain, domainMap);

        Object filterOrList = domainMap.get("filter");
        if (filterOrList != null) {
          assert domain.filters == null;
          domain.filters = parseJSONQueryStruct(filterOrList);
        }

      } // end "domain"
    }
  }

  /** returns null on null input, otherwise returns a list of the JSON query structures -- either
   * directly from the raw (list) input, or if raw input is a not a list then it encapsulates 
   * it in a new list.
   */
  private List<Object> parseJSONQueryStruct(Object raw) {
    List<Object> result = null;
    if (null == raw) {
      return result;
    } else if (raw instanceof List) {
      result = (List<Object>) raw;
    } else {
      result = new ArrayList<>(1);
      result.add(raw);
    }
    return result;
  }

  public String getField(Map<String,Object> args) {
    Object fieldName = args.get("field"); // TODO: pull out into defined constant
    if (fieldName == null) {
      fieldName = args.get("f");  // short form
    }
    if (fieldName == null) {
      throw err("Missing 'field'");
    }

    if (!(fieldName instanceof String)) {
      throw err("Expected string for 'field', got" + fieldName);
    }

    return (String)fieldName;
  }


  public Long getLongOrNull(Map<String,Object> args, String paramName, boolean required) {
    Object o = args.get(paramName);
    if (o == null) {
      if (required) {
        throw err("Missing required parameter '" + paramName + "'");
      }
      return null;
    }
    if (!(o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte)) {
      throw err("Expected integer type for param '"+paramName + "' but got " + o);
    }

    return ((Number)o).longValue();
  }

  public long getLong(Map<String,Object> args, String paramName, long defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    if (!(o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte)) {
      throw err("Expected integer type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return ((Number)o).longValue();
  }

  public boolean getBoolean(Map<String,Object> args, String paramName, boolean defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    // TODO: should we be more flexible and accept things like "true" (strings)?
    // Perhaps wait until the use case comes up.
    if (!(o instanceof Boolean)) {
      throw err("Expected boolean type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (Boolean)o;
  }

  public String getString(Map<String,Object> args, String paramName, String defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    if (!(o instanceof String)) {
      throw err("Expected string type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (String)o;
  }

  public List<String> getStringList(Map<String,Object> args, String paramName) {
    Object o = args.get(paramName);
    if (o == null) {
      return null;
    }
    if (o instanceof List) {
      return (List<String>)o;
    }
    if (o instanceof String) {
      return StrUtils.splitSmart((String)o, ",", true);
    }

    throw err("Expected list of string or comma separated string values.");
  }

  public IndexSchema getSchema() {
    return parent.getSchema();
  }

  public SolrQueryRequest getSolrRequest() {
    return parent.getSolrRequest();
  }

}


class FacetTopParser extends FacetParser<FacetQuery> {
  private SolrQueryRequest req;

  public FacetTopParser(SolrQueryRequest req) {
    super(null, "facet");
    this.facet = new FacetQuery();
    this.req = req;
  }

  @Override
  public FacetQuery parse(Object args) throws SyntaxError {
    parseSubs(args);
    return facet;
  }

  @Override
  public SolrQueryRequest getSolrRequest() {
    return req;
  }

  @Override
  public IndexSchema getSchema() {
    return req.getSchema();
  }
}

class FacetQueryParser extends FacetParser<FacetQuery> {
  public FacetQueryParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetQuery();
  }

  @Override
  public FacetQuery parse(Object arg) throws SyntaxError {
    parseCommonParams(arg);

    String qstring = null;
    if (arg instanceof String) {
      // just the field name...
      qstring = (String)arg;

    } else if (arg instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) arg;
      qstring = getString(m, "q", null);
      if (qstring == null) {
        qstring = getString(m, "query", null);
      }

      // OK to parse subs before we have parsed our own query?
      // as long as subs don't need to know about it.
      parseSubs( m.get("facet") );
    }

    // TODO: substats that are from defaults!!!

    if (qstring != null) {
      QParser parser = QParser.getParser(qstring, getSolrRequest());
      parser.setIsFilter(true);
      facet.q = parser.getQuery();
    }

    return facet;
  }
}

/*** not a separate type of parser for now...
class FacetBlockParentParser extends FacetParser<FacetBlockParent> {
  public FacetBlockParentParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetBlockParent();
  }

  @Override
  public FacetBlockParent parse(Object arg) throws SyntaxError {
    parseCommonParams(arg);

    if (arg instanceof String) {
      // just the field name...
      facet.parents = (String)arg;

    } else if (arg instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) arg;
      facet.parents = getString(m, "parents", null);

      parseSubs( m.get("facet") );
    }

    return facet;
  }
}
***/


class FacetFieldParser extends FacetParser<FacetField> {
  public FacetFieldParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetField();
  }

  public FacetField parse(Object arg) throws SyntaxError {
    parseCommonParams(arg);
    if (arg instanceof String) {
      // just the field name...
      facet.field = (String)arg;
      parseSort( null );  // TODO: defaults

    } else if (arg instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) arg;
      facet.field = getField(m);
      facet.offset = getLong(m, "offset", facet.offset);
      facet.limit = getLong(m, "limit", facet.limit);
      facet.overrequest = (int) getLong(m, "overrequest", facet.overrequest);
      if (facet.limit == 0) facet.offset = 0;  // normalize.  an offset with a limit of non-zero isn't useful.
      facet.mincount = getLong(m, "mincount", facet.mincount);
      facet.missing = getBoolean(m, "missing", facet.missing);
      facet.numBuckets = getBoolean(m, "numBuckets", facet.numBuckets);
      facet.prefix = getString(m, "prefix", facet.prefix);
      facet.allBuckets = getBoolean(m, "allBuckets", facet.allBuckets);
      facet.method = FacetField.FacetMethod.fromString(getString(m, "method", null));
      facet.cacheDf = (int)getLong(m, "cacheDf", facet.cacheDf);

      // TODO: pull up to higher level?
      facet.refine = FacetField.RefineMethod.fromObj(m.get("refine"));

      facet.perSeg = (Boolean)m.get("perSeg");

      // facet.sort may depend on a facet stat...
      // should we be parsing / validating this here, or in the execution environment?
      Object o = m.get("facet");
      parseSubs(o);

      parseSort( m.get(SORT) );
    }

    return facet;
  }


  // Sort specification is currently
  // sort : 'mystat desc'
  // OR
  // sort : { mystat : 'desc' }
  private void parseSort(Object sort) {
    if (sort == null) {
      facet.sortVariable = "count";
      facet.sortDirection = FacetRequest.SortDirection.desc;
    } else if (sort instanceof String) {
      String sortStr = (String)sort;
      if (sortStr.endsWith(" asc")) {
        facet.sortVariable = sortStr.substring(0, sortStr.length()-" asc".length());
        facet.sortDirection = FacetRequest.SortDirection.asc;
      } else if (sortStr.endsWith(" desc")) {
        facet.sortVariable = sortStr.substring(0, sortStr.length()-" desc".length());
        facet.sortDirection = FacetRequest.SortDirection.desc;
      } else {
        facet.sortVariable = sortStr;
        facet.sortDirection = "index".equals(facet.sortVariable) ? FacetRequest.SortDirection.asc : FacetRequest.SortDirection.desc;  // default direction for "index" is ascending
      }
    } else {
     // sort : { myvar : 'desc' }
      Map<String,Object> map = (Map<String,Object>)sort;
      // TODO: validate
      Map.Entry<String,Object> entry = map.entrySet().iterator().next();
      String k = entry.getKey();
      Object v = entry.getValue();
      facet.sortVariable = k;
      facet.sortDirection = FacetRequest.SortDirection.valueOf(v.toString());
    }

  }
}



class FacetRangeParser extends FacetParser<FacetRange> {
  public FacetRangeParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetRange();
  }

  public FacetRange parse(Object arg) throws SyntaxError {
    parseCommonParams(arg);

    if (!(arg instanceof Map)) {
      throw err("Missing range facet arguments");
    }

    Map<String, Object> m = (Map<String, Object>) arg;

    facet.field = getString(m, "field", null);

    facet.start = m.get("start");
    facet.end = m.get("end");
    facet.gap = m.get("gap");
    facet.hardend = getBoolean(m, "hardend", facet.hardend);
    facet.mincount = getLong(m, "mincount", 0);

    // TODO: refactor list-of-options code

    Object o = m.get("include");
    String[] includeList = null;
    if (o != null) {
      List lst = null;

      if (o instanceof List) {
        lst = (List)o;
      } else if (o instanceof String) {
        lst = StrUtils.splitSmart((String)o, ',');
      }

      includeList = (String[])lst.toArray(new String[lst.size()]);
    }
    facet.include = FacetParams.FacetRangeInclude.parseParam( includeList );

    facet.others = EnumSet.noneOf(FacetParams.FacetRangeOther.class);

    o = m.get("other");
    if (o != null) {
      List<String> lst = null;

      if (o instanceof List) {
        lst = (List)o;
      } else if (o instanceof String) {
        lst = StrUtils.splitSmart((String)o, ',');
      }

      for (String otherStr : lst) {
        facet.others.add( FacetParams.FacetRangeOther.get(otherStr) );
      }
    }


    Object facetObj = m.get("facet");
    parseSubs(facetObj);

    return facet;
  }

}



