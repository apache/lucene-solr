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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.JoinQParserPlugin;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.join.GraphQuery;
import org.apache.solr.search.join.GraphQueryParser;
import org.apache.solr.util.RTimer;

import static org.apache.solr.search.facet.FacetRequest.RefineMethod.NONE;

/**
 * A request to do facets/stats that might itself be composed of sub-FacetRequests.
 * This is a cornerstone of the facet module.
 *
 * @see #parse(SolrQueryRequest, Map)
 */
public abstract class FacetRequest {

  /**
   * Simple structure for encapsulating a sort variable and a direction
   */
  public static final class FacetSort {
    final String sortVariable;
    final SortDirection sortDirection;

    public FacetSort(final String sortVariable, final SortDirection sortDirection) {
      assert null != sortVariable;
      assert null != sortDirection;

      this.sortVariable = sortVariable;
      this.sortDirection = sortDirection;
    }

    public boolean equals(Object other) {
      if (other instanceof FacetSort) {
        final FacetSort that = (FacetSort) other;
        return this.sortVariable.equals(that.sortVariable)
            && this.sortDirection.equals(that.sortDirection);
      }
      return false;
    }

    public int hashCode() {
      return Objects.hash(sortVariable, sortDirection);
    }

    public String toString() {
      return sortVariable + " " + sortDirection;
    }

    /**
     * Commonly Re-used "count desc" (default)
     */
    public static final FacetSort COUNT_DESC = new FacetSort("count", SortDirection.desc);
    /**
     * Commonly Re-used "index asc" (index order / streaming)
     */
    public static final FacetSort INDEX_ASC = new FacetSort("index", SortDirection.asc);
  }

  public static enum SortDirection {
    asc(-1),
    desc(1);

    private final int multiplier;

    private SortDirection(int multiplier) {
      this.multiplier = multiplier;
    }

    public static SortDirection fromObj(Object direction) {
      if (direction == null) {
        // should we just default either to desc/asc??
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing Sort direction");
      }

      switch (direction.toString()) {
        case "asc":
          return asc;
        case "desc":
          return desc;
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown Sort direction '" + direction + "'");
      }
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
      if (method instanceof Boolean) {
        return ((Boolean) method) ? SIMPLE : NONE;
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


  protected Map<String, AggValueSource> facetStats;  // per-bucket statistics
  protected Map<String, FacetRequest> subFacets;     // per-bucket sub-facets
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

    /**
     * Are we doing a query time join across other documents
     */
    public static class JoinField {
      private static final String FROM_PARAM = "from";
      private static final String TO_PARAM = "to";
      private static final String METHOD_PARAM = "method";
      private static final Set<String> SUPPORTED_JOIN_PROPERTIES = Sets.newHashSet(FROM_PARAM, TO_PARAM, METHOD_PARAM);

      public final String from;
      public final String to;
      public final String method;

      private JoinField(String from, String to, String method) {
        assert null != from;
        assert null != to;
        assert null != method;

        this.from = from;
        this.to = to;
        this.method = method;
      }

      /**
       * Given a <code>Domain</code>, and a (JSON) map specifying the configuration for that Domain,
       * validates if a '<code>join</code>' is specified, and if so creates a <code>JoinField</code>
       * and sets it on the <code>Domain</code>.
       * <p>
       * (params must not be null)
       */
      public static void createJoinField(FacetRequest.Domain domain, Map<String, Object> domainMap) {
        assert null != domain;
        assert null != domainMap;

        final Object queryJoin = domainMap.get("join");
        if (null != queryJoin) {
          // TODO: maybe allow simple string (instead of map) to mean "self join on this field name" ?
          if (!(queryJoin instanceof Map)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'join' domain change requires a map containing the 'from' and 'to' fields");
          }

          @SuppressWarnings({"unchecked"})
          final Map<String,String> join = (Map<String,String>) queryJoin;
          if (! (join.containsKey(FROM_PARAM) && join.containsKey(TO_PARAM) &&
              null != join.get(FROM_PARAM) && null != join.get(TO_PARAM)) ) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'join' domain change requires non-null 'from' and 'to' field names");
          }

          for (String providedKey : join.keySet()) {
            if (! SUPPORTED_JOIN_PROPERTIES.contains(providedKey)) {
              final String supportedPropsStr = String.join(", ", SUPPORTED_JOIN_PROPERTIES);
              final String message = String.format(Locale.ROOT,
                  "'join' domain change contains unexpected key [%s], only %s supported", providedKey, supportedPropsStr);
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message);
            }
          }

          final String method = join.containsKey(METHOD_PARAM) ? join.get(METHOD_PARAM) : "index";
          domain.joinField = new JoinField(join.get(FROM_PARAM), join.get(TO_PARAM), method);
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

        return JoinQParserPlugin.createJoinQuery(fromQuery, this.from, this.to, this.method);
      }


    }

    /**
     * Are we doing a query time graph across other documents
     */
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
       * <p>
       * (params must not be null)
       */
      public static void createGraphField(FacetRequest.Domain domain, Map<String, Object> domainMap) {
        assert null != domain;
        assert null != domainMap;

        final Object queryGraph = domainMap.get("graph");
        if (null != queryGraph) {
          if (!(queryGraph instanceof Map)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'graph' domain change requires a map containing the 'from' and 'to' fields");
          }
          @SuppressWarnings({"unchecked"}) final Map<String, String> graph = (Map<String, String>) queryGraph;
          if (!(graph.containsKey("from") && graph.containsKey("to") &&
              null != graph.get("from") && null != graph.get("to"))) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'graph' domain change requires non-null 'from' and 'to' field names");
          }

          domain.graphField = new GraphField(FacetParser.jsonToSolrParams(graph));
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
          GraphQuery graphQuery = (GraphQuery) graphParser.parse();
          graphQuery.setQ(fromQuery);
          return graphQuery;
        } catch (SyntaxError syntaxError) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
        }
      }


    }

  }

  /**
   * Factory method to parse a facet request tree.  The outer keys are arbitrary labels and their values are
   * facet request specifications. Will throw a {@link SolrException} if it fails to parse.
   *
   * @param req    the overall request
   * @param params a typed parameter structure (unlike SolrParams which are all string values).
   */
  public static FacetRequest parse(SolrQueryRequest req, Map<String, Object> params) {
    @SuppressWarnings({"rawtypes"})
    FacetParser parser = new FacetParser.FacetTopParser(req);
    try {
      return parser.parse(params);
    } catch (SyntaxError syntaxError) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
    }
  }

  //TODO it would be nice if there was no distinction.  If the top level request had "type" as special then there wouldn't be a need.

  /**
   * Factory method to parse out a rooted facet request tree that would normally go one level below a label.
   * The params must contain a "type".
   * This is intended to be useful externally, such as by {@link org.apache.solr.request.SimpleFacets}.
   *
   * @param req    the overall request
   * @param params a typed parameter structure (unlike SolrParams which are all string values).
   */
  public static FacetRequest parseOneFacetReq(SolrQueryRequest req, Map<String, Object> params) {
    @SuppressWarnings("rawtypes")
    FacetParser parser = new FacetParser.FacetTopParser(req);
    try {
      return (FacetRequest) parser.parseFacetOrStat("", params);
    } catch (SyntaxError syntaxError) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
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

  /**
   * Returns null if unset
   */
  public RefineMethod getRefineMethod() {
    return null;
  }

  public boolean doRefine() {
    return !(getRefineMethod() == null || getRefineMethod() == NONE);
  }

  /**
   * Returns true if this facet can return just some of the facet buckets that match all the criteria.
   * This is normally true only for facets with a limit.
   */
  public boolean returnsPartial() {
    // TODO: should the default impl check processEmpty ?
    return false;
  }

  /**
   * Returns true if this facet, or any sub-facets can produce results from an empty domain.
   */
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
    StringBuilder s = new StringBuilder("facet request: { ");
    for (Map.Entry<String, Object> entry : descr.entrySet()) {
      s.append(entry.getKey()).append(':').append(entry.getValue()).append(',');
    }
    s.append('}');
    return s.toString();
  }

  /**
   * Process this facet request against the given domain of docs.
   * Note: this is currently used externally by {@link org.apache.solr.request.SimpleFacets}.
   */
  public final Object process(SolrQueryRequest req, DocSet domain) throws IOException {
    //TODO check for FacetDebugInfo?  and if so set on fcontext
    //  rb.req.getContext().get("FacetDebugInfo");
    //TODO should the SolrQueryRequest be held on the FacetRequest?  It was created from parse(req,...) so is known.
    FacetContext fcontext = new FacetContext();
    fcontext.base = domain;
    fcontext.req = req;
    fcontext.searcher = req.getSearcher();
    fcontext.qcontext = QueryContext.newContext(fcontext.searcher);

    return process(fcontext);
  }

  /**
   * Process the request with the facet context settings, a parameter-object.
   */
  final Object process(FacetContext fcontext) throws IOException {
    @SuppressWarnings("rawtypes")
    FacetProcessor facetProcessor = createFacetProcessor(fcontext);

    FacetDebugInfo debugInfo = fcontext.getDebugInfo();
    if (debugInfo == null) {
      facetProcessor.process();
    } else {
      if (fcontext.filter != null) {
        debugInfo.setFilter(fcontext.filter.toString());
      }
      debugInfo.setReqDescription(getFacetDescription());
      debugInfo.setProcessor(facetProcessor.getClass().getSimpleName());
      debugInfo.putInfoItem("domainSize", (long) fcontext.base.size());
      RTimer timer = new RTimer();
      try {
        facetProcessor.process();
      } finally {
        debugInfo.setElapse((long) timer.getTime());
      }
    }

    return facetProcessor.getResponse();
  }

  @SuppressWarnings("rawtypes")
  public abstract FacetProcessor createFacetProcessor(FacetContext fcontext);

  public abstract FacetMerger createFacetMerger(Object prototype);

  public abstract Map<String, Object> getFacetDescription();
}





