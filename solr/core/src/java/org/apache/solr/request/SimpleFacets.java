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
package org.apache.solr.request;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiPostingsEnum;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.grouping.AllGroupHeadsCollector;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.TermGroupFacetCollector;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SpatialHeatmapFacets;
import org.apache.solr.request.IntervalFacets.FacetInterval;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Grouping;
import org.apache.solr.search.HashDocSet;
import org.apache.solr.search.Insanity;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.facet.FacetDebugInfo;
import org.apache.solr.search.facet.FacetRequest;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.util.BoundedTreeSet;
import org.apache.solr.util.RTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * A class that generates simple Facet information for a request.
 *
 * More advanced facet implementations may compose or subclass this class 
 * to leverage any of its functionality.
 */
public class SimpleFacets {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  /** The main set of documents all facet counts should be relative to */
  protected DocSet docsOrig;
  /** Configuration params behavior should be driven by */
  protected final SolrParams global;
  /** Searcher to use for all calculations */
  protected final SolrIndexSearcher searcher;
  protected final SolrQueryRequest req;
  protected final ResponseBuilder rb;

  protected FacetDebugInfo fdebugParent;
  protected FacetDebugInfo fdebug;

  // per-facet values
  protected final static class ParsedParams {
    final public SolrParams localParams; // localParams on this particular facet command
    final public SolrParams params;      // local+original
    final public SolrParams required;    // required version of params
    final public String facetValue;      // the field to or query to facet on (minus local params)
    final public DocSet docs;            // the base docset for this particular facet
    final public String key;             // what name should the results be stored under
    final public List<String> tags;      // the tags applied to this facet value
    final public int threads;
    
    public ParsedParams(final SolrParams localParams, // localParams on this particular facet command
                        final SolrParams params,      // local+original
                        final SolrParams required,    // required version of params
                        final String facetValue,      // the field to or query to facet on (minus local params)
                        final DocSet docs,            // the base docset for this particular facet
                        final String key,             // what name should the results be stored under
                        final List<String> tags,
                        final int threads) {
      this.localParams = localParams;
      this.params = params;
      this.required = required;
      this.facetValue = facetValue;
      this.docs = docs;
      this.key = key;
      this.tags = tags;
      this.threads = threads;
    }
    
    public ParsedParams withDocs(final DocSet docs) {
      return new ParsedParams(localParams, params, required, facetValue, docs, key, tags, threads);
    }
  }

  public SimpleFacets(SolrQueryRequest req,
                      DocSet docs,
                      SolrParams params) {
    this(req,docs,params,null);
  }

  public SimpleFacets(SolrQueryRequest req,
                      DocSet docs,
                      SolrParams params,
                      ResponseBuilder rb) {
    this.req = req;
    this.searcher = req.getSearcher();
    this.docsOrig = docs;
    this.global = params;
    this.rb = rb;
    this.facetExecutor = req.getCore().getCoreContainer().getUpdateShardHandler().getUpdateExecutor();
  }

  public void setFacetDebugInfo(FacetDebugInfo fdebugParent) {
    this.fdebugParent = fdebugParent;
  }

  protected ParsedParams parseParams(String type, String param) throws SyntaxError, IOException {
    SolrParams localParams = QueryParsing.getLocalParams(param, req.getParams());
    DocSet docs = docsOrig;
    String facetValue = param;
    String key = param;
    List<String> tags = Collections.emptyList();
    int threads = -1;

    if (localParams == null) {
      SolrParams params = global;
      SolrParams required = new RequiredSolrParams(params);
      return new ParsedParams(localParams, params, required, facetValue, docs, key, tags, threads);
    }
    
    SolrParams params = SolrParams.wrapDefaults(localParams, global);
    SolrParams required = new RequiredSolrParams(params);

    // remove local params unless it's a query
    if (type != FacetParams.FACET_QUERY) { // TODO Cut over to an Enum here
      facetValue = localParams.get(CommonParams.VALUE);
    }

    // reset set the default key now that localParams have been removed
    key = facetValue;

    // allow explicit set of the key
    key = localParams.get(CommonParams.OUTPUT_KEY, key);

    String tagStr = localParams.get(CommonParams.TAG);
    tags = tagStr == null ? Collections.<String>emptyList() : StrUtils.splitSmart(tagStr,',');

    String threadStr = localParams.get(CommonParams.THREADS);
    if (threadStr != null) {
      threads = Integer.parseInt(threadStr);
    }

    // figure out if we need a new base DocSet
    String excludeStr = localParams.get(CommonParams.EXCLUDE);
    if (excludeStr == null) return new ParsedParams(localParams, params, required, facetValue, docs, key, tags, threads);

    List<String> excludeTagList = StrUtils.splitSmart(excludeStr,',');
    docs = computeDocSet(docs, excludeTagList);
    return new ParsedParams(localParams, params, required, facetValue, docs, key, tags, threads);
  }

  protected DocSet computeDocSet(DocSet baseDocSet, List<String> excludeTagList) throws SyntaxError, IOException {
    Map<?,?> tagMap = (Map<?,?>)req.getContext().get("tags");
    // rb can be null if facets are being calculated from a RequestHandler e.g. MoreLikeThisHandler
    if (tagMap == null || rb == null) {
      return baseDocSet;
    }

    IdentityHashMap<Query,Boolean> excludeSet = new IdentityHashMap<>();
    for (String excludeTag : excludeTagList) {
      Object olst = tagMap.get(excludeTag);
      // tagMap has entries of List<String,List<QParser>>, but subject to change in the future
      if (!(olst instanceof Collection)) continue;
      for (Object o : (Collection<?>)olst) {
        if (!(o instanceof QParser)) continue;
        QParser qp = (QParser)o;
        excludeSet.put(qp.getQuery(), Boolean.TRUE);
      }
    }
    if (excludeSet.size() == 0) return baseDocSet;

    List<Query> qlist = new ArrayList<>();

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
    DocSet base = searcher.getDocSet(qlist);
    if (rb.grouping() && rb.getGroupingSpec().isTruncateGroups()) {
      Grouping grouping = new Grouping(searcher, null, rb.createQueryCommand(), false, 0, false);
      grouping.setWithinGroupSort(rb.getGroupingSpec().getWithinGroupSortSpec().getSort());
      if (rb.getGroupingSpec().getFields().length > 0) {
        grouping.addFieldCommand(rb.getGroupingSpec().getFields()[0], req);
      } else if (rb.getGroupingSpec().getFunctions().length > 0) {
        grouping.addFunctionCommand(rb.getGroupingSpec().getFunctions()[0], req);
      } else {
        return base;
      }
      @SuppressWarnings({"rawtypes"})
      AllGroupHeadsCollector allGroupHeadsCollector = grouping.getCommands().get(0).createAllGroupCollector();
      searcher.search(base.getTopFilter(), allGroupHeadsCollector);
      return new BitDocSet(allGroupHeadsCollector.retrieveGroupHeads(searcher.maxDoc()));
    } else {
      return base;
    }
  }

  /**
   * Returns a list of facet counts for each of the facet queries
   * specified in the params
   *
   * @see FacetParams#FACET_QUERY
   */
  public NamedList<Integer> getFacetQueryCounts() throws IOException,SyntaxError {

    NamedList<Integer> res = new SimpleOrderedMap<>();

    /* Ignore CommonParams.DF - could have init param facet.query assuming
     * the schema default with query param DF intented to only affect Q.
     * If user doesn't want schema default for facet.query, they should be
     * explicit.
     */
    // SolrQueryParser qp = searcher.getSchema().getSolrQueryParser(null);

    String[] facetQs = global.getParams(FacetParams.FACET_QUERY);

    if (null != facetQs && 0 != facetQs.length) {
      for (String q : facetQs) {
        final ParsedParams parsed = parseParams(FacetParams.FACET_QUERY, q);
        getFacetQueryCount(parsed, res);
      }
    }

    return res;
  }

  public void getFacetQueryCount(ParsedParams parsed, NamedList<Integer> res) throws SyntaxError, IOException {
    // TODO: slight optimization would prevent double-parsing of any localParams
    // TODO: SOLR-7753
    Query qobj = QParser.getParser(parsed.facetValue, req).getQuery();

    if (qobj == null) {
      res.add(parsed.key, 0);
    } else if (parsed.params.getBool(GroupParams.GROUP_FACET, false)) {
      res.add(parsed.key, getGroupedFacetQueryCount(qobj, parsed.docs));
    } else {
      res.add(parsed.key, searcher.numDocs(qobj, parsed.docs));
    }
  }

  /**
   * Returns a grouped facet count for the facet query
   *
   * @see FacetParams#FACET_QUERY
   */
  public int getGroupedFacetQueryCount(Query facetQuery, DocSet docSet) throws IOException {
    // It is okay to retrieve group.field from global because it is never a local param
    String groupField = global.get(GroupParams.GROUP_FIELD);
    if (groupField == null) {
      throw new SolrException (
          SolrException.ErrorCode.BAD_REQUEST,
          "Specify the group.field as parameter or local parameter"
      );
    }

    @SuppressWarnings({"rawtypes"})
    AllGroupsCollector collector = new AllGroupsCollector<>(new TermGroupSelector(groupField));
    searcher.search(QueryUtils.combineQueryAndFilter(facetQuery, docSet.getTopFilter()), collector);
    return collector.getGroupCount();
  }

  enum FacetMethod {
    ENUM, FC, FCS, UIF;
  }

  /**
   * Create a new bytes ref filter for excluding facet terms.
   *
   * This method by default uses the {@link FacetParams#FACET_EXCLUDETERMS} parameter
   * but custom SimpleFacets classes could use a different implementation.
   *
   * @param field the field to check for facet term filters
   * @param params the request parameter object
   * @return A predicate for filtering terms or null if no filters are applicable.
   */
  protected Predicate<BytesRef> newExcludeBytesRefFilter(String field, SolrParams params) {
    final String exclude = params.getFieldParam(field, FacetParams.FACET_EXCLUDETERMS);
    if (exclude == null) {
      return null;
    }

    final Set<String> excludeTerms = new HashSet<>(StrUtils.splitSmart(exclude, ",", true));

    return new Predicate<BytesRef>() {
      @Override
      public boolean test(BytesRef bytesRef) {
        return !excludeTerms.contains(bytesRef.utf8ToString());
      }
    };
  }

  /**
   * Create a new bytes ref filter for filtering facet terms. If more than one filter is
   * applicable the applicable filters will be returned as an {@link Predicate#and(Predicate)}
   * of all such filters.
   *
   * @param field the field to check for facet term filters
   * @param params the request parameter object
   * @return A predicate for filtering terms or null if no filters are applicable.
   */
  protected Predicate<BytesRef> newBytesRefFilter(String field, SolrParams params) {
    final String contains = params.getFieldParam(field, FacetParams.FACET_CONTAINS);

    Predicate<BytesRef> finalFilter = null;

    if (contains != null) {
      final boolean containsIgnoreCase = params.getFieldBool(field, FacetParams.FACET_CONTAINS_IGNORE_CASE, false);
      finalFilter = new SubstringBytesRefFilter(contains, containsIgnoreCase);
    }

    final String regex = params.getFieldParam(field, FacetParams.FACET_MATCHES);
    if (regex != null) {
      final RegexBytesRefFilter regexBytesRefFilter = new RegexBytesRefFilter(regex);
      finalFilter = (finalFilter == null) ? regexBytesRefFilter : finalFilter.and(regexBytesRefFilter);
    }

    final Predicate<BytesRef> excludeFilter = newExcludeBytesRefFilter(field, params);
    if (excludeFilter != null) {
      finalFilter = (finalFilter == null) ? excludeFilter : finalFilter.and(excludeFilter);
    }

    return finalFilter;
  }

  /**
   * Term counts for use in pivot faceting that resepcts the appropriate mincount
   * @see FacetParams#FACET_PIVOT_MINCOUNT
   */
  public NamedList<Integer> getTermCountsForPivots(String field, ParsedParams parsed) throws IOException {
    Integer mincount = parsed.params.getFieldInt(field, FacetParams.FACET_PIVOT_MINCOUNT, 1);
    return getTermCounts(field, mincount, parsed);
  }

  /**
   * Term counts for use in field faceting that resepects the appropriate mincount
   *
   * @see FacetParams#FACET_MINCOUNT
   */
  public NamedList<Integer> getTermCounts(String field, ParsedParams parsed) throws IOException {
    Integer mincount = parsed.params.getFieldInt(field, FacetParams.FACET_MINCOUNT);
    return getTermCounts(field, mincount, parsed);
  }

  /**
   * Term counts for use in field faceting that resepcts the specified mincount - 
   * if mincount is null, the "zeros" param is consulted for the appropriate backcompat 
   * default
   *
   * @see FacetParams#FACET_ZEROS
   */
  private NamedList<Integer> getTermCounts(String field, Integer mincount, ParsedParams parsed) throws IOException {
    final SolrParams params = parsed.params;
    final DocSet docs = parsed.docs;
    final int threads = parsed.threads;
    int offset = params.getFieldInt(field, FacetParams.FACET_OFFSET, 0);
    int limit = params.getFieldInt(field, FacetParams.FACET_LIMIT, 100);
    boolean missing = params.getFieldBool(field, FacetParams.FACET_MISSING, false);

    // when limit=0 and missing=false then return empty list
    if (limit == 0 && !missing) return new NamedList<>();

    if (mincount==null) {
      Boolean zeros = params.getFieldBool(field, FacetParams.FACET_ZEROS);
      // mincount = (zeros!=null && zeros) ? 0 : 1;
      mincount = (zeros!=null && !zeros) ? 1 : 0;
      // current default is to include zeros.
    }

    // default to sorting if there is a limit.
    String sort = params.getFieldParam(field, FacetParams.FACET_SORT, limit>0 ? FacetParams.FACET_SORT_COUNT : FacetParams.FACET_SORT_INDEX);
    String prefix = params.getFieldParam(field, FacetParams.FACET_PREFIX);

    final Predicate<BytesRef> termFilter = newBytesRefFilter(field, params);

    boolean exists = params.getFieldBool(field, FacetParams.FACET_EXISTS, false);
    
    NamedList<Integer> counts;
    SchemaField sf = searcher.getSchema().getField(field);
    if (sf.getType().isPointField() && !sf.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
          "Can't facet on a PointField without docValues");
    }
    FieldType ft = sf.getType();

    // determine what type of faceting method to use
    final String methodStr = params.getFieldParam(field, FacetParams.FACET_METHOD);
    final FacetMethod requestedMethod;
    if (FacetParams.FACET_METHOD_enum.equals(methodStr)) {
      requestedMethod = FacetMethod.ENUM;
    } else if (FacetParams.FACET_METHOD_fcs.equals(methodStr)) {
      requestedMethod = FacetMethod.FCS;
    } else if (FacetParams.FACET_METHOD_fc.equals(methodStr)) {
      requestedMethod = FacetMethod.FC;
    } else if(FacetParams.FACET_METHOD_uif.equals(methodStr)) {
      requestedMethod = FacetMethod.UIF;
    } else {
      requestedMethod=null;
    }

    final boolean multiToken = sf.multiValued() || ft.multiValuedFieldCache();

    FacetMethod appliedFacetMethod = selectFacetMethod(field,
                                sf, requestedMethod, mincount,
                                exists);

    RTimer timer = null;
    if (fdebug != null) {
       fdebug.putInfoItem("requestedMethod", requestedMethod==null?"not specified":requestedMethod.name());
       fdebug.putInfoItem("appliedMethod", appliedFacetMethod.name());
       fdebug.putInfoItem("inputDocSetSize", docs.size());
       fdebug.putInfoItem("field", field);
       timer = new RTimer();
    }

    if (params.getFieldBool(field, GroupParams.GROUP_FACET, false)) {
      counts = getGroupedCounts(searcher, docs, field, multiToken, offset,limit, mincount, missing, sort, prefix, termFilter);
    } else {
      assert appliedFacetMethod != null;
      switch (appliedFacetMethod) {
        case ENUM:
          assert TrieField.getMainValuePrefix(ft) == null;
          counts = getFacetTermEnumCounts(searcher, docs, field, offset, limit, mincount,missing,sort,prefix, termFilter, exists);
          break;
        case FCS:
          assert ft.isPointField() || !multiToken;
          if (ft.isPointField() || (ft.getNumberType() != null && !sf.multiValued())) {
            if (prefix != null) {
              throw new SolrException(ErrorCode.BAD_REQUEST, FacetParams.FACET_PREFIX + " is not supported on numeric types");
            }
            if (termFilter != null) {
              throw new SolrException(ErrorCode.BAD_REQUEST, "BytesRef term filters ("
                      + FacetParams.FACET_MATCHES + ", "
                      + FacetParams.FACET_CONTAINS + ", "
                      + FacetParams.FACET_EXCLUDETERMS + ") are not supported on numeric types");
            }
            if (ft.isPointField() && mincount <= 0) { // default is mincount=0.  See SOLR-10033 & SOLR-11174.
              String warningMessage 
                  = "Raising facet.mincount from " + mincount + " to 1, because field " + field + " is Points-based.";
              log.warn(warningMessage);
              @SuppressWarnings({"unchecked"})
              List<String> warnings = (List<String>)rb.rsp.getResponseHeader().get("warnings");
              if (null == warnings) {
                warnings = new ArrayList<>();
                rb.rsp.getResponseHeader().add("warnings", warnings);
              }
              warnings.add(warningMessage);

              mincount = 1;
            }
            counts = NumericFacets.getCounts(searcher, docs, field, offset, limit, mincount, missing, sort);
          } else {
            PerSegmentSingleValuedFaceting ps = new PerSegmentSingleValuedFaceting(searcher, docs, field, offset, limit, mincount, missing, sort, prefix, termFilter);
            Executor executor = threads == 0 ? directExecutor : facetExecutor;
            ps.setNumThreads(threads);
            counts = ps.getFacetCounts(executor);
          }
          break;
        case UIF:
            //Emulate the JSON Faceting structure so we can use the same parsing classes
            Map<String, Object> jsonFacet = new HashMap<>(13);
            jsonFacet.put("type", "terms");
            jsonFacet.put("field", field);
            jsonFacet.put("offset", offset);
            jsonFacet.put("limit", limit);
            jsonFacet.put("mincount", mincount);
            jsonFacet.put("missing", missing);
            jsonFacet.put("prefix", prefix);
            jsonFacet.put("numBuckets", params.getFieldBool(field, "numBuckets", false));
            jsonFacet.put("allBuckets", params.getFieldBool(field, "allBuckets", false));
            jsonFacet.put("method", "uif");
            jsonFacet.put("cacheDf", 0);
            jsonFacet.put("perSeg", false);
            
            final String sortVal;
            switch(sort){
              case FacetParams.FACET_SORT_COUNT_LEGACY:
                sortVal = FacetParams.FACET_SORT_COUNT;
              break;
              case FacetParams.FACET_SORT_INDEX_LEGACY:
                sortVal = FacetParams.FACET_SORT_INDEX;
              break;
              default:
                sortVal = sort;
            }
            jsonFacet.put(SORT, sortVal );

            //TODO do we handle debug?  Should probably already be handled by the legacy code

            Object resObj = FacetRequest.parseOneFacetReq(req, jsonFacet).process(req, docs);
            //Go through the response to build the expected output for SimpleFacets
            counts = new NamedList<>();
            if(resObj != null) {
              @SuppressWarnings({"unchecked"})
              NamedList<Object> res = (NamedList<Object>) resObj;

              @SuppressWarnings({"unchecked"})
              List<NamedList<Object>> buckets = (List<NamedList<Object>>)res.get("buckets");
              for(NamedList<Object> b : buckets) {
                counts.add(b.get("val").toString(), (Integer)b.get("count"));
              }
              if(missing) {
                @SuppressWarnings({"unchecked"})
                NamedList<Object> missingCounts = (NamedList<Object>) res.get("missing");
                counts.add(null, (Integer)missingCounts.get("count"));
              }
            }
          break;
        case FC:
          counts = DocValuesFacets.getCounts(searcher, docs, field, offset,limit, mincount, missing, sort, prefix, termFilter, fdebug);
          break;
        default:
          throw new AssertionError();
      }
    }

    if (fdebug != null) {
      long timeElapsed = (long) timer.getTime();
      fdebug.setElapse(timeElapsed);
    }

    return counts;
  }

   /**
    * @param existsRequested facet.exists=true is passed for the given field
    * */
  static FacetMethod selectFacetMethod(String fieldName, 
                                       SchemaField field, FacetMethod method, Integer mincount,
                                       boolean existsRequested) {
    if (existsRequested) {
      checkMincountOnExists(fieldName, mincount);
      if (method == null) {
        method = FacetMethod.ENUM;
      }
    }
    final FacetMethod facetMethod = selectFacetMethod(field, method, mincount);
    
    if (existsRequested && facetMethod!=FacetMethod.ENUM) {
      throw new SolrException (ErrorCode.BAD_REQUEST, 
          FacetParams.FACET_EXISTS + "=true is requested, but "+
          FacetParams.FACET_METHOD+"="+FacetParams.FACET_METHOD_enum+ " can't be used with "+fieldName
      );
    }
    return facetMethod;
  }
    
  /**
   * This method will force the appropriate facet method even if the user provided a different one as a request parameter
   *
   * N.B. this method could overwrite what you passed as request parameter. Be Extra careful
   *
   * @param field field we are faceting
   * @param method the facet method passed as a request parameter
   * @param mincount the minimum value a facet should have to be returned
   * @return the FacetMethod to use
   */
   static FacetMethod selectFacetMethod(SchemaField field, FacetMethod method, Integer mincount) {

     FieldType type = field.getType();
     if (type.isPointField()) {
       // Only FCS is supported for PointFields for now
       return FacetMethod.FCS;
     }

     /*The user did not specify any preference*/
     if (method == null) {
       /* Always use filters for booleans if not DocValues only... we know the number of values is very small. */
       if (type instanceof BoolField && (field.indexed() == true || field.hasDocValues() == false)) {
         method = FacetMethod.ENUM;
       } else if (type.getNumberType() != null && !field.multiValued()) {
        /* the per-segment approach is optimal for numeric field types since there
           are no global ords to merge and no need to create an expensive
           top-level reader */
         method = FacetMethod.FCS;
       } else {
         // TODO: default to per-segment or not?
         method = FacetMethod.FC;
       }
     }

     /* FC without docValues does not support single valued numeric facets */
     if (method == FacetMethod.FC
         && type.getNumberType() != null && !field.multiValued()) {
       method = FacetMethod.FCS;
     }

     /* UIF without DocValues can't deal with mincount=0, the reason is because
         we create the buckets based on the values present in the result set.
         So we are not going to see facet values which are not in the result set */
     if (method == FacetMethod.UIF
         && !field.hasDocValues() && mincount == 0) {
       method = field.multiValued() ? FacetMethod.FC : FacetMethod.FCS;
     }

     /* Unless isUninvertible() is true, we prohibit any use of UIF...
        Here we just force FC(S) instead, and trust that the DocValues faceting logic will
        do the right thing either way (with or w/o docvalues) */
     if (FacetMethod.UIF == method && ! field.isUninvertible()) {
       method = field.multiValued() ? FacetMethod.FC : FacetMethod.FCS;
     }
     
     /* ENUM can't deal with trie fields that index several terms per value */
     if (method == FacetMethod.ENUM
         && TrieField.getMainValuePrefix(type) != null) {
       method = field.multiValued() ? FacetMethod.FC : FacetMethod.FCS;
     }

     /* FCS can't deal with multi token fields */
     final boolean multiToken = field.multiValued() || type.multiValuedFieldCache();
     if (method == FacetMethod.FCS
         && multiToken) {
       method = FacetMethod.FC;
     }

     return method;
  }

  public NamedList<Integer> getGroupedCounts(SolrIndexSearcher searcher,
                                             DocSet base,
                                             String field,
                                             boolean multiToken,
                                             int offset,
                                             int limit,
                                             int mincount,
                                             boolean missing,
                                             String sort,
                                             String prefix,
                                             Predicate<BytesRef> termFilter) throws IOException {
    GroupingSpecification groupingSpecification = rb.getGroupingSpec();
    String[] groupFields = groupingSpecification != null? groupingSpecification.getFields(): null;
    final String groupField = ArrayUtils.isNotEmpty(groupFields) ? groupFields[0] : null;
    if (groupField == null) {
      throw new SolrException (
          SolrException.ErrorCode.BAD_REQUEST,
          "Specify the group.field as parameter or local parameter"
      );
    }

    BytesRef prefixBytesRef = prefix != null ? new BytesRef(prefix) : null;
    final TermGroupFacetCollector collector = TermGroupFacetCollector.createTermGroupFacetCollector(groupField, field, multiToken, prefixBytesRef, 128);
    
    Collector groupWrapper = getInsanityWrapper(groupField, collector);
    Collector fieldWrapper = getInsanityWrapper(field, groupWrapper);
    // When GroupedFacetCollector can handle numerics we can remove the wrapped collectors
    searcher.search(base.getTopFilter(), fieldWrapper);
    
    boolean orderByCount = sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY);
    TermGroupFacetCollector.GroupedFacetResult result 
      = collector.mergeSegmentResults(limit < 0 ? Integer.MAX_VALUE : 
                                      (offset + limit), 
                                      mincount, orderByCount);

    CharsRefBuilder charsRef = new CharsRefBuilder();
    FieldType facetFieldType = searcher.getSchema().getFieldType(field);
    NamedList<Integer> facetCounts = new NamedList<>();
    List<TermGroupFacetCollector.FacetEntry> scopedEntries 
      = result.getFacetEntries(offset, limit < 0 ? Integer.MAX_VALUE : limit);
    for (TermGroupFacetCollector.FacetEntry facetEntry : scopedEntries) {
      //:TODO:can we filter earlier than this to make it more efficient?
      if (termFilter != null && !termFilter.test(facetEntry.getValue())) {
        continue;
      }
      facetFieldType.indexedToReadable(facetEntry.getValue(), charsRef);
      facetCounts.add(charsRef.toString(), facetEntry.getCount());
    }

    if (missing) {
      facetCounts.add(null, result.getTotalMissingCount());
    }

    return facetCounts;
  }
  
  private Collector getInsanityWrapper(final String field, Collector collector) {
    SchemaField sf = searcher.getSchema().getFieldOrNull(field);
    if (sf != null && !sf.hasDocValues() && !sf.multiValued() && sf.getType().getNumberType() != null) {
      // it's a single-valued numeric field: we must currently create insanity :(
      // there isn't a GroupedFacetCollector that works on numerics right now...
      return new FilterCollector(collector) {
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
          LeafReader insane = Insanity.wrapInsanity(context.reader(), field);
          return in.getLeafCollector(insane.getContext());
        }
      };
    } else {
      return collector;
    }
  }


  static final Executor directExecutor = new Executor() {
    @Override
    public void execute(Runnable r) {
      r.run();
    }
  };

  private final Executor facetExecutor;
  
  /**
   * Returns a list of value constraints and the associated facet counts 
   * for each facet field specified in the params.
   *
   * @see FacetParams#FACET_FIELD
   * @see #getFieldMissingCount
   * @see #getFacetTermEnumCounts
   */
  @SuppressWarnings("unchecked")
  public NamedList<Object> getFacetFieldCounts()
      throws IOException, SyntaxError {

    NamedList<Object> res = new SimpleOrderedMap<>();
    String[] facetFs = global.getParams(FacetParams.FACET_FIELD);
    if (null == facetFs) {
      return res;
    }

    // Passing a negative number for FACET_THREADS implies an unlimited number of threads is acceptable.
    // Also, a subtlety of directExecutor is that no matter how many times you "submit" a job, it's really
    // just a method call in that it's run by the calling thread.
    int maxThreads = req.getParams().getInt(FacetParams.FACET_THREADS, 0);
    Executor executor = maxThreads == 0 ? directExecutor : facetExecutor;
    final Semaphore semaphore = new Semaphore((maxThreads <= 0) ? Integer.MAX_VALUE : maxThreads);
    @SuppressWarnings({"rawtypes"})
    List<Future<NamedList>> futures = new ArrayList<>(facetFs.length);

    if (fdebugParent != null) {
      fdebugParent.putInfoItem("maxThreads", maxThreads);
    }

    try {
      //Loop over fields; submit to executor, keeping the future
      for (String f : facetFs) {
        if (fdebugParent != null) {
          fdebug = new FacetDebugInfo();
          fdebugParent.addChild(fdebug);
        }
        final ParsedParams parsed = parseParams(FacetParams.FACET_FIELD, f);
        final SolrParams localParams = parsed.localParams;
        final String termList = localParams == null ? null : localParams.get(CommonParams.TERMS);
        final String key = parsed.key;
        final String facetValue = parsed.facetValue;
        @SuppressWarnings({"rawtypes"})
        Callable<NamedList> callable = () -> {
          try {
            NamedList<Object> result = new SimpleOrderedMap<>();
            if(termList != null) {
              List<String> terms = StrUtils.splitSmart(termList, ",", true);
              result.add(key, getListedTermCounts(facetValue, parsed, terms));
            } else {
              result.add(key, getTermCounts(facetValue, parsed));
            }
            return result;
          } catch (SolrException se) {
            throw se;
          } 
          catch(ExitableDirectoryReader.ExitingReaderException timeout) {
            throw timeout;
          }
          catch (Exception e) {
            throw new SolrException(ErrorCode.SERVER_ERROR,
                                    "Exception during facet.field: " + facetValue, e);
          } finally {
            semaphore.release();
          }
        };

        @SuppressWarnings({"rawtypes"})
        RunnableFuture<NamedList> runnableFuture = new FutureTask<>(callable);
        semaphore.acquire();//may block and/or interrupt
        executor.execute(runnableFuture);//releases semaphore when done
        futures.add(runnableFuture);
      }//facetFs loop

      //Loop over futures to get the values. The order is the same as facetFs but shouldn't matter.
      for (@SuppressWarnings({"rawtypes"})Future<NamedList> future : futures) {
        res.addAll(future.get());
      }
      assert semaphore.availablePermits() >= maxThreads;
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error while processing facet fields: InterruptedException", e);
    } catch (ExecutionException ee) {
      Throwable e = ee.getCause();//unwrap
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error while processing facet fields: " + e.toString(), e);
    }

    return res;
  }

  /**
   * Computes the term-&gt;count counts for the specified term values relative to the
   *
   * @param field the name of the field to compute term counts against
   * @param parsed contains the docset to compute term counts relative to
   * @param terms a list of term values (in the specified field) to compute the counts for
   */
  protected NamedList<Integer> getListedTermCounts(String field, final ParsedParams parsed, List<String> terms)
      throws IOException {
    final String sort = parsed.params.getFieldParam(field, FacetParams.FACET_SORT, "empty");
    final SchemaField sf = searcher.getSchema().getField(field);
    final FieldType ft = sf.getType();
    final DocSet baseDocset = parsed.docs;
    final NamedList<Integer> res = new NamedList<>();
    Stream<String> inputStream = terms.stream();
    if (sort.equals(FacetParams.FACET_SORT_INDEX)) { // it might always make sense
      inputStream = inputStream.sorted();
    }
    Stream<SimpleImmutableEntry<String,Integer>> termCountEntries = inputStream
        .map((term) -> new SimpleImmutableEntry<>(term, numDocs(term, sf, ft, baseDocset)));
    if (sort.equals(FacetParams.FACET_SORT_COUNT)) {
      termCountEntries = termCountEntries.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
    }
    termCountEntries.forEach(e -> res.add(e.getKey(), e.getValue()));
    return res;
  }

  private int numDocs(String term, final SchemaField sf, final FieldType ft, final DocSet baseDocset) {
    try {
      return searcher.numDocs(ft.getFieldTermQuery(null, sf, term), baseDocset);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
  }

  /**
   * Returns a count of the documents in the set which do not have any 
   * terms for for the specified field.
   *
   * @see FacetParams#FACET_MISSING
   */
  public static int getFieldMissingCount(SolrIndexSearcher searcher, DocSet docs, String fieldName)
    throws IOException {
    SchemaField sf = searcher.getSchema().getField(fieldName);
    DocSet hasVal = searcher.getDocSet
        (sf.getType().getRangeQuery(null, sf, null, null, false, false));
    return docs.andNotSize(hasVal);
  }

  /**
   *  Works like {@link #getFacetTermEnumCounts(SolrIndexSearcher, DocSet, String, int, int, int, boolean, String, String, Predicate, boolean)}
   *  but takes a substring directly for the contains check rather than a {@link Predicate} instance.
   */
  public NamedList<Integer> getFacetTermEnumCounts(SolrIndexSearcher searcher, DocSet docs, String field, int offset, int limit, int mincount, boolean missing,
                                                   String sort, String prefix, String contains, boolean ignoreCase, boolean intersectsCheck)
    throws IOException {

    final Predicate<BytesRef> termFilter = new SubstringBytesRefFilter(contains, ignoreCase);
    return getFacetTermEnumCounts(searcher, docs, field, offset, limit, mincount, missing, sort, prefix, termFilter, intersectsCheck);
  }

  /**
   * Returns a list of terms in the specified field along with the 
   * corresponding count of documents in the set that match that constraint.
   * This method uses the FilterCache to get the intersection count between <code>docs</code>
   * and the DocSet for each term in the filter.
   *
   * @see FacetParams#FACET_LIMIT
   * @see FacetParams#FACET_ZEROS
   * @see FacetParams#FACET_MISSING
   */
  public NamedList<Integer> getFacetTermEnumCounts(SolrIndexSearcher searcher, DocSet docs, String field, int offset, int limit, int mincount, boolean missing,
                                                   String sort, String prefix, Predicate<BytesRef> termFilter, boolean intersectsCheck)
    throws IOException {
    
    /* :TODO: potential optimization...
    * cache the Terms with the highest docFreq and try them first
    * don't enum if we get our max from them
    */

    final NamedList<Integer> res = new NamedList<>();
    if (limit == 0) {
      return finalize(res, searcher, docs, field, missing);
    }

    // Minimum term docFreq in order to use the filterCache for that term.
    int minDfFilterCache = global.getFieldInt(field, FacetParams.FACET_ENUM_CACHE_MINDF, 0);

    // make sure we have a set that is fast for random access, if we will use it for that
    DocSet fastForRandomSet = docs;
    if (minDfFilterCache>0 && docs instanceof SortedIntDocSet) {
      SortedIntDocSet sset = (SortedIntDocSet)docs;
      fastForRandomSet = new HashDocSet(sset.getDocs(), 0, sset.size());
    }

    IndexSchema schema = searcher.getSchema();
    FieldType ft = schema.getFieldType(field);
    assert !ft.isPointField(): "Point Fields don't support enum method";

    boolean sortByCount = sort.equals("count") || sort.equals("true");
    final int maxsize = limit>=0 ? offset+limit : Integer.MAX_VALUE-1;
    final BoundedTreeSet<CountPair<BytesRef,Integer>> queue = sortByCount ? new BoundedTreeSet<CountPair<BytesRef,Integer>>(maxsize) : null;

    int min=mincount-1;  // the smallest value in the top 'N' values    
    int off=offset;
    int lim=limit>=0 ? limit : Integer.MAX_VALUE;

    BytesRef prefixTermBytes = null;
    if (prefix != null) {
      String indexedPrefix = ft.toInternal(prefix);
      prefixTermBytes = new BytesRef(indexedPrefix);
    }

    Terms terms = MultiTerms.getTerms(searcher.getIndexReader(), field);
    TermsEnum termsEnum = null;
    SolrIndexSearcher.DocsEnumState deState = null;
    BytesRef term = null;
    if (terms != null) {
      termsEnum = terms.iterator();

      // TODO: OPT: if seek(ord) is supported for this termsEnum, then we could use it for
      // facet.offset when sorting by index order.

      if (prefixTermBytes != null) {
        if (termsEnum.seekCeil(prefixTermBytes) == TermsEnum.SeekStatus.END) {
          termsEnum = null;
        } else {
          term = termsEnum.term();
        }
      } else {
        // position termsEnum on first term
        term = termsEnum.next();
      }
    }

    PostingsEnum postingsEnum = null;
    CharsRefBuilder charsRef = new CharsRefBuilder();

    if (docs.size() >= mincount) {
      while (term != null) {

        if (prefixTermBytes != null && !StringHelper.startsWith(term, prefixTermBytes))
          break;

        if (termFilter == null || termFilter.test(term)) {
          int df = termsEnum.docFreq();

          // If we are sorting, we can use df>min (rather than >=) since we
          // are going in index order.  For certain term distributions this can
          // make a large difference (for example, many terms with df=1).
          if (df > 0 && df > min) {
            int c;

            if (df >= minDfFilterCache) {
              // use the filter cache

              if (deState == null) {
                deState = new SolrIndexSearcher.DocsEnumState();
                deState.fieldName = field;
                deState.liveDocs = searcher.getLiveDocsBits();
                deState.termsEnum = termsEnum;
                deState.postingsEnum = postingsEnum;
              }

              if (intersectsCheck) {
                c = searcher.intersects(docs, deState) ? 1 : 0;
              } else {
                c = searcher.numDocs(docs, deState);
              }

              postingsEnum = deState.postingsEnum;
            } else {
              // iterate over TermDocs to calculate the intersection

              // TODO: specialize when base docset is a bitset or hash set (skipDocs)?  or does it matter for this?
              // TODO: do this per-segment for better efficiency (MultiDocsEnum just uses base class impl)
              // TODO: would passing deleted docs lead to better efficiency over checking the fastForRandomSet?
              postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
              c = 0;

              if (postingsEnum instanceof MultiPostingsEnum) {
                MultiPostingsEnum.EnumWithSlice[] subs = ((MultiPostingsEnum) postingsEnum).getSubs();
                int numSubs = ((MultiPostingsEnum) postingsEnum).getNumSubs();
                
                SEGMENTS_LOOP:
                for (int subindex = 0; subindex < numSubs; subindex++) {
                  MultiPostingsEnum.EnumWithSlice sub = subs[subindex];
                  if (sub.postingsEnum == null) continue;
                  int base = sub.slice.start;
                  int docid;
                  while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (fastForRandomSet.exists(docid + base)) {
                      c++;
                      if (intersectsCheck) {
                        assert c==1;
                        break SEGMENTS_LOOP;
                      }
                    }
                  }
                }
              } else {
                int docid;
                while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid)) {
                    c++;
                    if (intersectsCheck) {
                      assert c==1;
                      break;
                    }
                  }
                }
              }

            }

            if (sortByCount) {
              if (c > min) {
                BytesRef termCopy = BytesRef.deepCopyOf(term);
                queue.add(new CountPair<>(termCopy, c));
                if (queue.size() >= maxsize) min = queue.last().val;
              }
            } else {
              if (c >= mincount && --off < 0) {
                if (--lim < 0) break;
                ft.indexedToReadable(term, charsRef);
                res.add(charsRef.toString(), c);
              }
            }
          }
        }
        term = termsEnum.next();
      }
    }

    if (sortByCount) {
      for (CountPair<BytesRef,Integer> p : queue) {
        if (--off>=0) continue;
        if (--lim<0) break;
        ft.indexedToReadable(p.key, charsRef);
        res.add(charsRef.toString(), p.val);
      }
    }

    return finalize(res, searcher, docs, field, missing);
  }

  private static NamedList<Integer> finalize(NamedList<Integer> res, SolrIndexSearcher searcher, DocSet docs,
                                             String field, boolean missing) throws IOException {
    if (missing) {
      res.add(null, getFieldMissingCount(searcher,docs,field));
    }

    return res;
  }

  public static void checkMincountOnExists(String fieldName, int mincount) {
    if (mincount > 1) {
        throw new SolrException (ErrorCode.BAD_REQUEST,
            FacetParams.FACET_MINCOUNT + "="+mincount+" exceed 1 that's not supported with " + 
                FacetParams.FACET_EXISTS + "=true for " + fieldName
        );
      }
  }

  /**
   * A simple key=&gt;val pair whose natural order is such that 
   * <b>higher</b> vals come before lower vals.
   * In case of tie vals, then <b>lower</b> keys come before higher keys.
   */
  public static class CountPair<K extends Comparable<? super K>, V extends Comparable<? super V>>
    implements Comparable<CountPair<K,V>> {

    public CountPair(K k, V v) {
      key = k; val = v;
    }
    public K key;
    public V val;
    @Override
    public int hashCode() {
      return key.hashCode() ^ val.hashCode();
    }
    @Override
    public boolean equals(Object o) {
      if (! (o instanceof CountPair)) return false;
      CountPair<?,?> that = (CountPair<?,?>) o;
      return (this.key.equals(that.key) && this.val.equals(that.val));
    }
    @Override
    public int compareTo(CountPair<K,V> o) {
      int vc = o.val.compareTo(val);
      return (0 != vc ? vc : key.compareTo(o.key));
    }
  }


  /**
   * Returns a <code>NamedList</code> with each entry having the "key" of the interval as name and the count of docs 
   * in that interval as value. All intervals added in the request are included in the returned 
   * <code>NamedList</code> (included those with 0 count), and it's required that the order of the intervals
   * is deterministic and equals in all shards of a distributed request, otherwise the collation of results
   * will fail. 
   * 
   */
  public NamedList<Object> getFacetIntervalCounts() throws IOException, SyntaxError {
    NamedList<Object> res = new SimpleOrderedMap<Object>();
    String[] fields = global.getParams(FacetParams.FACET_INTERVAL);
    if (fields == null || fields.length == 0) return res;

    for (String field : fields) {
      final ParsedParams parsed = parseParams(FacetParams.FACET_INTERVAL, field);
      String[] intervalStrs = parsed.required.getFieldParams(parsed.facetValue, FacetParams.FACET_INTERVAL_SET);
      SchemaField schemaField = searcher.getCore().getLatestSchema().getField(parsed.facetValue);
      if (parsed.params.getBool(GroupParams.GROUP_FACET, false)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Interval Faceting can't be used with " + GroupParams.GROUP_FACET);
      }
      if (schemaField.getType().isPointField() && !schemaField.hasDocValues()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can't use interval faceting on a PointField without docValues");
      }
      
      SimpleOrderedMap<Integer> fieldResults = new SimpleOrderedMap<Integer>();
      res.add(parsed.key, fieldResults);
      IntervalFacets intervalFacets = new IntervalFacets(schemaField, searcher, parsed.docs, intervalStrs, parsed.params);
      for (FacetInterval interval : intervalFacets) {
        fieldResults.add(interval.getKey(), interval.getCount());
      }
    }

    return res;
  }

  @SuppressWarnings({"rawtypes"})
  public NamedList getHeatmapCounts() throws IOException, SyntaxError {
    final NamedList<Object> resOuter = new SimpleOrderedMap<>();
    String[] unparsedFields = rb.req.getParams().getParams(FacetParams.FACET_HEATMAP);
    if (unparsedFields == null || unparsedFields.length == 0) {
      return resOuter;
    }
    if (global.getBool(GroupParams.GROUP_FACET, false)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Heatmaps can't be used with " + GroupParams.GROUP_FACET);
    }
    for (String unparsedField : unparsedFields) {
      final ParsedParams parsed = parseParams(FacetParams.FACET_HEATMAP, unparsedField); // populates facetValue, rb, params, docs

      resOuter.add(parsed.key, SpatialHeatmapFacets.getHeatmapForField(parsed.key, parsed.facetValue, rb, parsed.params, parsed.docs));
    }
    return resOuter;
  }

  public SolrParams getGlobalParams() {
    return global;
  }

  public DocSet getDocsOrig() {
    return docsOrig;
  }

  public SolrQueryRequest getRequest() {
    return req;
  }

  public ResponseBuilder getResponseBuilder() {
    return rb;
  }
}
