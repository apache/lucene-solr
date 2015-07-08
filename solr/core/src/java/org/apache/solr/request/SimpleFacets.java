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

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiPostingsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermGroupFacetCollector;
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
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.FacetComponent;
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
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.util.BoundedTreeSet;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * A class that generates simple Facet information for a request.
 *
 * More advanced facet implementations may compose or subclass this class 
 * to leverage any of its functionality.
 */
public class SimpleFacets {
  
  private final static Logger log = LoggerFactory.getLogger(SimpleFacets.class);

  /** The main set of documents all facet counts should be relative to */
  protected DocSet docsOrig;
  /** Configuration params behavior should be driven by */
  protected final SolrParams global;
  /** Searcher to use for all calculations */
  protected final SolrIndexSearcher searcher;
  protected final SolrQueryRequest req;
  protected final ResponseBuilder rb;

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
  }

  /**
   * Returns <code>true</code> if a String contains the given substring. Otherwise
   * <code>false</code>.
   *
   * @param ref
   *          the {@link String} to test
   * @param substring
   *          the substring to look for
   * @param ignoreCase
   *          whether the comparison should be case-insensitive
   * @return Returns <code>true</code> iff the String contains the given substring.
   *         Otherwise <code>false</code>.
   */
  public static boolean contains(String ref, String substring, boolean ignoreCase) {
    if (ignoreCase)
      return StringUtils.containsIgnoreCase(ref, substring);
    return StringUtils.contains(ref, substring);
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
      Grouping grouping = new Grouping(searcher, null, rb.getQueryCommand(), false, 0, false);
      grouping.setGroupSort(rb.getGroupingSpec().getSortWithinGroup());
      if (rb.getGroupingSpec().getFields().length > 0) {
        grouping.addFieldCommand(rb.getGroupingSpec().getFields()[0], req);
      } else if (rb.getGroupingSpec().getFunctions().length > 0) {
        grouping.addFunctionCommand(rb.getGroupingSpec().getFunctions()[0], req);
      } else {
        return base;
      }
      AbstractAllGroupHeadsCollector allGroupHeadsCollector = grouping.getCommands().get(0).createAllGroupCollector();
      searcher.search(new FilteredQuery(new MatchAllDocsQuery(), base.getTopFilter()), allGroupHeadsCollector);
      return new BitDocSet(allGroupHeadsCollector.retrieveGroupHeads(searcher.maxDoc()));
    } else {
      return base;
    }
  }

  /**
   * Looks at various Params to determining if any simple Facet Constraint count
   * computations are desired.
   *
   * @return a NamedList of Facet Count info or null
   * @deprecated use {@link org.apache.solr.handler.component.FacetComponent#getFacetCounts(SimpleFacets)} instead
   */
  @Deprecated
  public NamedList<Object> getFacetCounts() {
    return FacetComponent.getFacetCounts(this);
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
    Query qobj = QParser.getParser(parsed.facetValue, null, req).getQuery();

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

    TermAllGroupsCollector collector = new TermAllGroupsCollector(groupField);
    Filter mainQueryFilter = docSet.getTopFilter(); // This returns a filter that only matches documents matching with q param and fq params
    searcher.search(new FilteredQuery(facetQuery, mainQueryFilter), collector);
    return collector.getGroupCount();
  }

  enum FacetMethod {
    ENUM, FC, FCS;
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
    if (limit == 0) return new NamedList<>();
    if (mincount==null) {
      Boolean zeros = params.getFieldBool(field, FacetParams.FACET_ZEROS);
      // mincount = (zeros!=null && zeros) ? 0 : 1;
      mincount = (zeros!=null && !zeros) ? 1 : 0;
      // current default is to include zeros.
    }
    boolean missing = params.getFieldBool(field, FacetParams.FACET_MISSING, false);
    // default to sorting if there is a limit.
    String sort = params.getFieldParam(field, FacetParams.FACET_SORT, limit>0 ? FacetParams.FACET_SORT_COUNT : FacetParams.FACET_SORT_INDEX);
    String prefix = params.getFieldParam(field, FacetParams.FACET_PREFIX);
    String contains = params.getFieldParam(field, FacetParams.FACET_CONTAINS);
    boolean ignoreCase = params.getFieldBool(field, FacetParams.FACET_CONTAINS_IGNORE_CASE, false);

    NamedList<Integer> counts;
    SchemaField sf = searcher.getSchema().getField(field);
    FieldType ft = sf.getType();

    // determine what type of faceting method to use
    final String methodStr = params.getFieldParam(field, FacetParams.FACET_METHOD);
    FacetMethod method = null;
    if (FacetParams.FACET_METHOD_enum.equals(methodStr)) {
      method = FacetMethod.ENUM;
    } else if (FacetParams.FACET_METHOD_fcs.equals(methodStr)) {
      method = FacetMethod.FCS;
    } else if (FacetParams.FACET_METHOD_fc.equals(methodStr)) {
      method = FacetMethod.FC;
    }

    if (method == FacetMethod.ENUM && TrieField.getMainValuePrefix(ft) != null) {
      // enum can't deal with trie fields that index several terms per value
      method = sf.multiValued() ? FacetMethod.FC : FacetMethod.FCS;
    }

    if (method == null && ft instanceof BoolField) {
      // Always use filters for booleans... we know the number of values is very small.
      method = FacetMethod.ENUM;
    }

    final boolean multiToken = sf.multiValued() || ft.multiValuedFieldCache();
    
    if (ft.getNumericType() != null && !sf.multiValued()) {
      // the per-segment approach is optimal for numeric field types since there
      // are no global ords to merge and no need to create an expensive
      // top-level reader
      method = FacetMethod.FCS;
    }

    if (method == null) {
      // TODO: default to per-segment or not?
      method = FacetMethod.FC;
    }

    if (method == FacetMethod.FCS && multiToken) {
      // only fc knows how to deal with multi-token fields
      method = FacetMethod.FC;
    }
    
    if (method == FacetMethod.ENUM && sf.hasDocValues()) {
      // only fc can handle docvalues types
      method = FacetMethod.FC;
    }

    if (params.getFieldBool(field, GroupParams.GROUP_FACET, false)) {
      counts = getGroupedCounts(searcher, docs, field, multiToken, offset,limit, mincount, missing, sort, prefix, contains, ignoreCase);
    } else {
      assert method != null;
      switch (method) {
        case ENUM:
          assert TrieField.getMainValuePrefix(ft) == null;
          counts = getFacetTermEnumCounts(searcher, docs, field, offset, limit, mincount,missing,sort,prefix, contains, ignoreCase, params);
          break;
        case FCS:
          assert !multiToken;
          if (ft.getNumericType() != null && !sf.multiValued()) {
            // force numeric faceting
            if (prefix != null && !prefix.isEmpty()) {
              throw new SolrException(ErrorCode.BAD_REQUEST, FacetParams.FACET_PREFIX + " is not supported on numeric types");
            }
            if (contains != null && !contains.isEmpty()) {
              throw new SolrException(ErrorCode.BAD_REQUEST, FacetParams.FACET_CONTAINS + " is not supported on numeric types");
            }
            counts = NumericFacets.getCounts(searcher, docs, field, offset, limit, mincount, missing, sort);
          } else {
            PerSegmentSingleValuedFaceting ps = new PerSegmentSingleValuedFaceting(searcher, docs, field, offset, limit, mincount, missing, sort, prefix, contains, ignoreCase);
            Executor executor = threads == 0 ? directExecutor : facetExecutor;
            ps.setNumThreads(threads);
            counts = ps.getFacetCounts(executor);
          }
          break;
        case FC:
          counts = DocValuesFacets.getCounts(searcher, docs, field, offset,limit, mincount, missing, sort, prefix, contains, ignoreCase);
          break;
        default:
          throw new AssertionError();
      }
    }

    return counts;
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
                                             String contains,
                                             boolean ignoreCase) throws IOException {
    GroupingSpecification groupingSpecification = rb.getGroupingSpec();
    final String groupField  = groupingSpecification != null ? groupingSpecification.getFields()[0] : null;
    if (groupField == null) {
      throw new SolrException (
          SolrException.ErrorCode.BAD_REQUEST,
          "Specify the group.field as parameter or local parameter"
      );
    }

    BytesRef prefixBytesRef = prefix != null ? new BytesRef(prefix) : null;
    final TermGroupFacetCollector collector = TermGroupFacetCollector.createTermGroupFacetCollector(groupField, field, multiToken, prefixBytesRef, 128);
    
    SchemaField sf = searcher.getSchema().getFieldOrNull(groupField);
    
    if (sf != null && sf.hasDocValues() == false && sf.multiValued() == false && sf.getType().getNumericType() != null) {
      // it's a single-valued numeric field: we must currently create insanity :(
      // there isn't a GroupedFacetCollector that works on numerics right now...
      searcher.search(base.getTopFilter(), new FilterCollector(collector) {
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
          LeafReader insane = Insanity.wrapInsanity(context.reader(), groupField);
          return in.getLeafCollector(insane.getContext());
        }
      });
    } else {
      searcher.search(base.getTopFilter(), collector);
    }
    
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
      //:TODO:can we do contains earlier than this to make it more efficient?
      if (contains != null && !contains(facetEntry.getValue().utf8ToString(), contains, ignoreCase)) {
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


  static final Executor directExecutor = new Executor() {
    @Override
    public void execute(Runnable r) {
      r.run();
    }
  };

  static final Executor facetExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
          0,
          Integer.MAX_VALUE,
          10, TimeUnit.SECONDS, // terminate idle threads after 10 sec
          new SynchronousQueue<Runnable>()  // directly hand off tasks
          , new DefaultSolrThreadFactory("facetExecutor")
  );
  
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
    List<Future<NamedList>> futures = new ArrayList<>(facetFs.length);

    try {
      //Loop over fields; submit to executor, keeping the future
      for (String f : facetFs) {
        final ParsedParams parsed = parseParams(FacetParams.FACET_FIELD, f);
        final SolrParams localParams = parsed.localParams;
        final String termList = localParams == null ? null : localParams.get(CommonParams.TERMS);
        final String key = parsed.key;
        final String facetValue = parsed.facetValue;
        Callable<NamedList> callable = new Callable<NamedList>() {
          @Override
          public NamedList call() throws Exception {
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
            } catch (Exception e) {
              throw new SolrException(ErrorCode.SERVER_ERROR,
                                      "Exception during facet.field: " + facetValue, e);
            } finally {
              semaphore.release();
            }
          }
        };

        RunnableFuture<NamedList> runnableFuture = new FutureTask<>(callable);
        semaphore.acquire();//may block and/or interrupt
        executor.execute(runnableFuture);//releases semaphore when done
        futures.add(runnableFuture);
      }//facetFs loop

      //Loop over futures to get the values. The order is the same as facetFs but shouldn't matter.
      for (Future<NamedList> future : futures) {
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
   * @param field the name of the field to compute term counts against
   * @param parsed contains the docset to compute term counts relative to
   * @param terms a list of term values (in the specified field) to compute the counts for 
   */
  protected NamedList<Integer> getListedTermCounts(String field, final ParsedParams parsed, List<String> terms) throws IOException {
    FieldType ft = searcher.getSchema().getFieldType(field);
    NamedList<Integer> res = new NamedList<>();
    for (String term : terms) {
      String internal = ft.toInternal(term);
      int count = searcher.numDocs(new TermQuery(new Term(field, internal)), parsed.docs);
      res.add(term, count);
    }
    return res;    
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
   * Returns a list of terms in the specified field along with the 
   * corresponding count of documents in the set that match that constraint.
   * This method uses the FilterCache to get the intersection count between <code>docs</code>
   * and the DocSet for each term in the filter.
   *
   * @see FacetParams#FACET_LIMIT
   * @see FacetParams#FACET_ZEROS
   * @see FacetParams#FACET_MISSING
   */
  public NamedList<Integer> getFacetTermEnumCounts(SolrIndexSearcher searcher, DocSet docs, String field, int offset, int limit, int mincount, boolean missing, String sort, String prefix, String contains, boolean ignoreCase, SolrParams params)
    throws IOException {

    /* :TODO: potential optimization...
    * cache the Terms with the highest docFreq and try them first
    * don't enum if we get our max from them
    */

    // Minimum term docFreq in order to use the filterCache for that term.
    int minDfFilterCache = global.getFieldInt(field, FacetParams.FACET_ENUM_CACHE_MINDF, 0);

    // make sure we have a set that is fast for random access, if we will use it for that
    DocSet fastForRandomSet = docs;
    if (minDfFilterCache>0 && docs instanceof SortedIntDocSet) {
      SortedIntDocSet sset = (SortedIntDocSet)docs;
      fastForRandomSet = new HashDocSet(sset.getDocs(), 0, sset.size());
    }


    IndexSchema schema = searcher.getSchema();
    LeafReader r = searcher.getLeafReader();
    FieldType ft = schema.getFieldType(field);

    boolean sortByCount = sort.equals("count") || sort.equals("true");
    final int maxsize = limit>=0 ? offset+limit : Integer.MAX_VALUE-1;
    final BoundedTreeSet<CountPair<BytesRef,Integer>> queue = sortByCount ? new BoundedTreeSet<CountPair<BytesRef,Integer>>(maxsize) : null;
    final NamedList<Integer> res = new NamedList<>();

    int min=mincount-1;  // the smallest value in the top 'N' values    
    int off=offset;
    int lim=limit>=0 ? limit : Integer.MAX_VALUE;

    BytesRef prefixTermBytes = null;
    if (prefix != null) {
      String indexedPrefix = ft.toInternal(prefix);
      prefixTermBytes = new BytesRef(indexedPrefix);
    }

    Fields fields = r.fields();
    Terms terms = fields==null ? null : fields.terms(field);
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

        if (contains == null || contains(term.utf8ToString(), contains, ignoreCase)) {
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
                deState.liveDocs = r.getLiveDocs();
                deState.termsEnum = termsEnum;
                deState.postingsEnum = postingsEnum;
              }

              c = searcher.numDocs(docs, deState);

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
                for (int subindex = 0; subindex < numSubs; subindex++) {
                  MultiPostingsEnum.EnumWithSlice sub = subs[subindex];
                  if (sub.postingsEnum == null) continue;
                  int base = sub.slice.start;
                  int docid;
                  while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (fastForRandomSet.exists(docid + base)) c++;
                  }
                }
              } else {
                int docid;
                while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid)) c++;
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

    if (missing) {
      res.add(null, getFieldMissingCount(searcher,docs,field));
    }

    return res;
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
      
      SimpleOrderedMap<Integer> fieldResults = new SimpleOrderedMap<Integer>();
      res.add(parsed.key, fieldResults);
      IntervalFacets intervalFacets = new IntervalFacets(schemaField, searcher, parsed.docs, intervalStrs, parsed.params);
      for (FacetInterval interval : intervalFacets) {
        fieldResults.add(interval.getKey(), interval.getCount());
      }
    }

    return res;
  }

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

