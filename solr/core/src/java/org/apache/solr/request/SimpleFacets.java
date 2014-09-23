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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
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
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
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
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.IntervalFacets.FacetInterval;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
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
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.DefaultSolrThreadFactory;

/**
 * A class that generates simple Facet information for a request.
 *
 * More advanced facet implementations may compose or subclass this class 
 * to leverage any of it's functionality.
 */
public class SimpleFacets {

  /** The main set of documents all facet counts should be relative to */
  protected DocSet docsOrig;
  /** Configuration params behavior should be driven by */
  protected final SolrParams orig;
  /** Searcher to use for all calculations */
  protected final SolrIndexSearcher searcher;
  protected final SolrQueryRequest req;
  protected final ResponseBuilder rb;

  protected SimpleOrderedMap<Object> facetResponse;

  // per-facet values
  protected SolrParams localParams; // localParams on this particular facet command
  protected SolrParams params;      // local+original
  protected SolrParams required;    // required version of params
  protected String facetValue;      // the field to or query to facet on (minus local params)
  protected DocSet docs;            // the base docset for this particular facet
  protected String key;             // what name should the results be stored under
  protected int threads;

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
    this.docs = this.docsOrig = docs;
    this.params = orig = params;
    this.required = new RequiredSolrParams(params);
    this.rb = rb;
  }


  protected void parseParams(String type, String param) throws SyntaxError, IOException {
    localParams = QueryParsing.getLocalParams(param, req.getParams());
    docs = docsOrig;
    facetValue = param;
    key = param;
    threads = -1;

    if (localParams == null) {
      params = orig;
      required = new RequiredSolrParams(params);
      return;
    }
    params = SolrParams.wrapDefaults(localParams, orig);
    required = new RequiredSolrParams(params);

    // remove local params unless it's a query
    if (type != FacetParams.FACET_QUERY) { // TODO Cut over to an Enum here
      facetValue = localParams.get(CommonParams.VALUE);
    }

    // reset set the default key now that localParams have been removed
    key = facetValue;

    // allow explicit set of the key
    key = localParams.get(CommonParams.OUTPUT_KEY, key);

    String threadStr = localParams.get(CommonParams.THREADS);
    if (threadStr != null) {
      threads = Integer.parseInt(threadStr);
    }

    // figure out if we need a new base DocSet
    String excludeStr = localParams.get(CommonParams.EXCLUDE);
    if (excludeStr == null) return;

    Map<?,?> tagMap = (Map<?,?>)req.getContext().get("tags");
    if (tagMap != null && rb != null) {
      List<String> excludeTagList = StrUtils.splitSmart(excludeStr,',');

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
      if (excludeSet.size() == 0) return;

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
          this.docs = base;
          return;
        }
        AbstractAllGroupHeadsCollector allGroupHeadsCollector = grouping.getCommands().get(0).createAllGroupCollector();
        searcher.search(new MatchAllDocsQuery(), base.getTopFilter(), allGroupHeadsCollector);
        this.docs = new BitDocSet(allGroupHeadsCollector.retrieveGroupHeads(searcher.maxDoc()));
      } else {
        this.docs = base;
      }
    }

  }


  /**
   * Looks at various Params to determining if any simple Facet Constraint count
   * computations are desired.
   *
   * @see #getFacetQueryCounts
   * @see #getFacetFieldCounts
   * @see #getFacetDateCounts
   * @see #getFacetRangeCounts
   * @see #getFacetIntervalCounts
   * @see FacetParams#FACET
   * @return a NamedList of Facet Count info or null
   */
  public NamedList<Object> getFacetCounts() {

    // if someone called this method, benefit of the doubt: assume true
    if (!params.getBool(FacetParams.FACET,true))
      return null;

    facetResponse = new SimpleOrderedMap<>();
    try {
      facetResponse.add("facet_queries", getFacetQueryCounts());
      facetResponse.add("facet_fields", getFacetFieldCounts());
      facetResponse.add("facet_dates", getFacetDateCounts());
      facetResponse.add("facet_ranges", getFacetRangeCounts());
      facetResponse.add("facet_intervals", getFacetIntervalCounts());

    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (SyntaxError e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, e);
    }
    return facetResponse;
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

    String[] facetQs = params.getParams(FacetParams.FACET_QUERY);

    
    if (null != facetQs && 0 != facetQs.length) {
      for (String q : facetQs) {
        parseParams(FacetParams.FACET_QUERY, q);

        // TODO: slight optimization would prevent double-parsing of any localParams
        Query qobj = QParser.getParser(q, null, req).getQuery();

        if (qobj == null) {
          res.add(key, 0);
        } else if (params.getBool(GroupParams.GROUP_FACET, false)) {
          res.add(key, getGroupedFacetQueryCount(qobj));
        } else {
          res.add(key, searcher.numDocs(qobj, docs));
        }
      }
    }

    return res;
  }
  
  /**
   * Returns a grouped facet count for the facet query
   *
   * @see FacetParams#FACET_QUERY
   */
  public int getGroupedFacetQueryCount(Query facetQuery) throws IOException {
    String groupField = params.get(GroupParams.GROUP_FIELD);
    if (groupField == null) {
      throw new SolrException (
          SolrException.ErrorCode.BAD_REQUEST,
          "Specify the group.field as parameter or local parameter"
      );
    }
    
    TermAllGroupsCollector collector = new TermAllGroupsCollector(groupField);
    Filter mainQueryFilter = docs.getTopFilter(); // This returns a filter that only matches documents matching with q param and fq params
    searcher.search(facetQuery, mainQueryFilter, collector);
    return collector.getGroupCount();
  }

  enum FacetMethod {
    ENUM, FC, FCS;
  }

  /**
   * Term counts for use in pivot faceting that resepcts the appropriate mincount
   * @see FacetParams#FACET_PIVOT_MINCOUNT
   */
  public NamedList<Integer> getTermCountsForPivots(String field, DocSet docs) throws IOException {
    Integer mincount = params.getFieldInt(field, FacetParams.FACET_PIVOT_MINCOUNT, 1);
    return getTermCounts(field, mincount, docs);
  }

  /**
   * Term counts for use in field faceting that resepects the appropriate mincount
   *
   * @see FacetParams#FACET_MINCOUNT
   */
  public NamedList<Integer> getTermCounts(String field) throws IOException {
    return getTermCounts(field, this.docs);
  }

  /**
   * Term counts for use in field faceting that resepects the appropriate mincount
   *
   * @see FacetParams#FACET_MINCOUNT
   */
  public NamedList<Integer> getTermCounts(String field, DocSet base) throws IOException {
    Integer mincount = params.getFieldInt(field, FacetParams.FACET_MINCOUNT);
    return getTermCounts(field, mincount, base);
  }

  /**
   * Term counts for use in field faceting that resepcts the specified mincount - 
   * if mincount is null, the "zeros" param is consulted for the appropriate backcompat 
   * default
   *
   * @see FacetParams#FACET_ZEROS
   */
  private NamedList<Integer> getTermCounts(String field, Integer mincount, DocSet base) throws IOException {
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
    String prefix = params.getFieldParam(field,FacetParams.FACET_PREFIX);


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
      counts = getGroupedCounts(searcher, base, field, multiToken, offset,limit, mincount, missing, sort, prefix);
    } else {
      assert method != null;
      switch (method) {
        case ENUM:
          assert TrieField.getMainValuePrefix(ft) == null;
          counts = getFacetTermEnumCounts(searcher, base, field, offset, limit, mincount,missing,sort,prefix);
          break;
        case FCS:
          assert !multiToken;
          if (ft.getNumericType() != null && !sf.multiValued()) {
            // force numeric faceting
            if (prefix != null && !prefix.isEmpty()) {
              throw new SolrException(ErrorCode.BAD_REQUEST, FacetParams.FACET_PREFIX + " is not supported on numeric types");
            }
            counts = NumericFacets.getCounts(searcher, base, field, offset, limit, mincount, missing, sort);
          } else {
            PerSegmentSingleValuedFaceting ps = new PerSegmentSingleValuedFaceting(searcher, base, field, offset,limit, mincount, missing, sort, prefix);
            Executor executor = threads == 0 ? directExecutor : facetExecutor;
            ps.setNumThreads(threads);
            counts = ps.getFacetCounts(executor);
          }
          break;
        case FC:
          counts = DocValuesFacets.getCounts(searcher, base, field, offset,limit, mincount, missing, sort, prefix);
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
                                             String prefix) throws IOException {
    GroupingSpecification groupingSpecification = rb.getGroupingSpec();
    final String groupField  = groupingSpecification != null ? groupingSpecification.getFields()[0] : null;
    if (groupField == null) {
      throw new SolrException (
          SolrException.ErrorCode.BAD_REQUEST,
          "Specify the group.field as parameter or local parameter"
      );
    }

    BytesRef prefixBR = prefix != null ? new BytesRef(prefix) : null;
    final TermGroupFacetCollector collector = TermGroupFacetCollector.createTermGroupFacetCollector(groupField, field, multiToken, prefixBR, 128);
    
    SchemaField sf = searcher.getSchema().getFieldOrNull(groupField);
    
    if (sf != null && sf.hasDocValues() == false && sf.multiValued() == false && sf.getType().getNumericType() != null) {
      // its a single-valued numeric field: we must currently create insanity :(
      // there isnt a GroupedFacetCollector that works on numerics right now...
      searcher.search(new MatchAllDocsQuery(), base.getTopFilter(), new FilterCollector(collector) {
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
          LeafReader insane = Insanity.wrapInsanity(context.reader(), groupField);
          return in.getLeafCollector(insane.getContext());
        }
      });
    } else {
      searcher.search(new MatchAllDocsQuery(), base.getTopFilter(), collector);
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

  static final Executor facetExecutor = new ThreadPoolExecutor(
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
    String[] facetFs = params.getParams(FacetParams.FACET_FIELD);
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
        parseParams(FacetParams.FACET_FIELD, f);
        final String termList = localParams == null ? null : localParams.get(CommonParams.TERMS);
        final String workerKey = key;
        final String workerFacetValue = facetValue;
        final DocSet workerBase = this.docs;
        Callable<NamedList> callable = new Callable<NamedList>() {
          @Override
          public NamedList call() throws Exception {
            try {
              NamedList<Object> result = new SimpleOrderedMap<>();
              if(termList != null) {
                List<String> terms = StrUtils.splitSmart(termList, ",", true);
                result.add(workerKey, getListedTermCounts(workerFacetValue, workerBase, terms));
              } else {
                result.add(workerKey, getTermCounts(workerFacetValue, workerBase));
              }
              return result;
            } catch (SolrException se) {
              throw se;
            } catch (Exception e) {
              throw new SolrException(ErrorCode.SERVER_ERROR,
                                      "Exception during facet.field: " + workerFacetValue, e);
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
   * Computes the term-&gt;count counts for the specified termList relative to the 
   * @param field the name of the field to compute term counts against
   * @param termList a comma seperated (and backslash escaped) list of term values (in the specified field) to compute the counts for
   * @see StrUtils#splitSmart
   */
  private NamedList<Integer> getListedTermCounts(String field, String termList) throws IOException {
    List<String> terms = StrUtils.splitSmart(termList, ",", true);
    return getListedTermCounts(field, this.docs, terms);
  }

  /**
   * Computes the term-&gt;count counts for the specified term values relative to the 
   * @param field the name of the field to compute term counts against
   * @param base the docset to compute term counts relative to
   * @param terms a list of term values (in the specified field) to compute the counts for 
   */
  protected NamedList<Integer> getListedTermCounts(String field, DocSet base, List<String> terms) throws IOException {
    FieldType ft = searcher.getSchema().getFieldType(field);
    NamedList<Integer> res = new NamedList<>();
    for (String term : terms) {
      String internal = ft.toInternal(term);
      int count = searcher.numDocs(new TermQuery(new Term(field, internal)), base);
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
  public NamedList<Integer> getFacetTermEnumCounts(SolrIndexSearcher searcher, DocSet docs, String field, int offset, int limit, int mincount, boolean missing, String sort, String prefix)
    throws IOException {

    /* :TODO: potential optimization...
    * cache the Terms with the highest docFreq and try them first
    * don't enum if we get our max from them
    */

    // Minimum term docFreq in order to use the filterCache for that term.
    int minDfFilterCache = params.getFieldInt(field, FacetParams.FACET_ENUM_CACHE_MINDF, 0);

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

    BytesRef startTermBytes = null;
    if (prefix != null) {
      String indexedPrefix = ft.toInternal(prefix);
      startTermBytes = new BytesRef(indexedPrefix);
    }

    Fields fields = r.fields();
    Terms terms = fields==null ? null : fields.terms(field);
    TermsEnum termsEnum = null;
    SolrIndexSearcher.DocsEnumState deState = null;
    BytesRef term = null;
    if (terms != null) {
      termsEnum = terms.iterator(null);

      // TODO: OPT: if seek(ord) is supported for this termsEnum, then we could use it for
      // facet.offset when sorting by index order.

      if (startTermBytes != null) {
        if (termsEnum.seekCeil(startTermBytes) == TermsEnum.SeekStatus.END) {
          termsEnum = null;
        } else {
          term = termsEnum.term();
        }
      } else {
        // position termsEnum on first term
        term = termsEnum.next();
      }
    }

    DocsEnum docsEnum = null;
    CharsRefBuilder charsRef = new CharsRefBuilder();

    if (docs.size() >= mincount) {
      while (term != null) {

        if (startTermBytes != null && !StringHelper.startsWith(term, startTermBytes))
          break;

        int df = termsEnum.docFreq();

        // If we are sorting, we can use df>min (rather than >=) since we
        // are going in index order.  For certain term distributions this can
        // make a large difference (for example, many terms with df=1).
        if (df>0 && df>min) {
          int c;

          if (df >= minDfFilterCache) {
            // use the filter cache

            if (deState==null) {
              deState = new SolrIndexSearcher.DocsEnumState();
              deState.fieldName = field;
              deState.liveDocs = r.getLiveDocs();
              deState.termsEnum = termsEnum;
              deState.docsEnum = docsEnum;
            }

            c = searcher.numDocs(docs, deState);

            docsEnum = deState.docsEnum;
          } else {
            // iterate over TermDocs to calculate the intersection

            // TODO: specialize when base docset is a bitset or hash set (skipDocs)?  or does it matter for this?
            // TODO: do this per-segment for better efficiency (MultiDocsEnum just uses base class impl)
            // TODO: would passing deleted docs lead to better efficiency over checking the fastForRandomSet?
            docsEnum = termsEnum.docs(null, docsEnum, DocsEnum.FLAG_NONE);
            c=0;

            if (docsEnum instanceof MultiDocsEnum) {
              MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
              int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
              for (int subindex = 0; subindex<numSubs; subindex++) {
                MultiDocsEnum.EnumWithSlice sub = subs[subindex];
                if (sub.docsEnum == null) continue;
                int base = sub.slice.start;
                int docid;
                while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid+base)) c++;
                }
              }
            } else {
              int docid;
              while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid)) c++;
              }
            }
            

          }

          if (sortByCount) {
            if (c>min) {
              BytesRef termCopy = BytesRef.deepCopyOf(term);
              queue.add(new CountPair<>(termCopy, c));
              if (queue.size()>=maxsize) min=queue.last().val;
            }
          } else {
            if (c >= mincount && --off<0) {
              if (--lim<0) break;
              ft.indexedToReadable(term, charsRef);
              res.add(charsRef.toString(), c);
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
   * Returns a list of value constraints and the associated facet counts 
   * for each facet date field, range, and interval specified in the
   * SolrParams
   *
   * @see FacetParams#FACET_DATE
   * @deprecated Use getFacetRangeCounts which is more generalized
   */
  @Deprecated
  public NamedList<Object> getFacetDateCounts()
    throws IOException, SyntaxError {

    final NamedList<Object> resOuter = new SimpleOrderedMap<>();
    final String[] fields = params.getParams(FacetParams.FACET_DATE);

    if (null == fields || 0 == fields.length) return resOuter;

    for (String f : fields) {
      getFacetDateCounts(f, resOuter);
    }

    return resOuter;
  }

  /**
   * @deprecated Use getFacetRangeCounts which is more generalized
   */
  @Deprecated
  public void getFacetDateCounts(String dateFacet, NamedList<Object> resOuter)
      throws IOException, SyntaxError {

    final IndexSchema schema = searcher.getSchema();

    parseParams(FacetParams.FACET_DATE, dateFacet);
    String f = facetValue;


    final NamedList<Object> resInner = new SimpleOrderedMap<>();
    resOuter.add(key, resInner);
    final SchemaField sf = schema.getField(f);
    if (! (sf.getType() instanceof TrieDateField)) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Can not date facet on a field which is not a TrieDateField: " + f);
    }
    final TrieDateField ft = (TrieDateField) sf.getType();
    final String startS
        = required.getFieldParam(f,FacetParams.FACET_DATE_START);
    final Date start;
    try {
      start = ft.parseMath(null, startS);
    } catch (SolrException e) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "date facet 'start' is not a valid Date string: " + startS, e);
    }
    final String endS
        = required.getFieldParam(f,FacetParams.FACET_DATE_END);
    Date end; // not final, hardend may change this
    try {
      end = ft.parseMath(null, endS);
    } catch (SolrException e) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "date facet 'end' is not a valid Date string: " + endS, e);
    }

    if (end.before(start)) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "date facet 'end' comes before 'start': "+endS+" < "+startS);
    }

    final String gap = required.getFieldParam(f,FacetParams.FACET_DATE_GAP);
    final DateMathParser dmp = new DateMathParser();

    final int minCount = params.getFieldInt(f,FacetParams.FACET_MINCOUNT, 0);

    String[] iStrs = params.getFieldParams(f,FacetParams.FACET_DATE_INCLUDE);
    // Legacy support for default of [lower,upper,edge] for date faceting
    // this is not handled by FacetRangeInclude.parseParam because
    // range faceting has differnet defaults
    final EnumSet<FacetRangeInclude> include = 
      (null == iStrs || 0 == iStrs.length ) ?
      EnumSet.of(FacetRangeInclude.LOWER, 
                 FacetRangeInclude.UPPER, 
                 FacetRangeInclude.EDGE)
      : FacetRangeInclude.parseParam(iStrs);

    try {
      Date low = start;
      while (low.before(end)) {
        dmp.setNow(low);
        String label = ft.toExternal(low);

        Date high = dmp.parseMath(gap);
        if (end.before(high)) {
          if (params.getFieldBool(f,FacetParams.FACET_DATE_HARD_END,false)) {
            high = end;
          } else {
            end = high;
          }
        }
        if (high.before(low)) {
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "date facet infinite loop (is gap negative?)");
        }
        if (high.equals(low)) {
          throw new SolrException
            (SolrException.ErrorCode.BAD_REQUEST,
             "date facet infinite loop: gap is effectively zero");
        }
        final boolean includeLower =
            (include.contains(FacetRangeInclude.LOWER) ||
                (include.contains(FacetRangeInclude.EDGE) && low.equals(start)));
        final boolean includeUpper =
            (include.contains(FacetRangeInclude.UPPER) ||
                (include.contains(FacetRangeInclude.EDGE) && high.equals(end)));

        final int count = rangeCount(sf,low,high,includeLower,includeUpper);
        if (count >= minCount) {
          resInner.add(label, count);
        }
        low = high;
      }
    } catch (java.text.ParseException e) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "date facet 'gap' is not a valid Date Math string: " + gap, e);
    }

    // explicitly return the gap and end so all the counts
    // (including before/after/between) are meaningful - even if mincount
    // has removed the neighboring ranges
    resInner.add("gap", gap);
    resInner.add("start", start);
    resInner.add("end", end);

    final String[] othersP =
        params.getFieldParams(f,FacetParams.FACET_DATE_OTHER);
    if (null != othersP && 0 < othersP.length ) {
      final Set<FacetRangeOther> others = EnumSet.noneOf(FacetRangeOther.class);

      for (final String o : othersP) {
        others.add(FacetRangeOther.get(o));
      }

      // no matter what other values are listed, we don't do
      // anything if "none" is specified.
      if (! others.contains(FacetRangeOther.NONE) ) {
        boolean all = others.contains(FacetRangeOther.ALL);

        if (all || others.contains(FacetRangeOther.BEFORE)) {
          // include upper bound if "outer" or if first gap doesn't already include it
          resInner.add(FacetRangeOther.BEFORE.toString(),
              rangeCount(sf,null,start,
                  false,
                  (include.contains(FacetRangeInclude.OUTER) ||
                      (! (include.contains(FacetRangeInclude.LOWER) ||
                          include.contains(FacetRangeInclude.EDGE))))));
        }
        if (all || others.contains(FacetRangeOther.AFTER)) {
          // include lower bound if "outer" or if last gap doesn't already include it
          resInner.add(FacetRangeOther.AFTER.toString(),
              rangeCount(sf,end,null,
                  (include.contains(FacetRangeInclude.OUTER) ||
                      (! (include.contains(FacetRangeInclude.UPPER) ||
                          include.contains(FacetRangeInclude.EDGE)))),
                  false));
        }
        if (all || others.contains(FacetRangeOther.BETWEEN)) {
          resInner.add(FacetRangeOther.BETWEEN.toString(),
              rangeCount(sf,start,end,
                  (include.contains(FacetRangeInclude.LOWER) ||
                      include.contains(FacetRangeInclude.EDGE)),
                  (include.contains(FacetRangeInclude.UPPER) ||
                      include.contains(FacetRangeInclude.EDGE))));
        }
      }
    }
  }

  
  /**
   * Returns a list of value constraints and the associated facet
   * counts for each facet numerical field, range, and interval
   * specified in the SolrParams
   *
   * @see FacetParams#FACET_RANGE
   */

  public NamedList<Object> getFacetRangeCounts() throws IOException, SyntaxError {
    final NamedList<Object> resOuter = new SimpleOrderedMap<>();
    final String[] fields = params.getParams(FacetParams.FACET_RANGE);

    if (null == fields || 0 == fields.length) return resOuter;

    for (String f : fields) {
      getFacetRangeCounts(f, resOuter);
    }

    return resOuter;
  }

  void getFacetRangeCounts(String facetRange, NamedList<Object> resOuter)
      throws IOException, SyntaxError {

    final IndexSchema schema = searcher.getSchema();

    parseParams(FacetParams.FACET_RANGE, facetRange);
    String f = facetValue;

    final SchemaField sf = schema.getField(f);
    final FieldType ft = sf.getType();

    RangeEndpointCalculator<?> calc = null;

    if (ft instanceof TrieField) {
      final TrieField trie = (TrieField)ft;

      switch (trie.getType()) {
        case FLOAT:
          calc = new FloatRangeEndpointCalculator(sf);
          break;
        case DOUBLE:
          calc = new DoubleRangeEndpointCalculator(sf);
          break;
        case INTEGER:
          calc = new IntegerRangeEndpointCalculator(sf);
          break;
        case LONG:
          calc = new LongRangeEndpointCalculator(sf);
          break;
        case DATE:
          calc = new DateRangeEndpointCalculator(sf, null);
          break;
        default:
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "Unable to range facet on tried field of unexpected type:" + f);
      }
    } else {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Unable to range facet on field:" + sf);
    }

    resOuter.add(key, getFacetRangeCounts(sf, calc));
  }

  private <T extends Comparable<T>> NamedList getFacetRangeCounts
    (final SchemaField sf,
     final RangeEndpointCalculator<T> calc) throws IOException {
    
    final String f = sf.getName();
    final NamedList<Object> res = new SimpleOrderedMap<>();
    final NamedList<Integer> counts = new NamedList<>();
    res.add("counts", counts);

    final T start = calc.getValue(required.getFieldParam(f,FacetParams.FACET_RANGE_START));
    // not final, hardend may change this
    T end = calc.getValue(required.getFieldParam(f,FacetParams.FACET_RANGE_END));
    if (end.compareTo(start) < 0) {
      throw new SolrException
        (SolrException.ErrorCode.BAD_REQUEST,
         "range facet 'end' comes before 'start': "+end+" < "+start);
    }
    
    final String gap = required.getFieldParam(f, FacetParams.FACET_RANGE_GAP);
    // explicitly return the gap.  compute this early so we are more 
    // likely to catch parse errors before attempting math
    res.add("gap", calc.getGap(gap));
    
    final int minCount = params.getFieldInt(f,FacetParams.FACET_MINCOUNT, 0);
    
    final EnumSet<FacetRangeInclude> include = FacetRangeInclude.parseParam
      (params.getFieldParams(f,FacetParams.FACET_RANGE_INCLUDE));
    
    T low = start;
    
    while (low.compareTo(end) < 0) {
      T high = calc.addGap(low, gap);
      if (end.compareTo(high) < 0) {
        if (params.getFieldBool(f,FacetParams.FACET_RANGE_HARD_END,false)) {
          high = end;
        } else {
          end = high;
        }
      }
      if (high.compareTo(low) < 0) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           "range facet infinite loop (is gap negative? did the math overflow?)");
      }
      if (high.compareTo(low) == 0) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           "range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow: " + low + " + " + gap + " = " + high );
      }
      
      final boolean includeLower = 
        (include.contains(FacetRangeInclude.LOWER) ||
         (include.contains(FacetRangeInclude.EDGE) && 
          0 == low.compareTo(start)));
      final boolean includeUpper = 
        (include.contains(FacetRangeInclude.UPPER) ||
         (include.contains(FacetRangeInclude.EDGE) && 
          0 == high.compareTo(end)));
      
      final String lowS = calc.formatValue(low);
      final String highS = calc.formatValue(high);

      final int count = rangeCount(sf, lowS, highS,
                                   includeLower,includeUpper);
      if (count >= minCount) {
        counts.add(lowS, count);
      }
      
      low = high;
    }
    
    // explicitly return the start and end so all the counts 
    // (including before/after/between) are meaningful - even if mincount
    // has removed the neighboring ranges
    res.add("start", start);
    res.add("end", end);
    
    final String[] othersP =
      params.getFieldParams(f,FacetParams.FACET_RANGE_OTHER);
    if (null != othersP && 0 < othersP.length ) {
      Set<FacetRangeOther> others = EnumSet.noneOf(FacetRangeOther.class);
      
      for (final String o : othersP) {
        others.add(FacetRangeOther.get(o));
      }
      
      // no matter what other values are listed, we don't do
      // anything if "none" is specified.
      if (! others.contains(FacetRangeOther.NONE) ) {
        
        boolean all = others.contains(FacetRangeOther.ALL);
        final String startS = calc.formatValue(start);
        final String endS = calc.formatValue(end);

        if (all || others.contains(FacetRangeOther.BEFORE)) {
          // include upper bound if "outer" or if first gap doesn't already include it
          res.add(FacetRangeOther.BEFORE.toString(),
                  rangeCount(sf,null,startS,
                             false,
                             (include.contains(FacetRangeInclude.OUTER) ||
                              (! (include.contains(FacetRangeInclude.LOWER) ||
                                  include.contains(FacetRangeInclude.EDGE))))));
          
        }
        if (all || others.contains(FacetRangeOther.AFTER)) {
          // include lower bound if "outer" or if last gap doesn't already include it
          res.add(FacetRangeOther.AFTER.toString(),
                  rangeCount(sf,endS,null,
                             (include.contains(FacetRangeInclude.OUTER) ||
                              (! (include.contains(FacetRangeInclude.UPPER) ||
                                  include.contains(FacetRangeInclude.EDGE)))),  
                             false));
        }
        if (all || others.contains(FacetRangeOther.BETWEEN)) {
         res.add(FacetRangeOther.BETWEEN.toString(),
                 rangeCount(sf,startS,endS,
                            (include.contains(FacetRangeInclude.LOWER) ||
                             include.contains(FacetRangeInclude.EDGE)),
                            (include.contains(FacetRangeInclude.UPPER) ||
                             include.contains(FacetRangeInclude.EDGE))));
         
        }
      }
    }
    return res;
  }  
  
  /**
   * Macro for getting the numDocs of range over docs
   * @see SolrIndexSearcher#numDocs
   * @see TermRangeQuery
   */
  protected int rangeCount(SchemaField sf, String low, String high,
                           boolean iLow, boolean iHigh) throws IOException {
    Query rangeQ = sf.getType().getRangeQuery(null, sf, low, high, iLow, iHigh);
    if (params.getBool(GroupParams.GROUP_FACET, false)) {
      return getGroupedFacetQueryCount(rangeQ);
    } else {
      return searcher.numDocs(rangeQ , docs);
    }
  }

  /**
   * @deprecated Use rangeCount(SchemaField,String,String,boolean,boolean) which is more generalized
   */
  @Deprecated
  protected int rangeCount(SchemaField sf, Date low, Date high,
                           boolean iLow, boolean iHigh) throws IOException {
    Query rangeQ = ((TrieDateField)(sf.getType())).getRangeQuery(null, sf, low, high, iLow, iHigh);
    return searcher.numDocs(rangeQ, docs);
  }
  
  /**
   * A simple key=>val pair whose natural order is such that 
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
   * Perhaps someday instead of having a giant "instanceof" case 
   * statement to pick an impl, we can add a "RangeFacetable" marker 
   * interface to FieldTypes and they can return instances of these 
   * directly from some method -- but until then, keep this locked down 
   * and private.
   */
  private static abstract class RangeEndpointCalculator<T extends Comparable<T>> {
    protected final SchemaField field;
    public RangeEndpointCalculator(final SchemaField field) {
      this.field = field;
    }

    /**
     * Formats a Range endpoint for use as a range label name in the response.
     * Default Impl just uses toString()
     */
    public String formatValue(final T val) {
      return val.toString();
    }
    /**
     * Parses a String param into an Range endpoint value throwing 
     * a useful exception if not possible
     */
    public final T getValue(final String rawval) {
      try {
        return parseVal(rawval);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Can't parse value "+rawval+" for field: " + 
                                field.getName(), e);
      }
    }
    /**
     * Parses a String param into an Range endpoint. 
     * Can throw a low level format exception as needed.
     */
    protected abstract T parseVal(final String rawval) 
      throws java.text.ParseException;

    /** 
     * Parses a String param into a value that represents the gap and 
     * can be included in the response, throwing 
     * a useful exception if not possible.
     *
     * Note: uses Object as the return type instead of T for things like 
     * Date where gap is just a DateMathParser string 
     */
    public final Object getGap(final String gap) {
      try {
        return parseGap(gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Can't parse gap "+gap+" for field: " + 
                                field.getName(), e);
      }
    }

    /**
     * Parses a String param into a value that represents the gap and 
     * can be included in the response. 
     * Can throw a low level format exception as needed.
     *
     * Default Impl calls parseVal
     */
    protected Object parseGap(final String rawval) 
      throws java.text.ParseException {
      return parseVal(rawval);
    }

    /**
     * Adds the String gap param to a low Range endpoint value to determine 
     * the corrisponding high Range endpoint value, throwing 
     * a useful exception if not possible.
     */
    public final T addGap(T value, String gap) {
      try {
        return parseAndAddGap(value, gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Can't add gap "+gap+" to value " + value +
                                " for field: " + field.getName(), e);
      }
    }
    /**
     * Adds the String gap param to a low Range endpoint value to determine 
     * the corrisponding high Range endpoint value.
     * Can throw a low level format exception as needed.
     */
    protected abstract T parseAndAddGap(T value, String gap) 
      throws java.text.ParseException;

  }

  private static class FloatRangeEndpointCalculator 
    extends RangeEndpointCalculator<Float> {

    public FloatRangeEndpointCalculator(final SchemaField f) { super(f); }
    @Override
    protected Float parseVal(String rawval) {
      return Float.valueOf(rawval);
    }
    @Override
    public Float parseAndAddGap(Float value, String gap) {
      return new Float(value.floatValue() + Float.valueOf(gap).floatValue());
    }
  }
  private static class DoubleRangeEndpointCalculator 
    extends RangeEndpointCalculator<Double> {

    public DoubleRangeEndpointCalculator(final SchemaField f) { super(f); }
    @Override
    protected Double parseVal(String rawval) {
      return Double.valueOf(rawval);
    }
    @Override
    public Double parseAndAddGap(Double value, String gap) {
      return new Double(value.doubleValue() + Double.valueOf(gap).doubleValue());
    }
  }
  private static class IntegerRangeEndpointCalculator 
    extends RangeEndpointCalculator<Integer> {

    public IntegerRangeEndpointCalculator(final SchemaField f) { super(f); }
    @Override
    protected Integer parseVal(String rawval) {
      return Integer.valueOf(rawval);
    }
    @Override
    public Integer parseAndAddGap(Integer value, String gap) {
      return new Integer(value.intValue() + Integer.valueOf(gap).intValue());
    }
  }
  private static class LongRangeEndpointCalculator 
    extends RangeEndpointCalculator<Long> {

    public LongRangeEndpointCalculator(final SchemaField f) { super(f); }
    @Override
    protected Long parseVal(String rawval) {
      return Long.valueOf(rawval);
    }
    @Override
    public Long parseAndAddGap(Long value, String gap) {
      return new Long(value.longValue() + Long.valueOf(gap).longValue());
    }
  }
  private static class DateRangeEndpointCalculator 
    extends RangeEndpointCalculator<Date> {
    private final Date now;
    public DateRangeEndpointCalculator(final SchemaField f, 
                                       final Date now) { 
      super(f); 
      this.now = now;
      if (! (field.getType() instanceof TrieDateField) ) {
        throw new IllegalArgumentException
          ("SchemaField must use field type extending TrieDateField");
      }
    }
    @Override
    public String formatValue(Date val) {
      return ((TrieDateField)field.getType()).toExternal(val);
    }
    @Override
    protected Date parseVal(String rawval) {
      return ((TrieDateField)field.getType()).parseMath(now, rawval);
    }
    @Override
    protected Object parseGap(final String rawval) {
      return rawval;
    }
    @Override
    public Date parseAndAddGap(Date value, String gap) throws java.text.ParseException {
      final DateMathParser dmp = new DateMathParser();
      dmp.setNow(value);
      return dmp.parseMath(gap);
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
    String[] fields = params.getParams(FacetParams.FACET_INTERVAL);
    if (fields == null || fields.length == 0) return res;

    for (String field : fields) {
      parseParams(FacetParams.FACET_INTERVAL, field);
      String[] intervalStrs = required.getFieldParams(field, FacetParams.FACET_INTERVAL_SET);
      SchemaField schemaField = searcher.getCore().getLatestSchema().getField(field);
      if (!schemaField.hasDocValues()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Interval Faceting only on fields with doc values");
      }
      if (params.getBool(GroupParams.GROUP_FACET, false)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Interval Faceting can't be used with " + GroupParams.GROUP_FACET);
      }
      
      SimpleOrderedMap<Integer> fieldResults = new SimpleOrderedMap<Integer>();
      res.add(field, fieldResults);
      IntervalFacets intervalFacets = new IntervalFacets(schemaField, searcher, docs, intervalStrs, params);
      for (FacetInterval interval : intervalFacets) {
        fieldResults.add(interval.getKey(), interval.getCount());
      }
    }

    return res;
  }

}

