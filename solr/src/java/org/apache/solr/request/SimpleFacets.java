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

package org.apache.solr.request;

import org.apache.lucene.index.*;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.packed.Direct16;
import org.apache.lucene.util.packed.Direct32;
import org.apache.lucene.util.packed.Direct8;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.*;
import org.apache.solr.search.*;
import org.apache.solr.util.BoundedTreeSet;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.util.LongPriorityQueue;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A class that generates simple Facet information for a request.
 *
 * More advanced facet implementations may compose or subclass this class 
 * to leverage any of it's functionality.
 */
public class SimpleFacets {

  /** The main set of documents all facet counts should be relative to */
  protected DocSet docs;
  /** Configuration params behavior should be driven by */
  protected SolrParams params;
  protected SolrParams required;
  /** Searcher to use for all calculations */
  protected SolrIndexSearcher searcher;
  protected SolrQueryRequest req;
  protected ResponseBuilder rb;

  protected SimpleOrderedMap<Object> facetResponse;

  // per-facet values
  SolrParams localParams; // localParams on this particular facet command
  String facetValue;      // the field to or query to facet on (minus local params)
  DocSet base;            // the base docset for this particular facet
  String key;             // what name should the results be stored under
  int threads;

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
    this.base = this.docs = docs;
    this.params = params;
    this.required = new RequiredSolrParams(params);
    this.rb = rb;
  }


  void parseParams(String type, String param) throws ParseException, IOException {
    localParams = QueryParsing.getLocalParams(param, req.getParams());
    base = docs;
    facetValue = param;
    key = param;
    threads = -1;

    if (localParams == null) return;

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

      IdentityHashMap<Query,Boolean> excludeSet = new IdentityHashMap<Query,Boolean>();
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

      List<Query> qlist = new ArrayList<Query>();

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
      base = searcher.getDocSet(qlist);
    }

  }


  /**
   * Looks at various Params to determing if any simple Facet Constraint count
   * computations are desired.
   *
   * @see #getFacetQueryCounts
   * @see #getFacetFieldCounts
   * @see #getFacetDateCounts
   * @see #getFacetRangeCounts
   * @see FacetParams#FACET
   * @return a NamedList of Facet Count info or null
   */
  public NamedList<Object> getFacetCounts() {

    // if someone called this method, benefit of the doubt: assume true
    if (!params.getBool(FacetParams.FACET,true))
      return null;

    facetResponse = new SimpleOrderedMap<Object>();
    try {
      facetResponse.add("facet_queries", getFacetQueryCounts());
      facetResponse.add("facet_fields", getFacetFieldCounts());
      facetResponse.add("facet_dates", getFacetDateCounts());
      facetResponse.add("facet_ranges", getFacetRangeCounts());

    } catch (Exception e) {
      SolrException.logOnce(SolrCore.log, "Exception during facet counts", e);
      addException("Exception during facet counts", e);
    }
    return facetResponse;
  }

  public void addException(String msg, Exception e) {
    @SuppressWarnings("unchecked") 
      List<String> exceptions = (List<String>)facetResponse.get("exception");

    if (exceptions == null) {
      exceptions = new ArrayList<String>();
      facetResponse.add("exception", exceptions);
    }

    String entry = msg + '\n' + SolrException.toStr(e);
    exceptions.add(entry);
  }

  /**
   * Returns a list of facet counts for each of the facet queries 
   * specified in the params
   *
   * @see FacetParams#FACET_QUERY
   */
  public NamedList<Integer> getFacetQueryCounts() throws IOException,ParseException {

    NamedList<Integer> res = new SimpleOrderedMap<Integer>();

    /* Ignore CommonParams.DF - could have init param facet.query assuming
     * the schema default with query param DF intented to only affect Q.
     * If user doesn't want schema default for facet.query, they should be
     * explicit.
     */
    // SolrQueryParser qp = searcher.getSchema().getSolrQueryParser(null);

    String[] facetQs = params.getParams(FacetParams.FACET_QUERY);

    if (null != facetQs && 0 != facetQs.length) {
      for (String q : facetQs) {
        try {
          parseParams(FacetParams.FACET_QUERY, q);

          // TODO: slight optimization would prevent double-parsing of any localParams
          Query qobj = QParser.getParser(q, null, req).getQuery();
          res.add(key, searcher.numDocs(qobj, base));
        }
        catch (Exception e) {
          String msg = "Exception during facet.query of " + q;
          SolrException.logOnce(SolrCore.log, msg, e);
          addException(msg , e);
        }
      }
    }

    return res;
  }


  public NamedList<Integer> getTermCounts(String field) throws IOException {
    int offset = params.getFieldInt(field, FacetParams.FACET_OFFSET, 0);
    int limit = params.getFieldInt(field, FacetParams.FACET_LIMIT, 100);
    if (limit == 0) return new NamedList<Integer>();
    Integer mincount = params.getFieldInt(field, FacetParams.FACET_MINCOUNT);
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
    String method = params.getFieldParam(field, FacetParams.FACET_METHOD);
    boolean enumMethod = FacetParams.FACET_METHOD_enum.equals(method);

    // TODO: default to per-segment or not?
    boolean per_segment = FacetParams.FACET_METHOD_fcs.equals(method);

    if (method == null && ft instanceof BoolField) {
      // Always use filters for booleans... we know the number of values is very small.
      enumMethod = true;
    }
    boolean multiToken = sf.multiValued() || ft.multiValuedFieldCache();

    if (TrieField.getMainValuePrefix(ft) != null) {
      // A TrieField with multiple parts indexed per value... currently only
      // UnInvertedField can handle this case, so force it's use.
      enumMethod = false;
      multiToken = true;
    }

    // unless the enum method is explicitly specified, use a counting method.
    if (enumMethod) {
      counts = getFacetTermEnumCounts(searcher, base, field, offset, limit, mincount,missing,sort,prefix);
    } else {
      if (multiToken) {
        UnInvertedField uif = UnInvertedField.getUnInvertedField(field, searcher);
        counts = uif.getCounts(searcher, base, offset, limit, mincount,missing,sort,prefix);
      } else {
        // TODO: future logic could use filters instead of the fieldcache if
        // the number of terms in the field is small enough.

        if (per_segment) {
          PerSegmentSingleValuedFaceting ps = new PerSegmentSingleValuedFaceting(searcher, base, field, offset,limit, mincount, missing, sort, prefix);
          Executor executor = threads==0 ? directExecutor : facetExecutor;
          ps.setNumThreads(threads);
          counts = ps.getFacetCounts(executor);
        } else {
          counts = getFieldCacheCounts(searcher, base, field, offset,limit, mincount, missing, sort, prefix);         
        }

      }
    }

    return counts;
  }


  static final Executor directExecutor = new Executor() {
    public void execute(Runnable r) {
      r.run();
    }
  };

  static final Executor facetExecutor = new ThreadPoolExecutor(
          0,
          Integer.MAX_VALUE,
          10, TimeUnit.SECONDS, // terminate idle threads after 10 sec
          new SynchronousQueue<Runnable>()  // directly hand off tasks
  );
  
  /**
   * Returns a list of value constraints and the associated facet counts 
   * for each facet field specified in the params.
   *
   * @see FacetParams#FACET_FIELD
   * @see #getFieldMissingCount
   * @see #getFacetTermEnumCounts
   */
  public NamedList<Object> getFacetFieldCounts()
          throws IOException, ParseException {

    NamedList<Object> res = new SimpleOrderedMap<Object>();
    String[] facetFs = params.getParams(FacetParams.FACET_FIELD);
    if (null != facetFs) {
      for (String f : facetFs) {
        try {
          parseParams(FacetParams.FACET_FIELD, f);
          String termList = localParams == null ? null : localParams.get(CommonParams.TERMS);
          if (termList != null) {
            res.add(key, getListedTermCounts(facetValue, termList));
          } else {
            res.add(key, getTermCounts(facetValue));
          }
        } catch (Exception e) {
          String msg = "Exception during facet.field of " + f;
          SolrException.logOnce(SolrCore.log, msg, e);
          addException(msg , e);
        }
      }
    }
    return res;
  }


  private NamedList<Integer> getListedTermCounts(String field, String termList) throws IOException {
    FieldType ft = searcher.getSchema().getFieldType(field);
    List<String> terms = StrUtils.splitSmart(termList, ",", true);
    NamedList<Integer> res = new NamedList<Integer>();
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

    DocSet hasVal = searcher.getDocSet
      (new TermRangeQuery(fieldName, null, null, false, false));
    return docs.andNotSize(hasVal);
  }


  /**
   * Use the Lucene FieldCache to get counts for each unique field value in <code>docs</code>.
   * The field must have at most one indexed token per document.
   */
  public static NamedList<Integer> getFieldCacheCounts(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort, String prefix) throws IOException {
    // TODO: If the number of terms is high compared to docs.size(), and zeros==false,
    //  we should use an alternate strategy to avoid
    //  1) creating another huge int[] for the counts
    //  2) looping over that huge int[] looking for the rare non-zeros.
    //
    // Yet another variation: if docs.size() is small and termvectors are stored,
    // then use them instead of the FieldCache.
    //

    // TODO: this function is too big and could use some refactoring, but
    // we also need a facet cache, and refactoring of SimpleFacets instead of
    // trying to pass all the various params around.

    FieldType ft = searcher.getSchema().getFieldType(fieldName);
    NamedList<Integer> res = new NamedList<Integer>();

    FieldCache.DocTermsIndex si = FieldCache.DEFAULT.getTermsIndex(searcher.getIndexReader(), fieldName);

    final BytesRef prefixRef;
    if (prefix == null) {
      prefixRef = null;
    } else if (prefix.length()==0) {
      prefix = null;
      prefixRef = null;
    } else {
      prefixRef = new BytesRef(prefix);
    }

    final BytesRef br = new BytesRef();

    int startTermIndex, endTermIndex;
    if (prefix!=null) {
      startTermIndex = si.binarySearchLookup(prefixRef, br);
      if (startTermIndex<0) startTermIndex=-startTermIndex-1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = si.binarySearchLookup(prefixRef, br);
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex-1;
    } else {
      startTermIndex=0;
      endTermIndex=si.numOrd();
    }

    final int nTerms=endTermIndex-startTermIndex;
    int missingCount = -1; 
    final CharsRef charsRef = new CharsRef(10);
    if (nTerms>0 && docs.size() >= mincount) {

      // count collection array only needs to be as big as the number of terms we are
      // going to collect counts for.
      final int[] counts = new int[nTerms];

      DocIterator iter = docs.iterator();

      PackedInts.Reader ordReader = si.getDocToOrd();
      if (ordReader instanceof Direct32) {
        int[] ords = ((Direct32)ordReader).getArray();
        if (prefix==null) {
          while (iter.hasNext()) {
            counts[ords[iter.nextDoc()]]++;
          }
        } else {
          while (iter.hasNext()) {
            int term = ords[iter.nextDoc()];
            int arrIdx = term-startTermIndex;
            if (arrIdx>=0 && arrIdx<nTerms) counts[arrIdx]++;
          }
        }
      } else if (ordReader instanceof Direct16) {
        short[] ords = ((Direct16)ordReader).getArray();
        if (prefix==null) {
          while (iter.hasNext()) {
            counts[ords[iter.nextDoc()] & 0xffff]++;
          }
        } else {
          while (iter.hasNext()) {
            int term = ords[iter.nextDoc()] & 0xffff;
            int arrIdx = term-startTermIndex;
            if (arrIdx>=0 && arrIdx<nTerms) counts[arrIdx]++;
          }
        }
      } else if (ordReader instanceof Direct8) {
        byte[] ords = ((Direct8)ordReader).getArray();
        if (prefix==null) {
          while (iter.hasNext()) {
            counts[ords[iter.nextDoc()] & 0xff]++;
          }
        } else {
          while (iter.hasNext()) {
            int term = ords[iter.nextDoc()] & 0xff;
            int arrIdx = term-startTermIndex;
            if (arrIdx>=0 && arrIdx<nTerms) counts[arrIdx]++;
          }
        }
      } else {
        while (iter.hasNext()) {
          int term = si.getOrd(iter.nextDoc());
          int arrIdx = term-startTermIndex;
          if (arrIdx>=0 && arrIdx<nTerms) counts[arrIdx]++;
        }
      }

      if (startTermIndex == 0) {
        missingCount = counts[0];
      }

      // IDEA: we could also maintain a count of "other"... everything that fell outside
      // of the top 'N'

      int off=offset;
      int lim=limit>=0 ? limit : Integer.MAX_VALUE;

      if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
        int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
        maxsize = Math.min(maxsize, nTerms);
        LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize,1000), maxsize, Long.MIN_VALUE);

        int min=mincount-1;  // the smallest value in the top 'N' values
        for (int i=(startTermIndex==0)?1:0; i<nTerms; i++) {
          int c = counts[i];
          if (c>min) {
            // NOTE: we use c>min rather than c>=min as an optimization because we are going in
            // index order, so we already know that the keys are ordered.  This can be very
            // important if a lot of the counts are repeated (like zero counts would be).

            // smaller term numbers sort higher, so subtract the term number instead
            long pair = (((long)c)<<32) + (Integer.MAX_VALUE - i);
            boolean displaced = queue.insert(pair);
            if (displaced) min=(int)(queue.top() >>> 32);
          }
        }

        // if we are deep paging, we don't have to order the highest "offset" counts.
        int collectCount = Math.max(0, queue.size() - off);
        assert collectCount <= lim;

        // the start and end indexes of our list "sorted" (starting with the highest value)
        int sortedIdxStart = queue.size() - (collectCount - 1);
        int sortedIdxEnd = queue.size() + 1;
        final long[] sorted = queue.sort(collectCount);

        for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
          long pair = sorted[i];
          int c = (int)(pair >>> 32);
          int tnum = Integer.MAX_VALUE - (int)pair;
          ft.indexedToReadable(si.lookup(startTermIndex+tnum, br), charsRef);
          res.add(charsRef.toString(), c);
        }
      
      } else {
        // add results in index order
        int i=(startTermIndex==0)?1:0;
        if (mincount<=0) {
          // if mincount<=0, then we won't discard any terms and we know exactly
          // where to start.
          i+=off;
          off=0;
        }

        for (; i<nTerms; i++) {          
          int c = counts[i];
          if (c<mincount || --off>=0) continue;
          if (--lim<0) break;
          ft.indexedToReadable(si.lookup(startTermIndex+i, br), charsRef);
          res.add(charsRef.toString(), c);
        }
      }
    }

    if (missing) {
      if (missingCount < 0) {
        missingCount = getFieldMissingCount(searcher,docs,fieldName);
      }
      res.add(null, missingCount);
    }
    
    return res;
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
    IndexReader r = searcher.getIndexReader();
    FieldType ft = schema.getFieldType(field);

    boolean sortByCount = sort.equals("count") || sort.equals("true");
    final int maxsize = limit>=0 ? offset+limit : Integer.MAX_VALUE-1;
    final BoundedTreeSet<CountPair<BytesRef,Integer>> queue = sortByCount ? new BoundedTreeSet<CountPair<BytesRef,Integer>>(maxsize) : null;
    final NamedList<Integer> res = new NamedList<Integer>();

    int min=mincount-1;  // the smallest value in the top 'N' values    
    int off=offset;
    int lim=limit>=0 ? limit : Integer.MAX_VALUE;

    BytesRef startTermBytes = null;
    if (prefix != null) {
      String indexedPrefix = ft.toInternal(prefix);
      startTermBytes = new BytesRef(indexedPrefix);
    }

    Fields fields = MultiFields.getFields(r);
    Terms terms = fields==null ? null : fields.terms(field);
    TermsEnum termsEnum = null;
    SolrIndexSearcher.DocsEnumState deState = null;
    BytesRef term = null;
    if (terms != null) {
      termsEnum = terms.iterator();

      // TODO: OPT: if seek(ord) is supported for this termsEnum, then we could use it for
      // facet.offset when sorting by index order.

      if (startTermBytes != null) {
        if (termsEnum.seekCeil(startTermBytes, true) == TermsEnum.SeekStatus.END) {
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
    CharsRef charsRef = new CharsRef(10);

    if (docs.size() >= mincount) {
      while (term != null) {

        if (startTermBytes != null && !term.startsWith(startTermBytes))
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
              deState.liveDocs = MultiFields.getLiveDocs(r);
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
            docsEnum = termsEnum.docs(null, docsEnum);
            c=0;

            if (docsEnum instanceof MultiDocsEnum) {
              MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
              int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
              for (int subindex = 0; subindex<numSubs; subindex++) {
                MultiDocsEnum.EnumWithSlice sub = subs[subindex];
                if (sub.docsEnum == null) continue;
                DocsEnum.BulkReadResult bulk = sub.docsEnum.getBulkResult();
                int base = sub.slice.start;
                for (;;) {
                  int nDocs = sub.docsEnum.read();
                  if (nDocs == 0) break;
                  int[] docArr = bulk.docs.ints;  // this might be movable outside the loop, but perhaps not worth the risk.
                  int end = bulk.docs.offset + nDocs;
                  for (int i=bulk.docs.offset; i<end; i++) {
                    if (fastForRandomSet.exists(docArr[i]+base)) c++;
                  }
                }
              }
            } else {

              // this should be the same bulk result object if sharing of the docsEnum succeeded
              DocsEnum.BulkReadResult bulk = docsEnum.getBulkResult();

              for (;;) {
                int nDocs = docsEnum.read();
                if (nDocs == 0) break;
                int[] docArr = bulk.docs.ints;  // this might be movable outside the loop, but perhaps not worth the risk.
                int end = bulk.docs.offset + nDocs;
                for (int i=bulk.docs.offset; i<end; i++) {
                  if (fastForRandomSet.exists(docArr[i])) c++;
                }
              }
            }
            

          }

          if (sortByCount) {
            if (c>min) {
              BytesRef termCopy = new BytesRef(term);
              queue.add(new CountPair<BytesRef,Integer>(termCopy, c));
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
    throws IOException, ParseException {

    final NamedList<Object> resOuter = new SimpleOrderedMap<Object>();
    final String[] fields = params.getParams(FacetParams.FACET_DATE);

    if (null == fields || 0 == fields.length) return resOuter;

    for (String f : fields) {
      try {
        getFacetDateCounts(f, resOuter);
      } catch (Exception e) {
        String msg = "Exception during facet.date of " + f;
        SolrException.logOnce(SolrCore.log, msg, e);
        addException(msg , e);
      }
    }

    return resOuter;
  }

  /**
   * @deprecated Use getFacetRangeCounts which is more generalized
   */
  @Deprecated
  public void getFacetDateCounts(String dateFacet, NamedList<Object> resOuter)
      throws IOException, ParseException {

    final IndexSchema schema = searcher.getSchema();

    parseParams(FacetParams.FACET_DATE, dateFacet);
    String f = facetValue;


    final NamedList<Object> resInner = new SimpleOrderedMap<Object>();
    resOuter.add(key, resInner);
    final SchemaField sf = schema.getField(f);
    if (! (sf.getType() instanceof DateField)) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Can not date facet on a field which is not a DateField: " + f);
    }
    final DateField ft = (DateField) sf.getType();
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
    final DateMathParser dmp = new DateMathParser(ft.UTC, Locale.US);

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

  public NamedList<Object> getFacetRangeCounts() {
    final NamedList<Object> resOuter = new SimpleOrderedMap<Object>();
    final String[] fields = params.getParams(FacetParams.FACET_RANGE);

    if (null == fields || 0 == fields.length) return resOuter;

    for (String f : fields) {
      try {
        getFacetRangeCounts(f, resOuter);
      } catch (Exception e) {
        String msg = "Exception during facet.range of " + f;
        SolrException.logOnce(SolrCore.log, msg, e);
        addException(msg , e);
      }
    }

    return resOuter;
  }

  void getFacetRangeCounts(String facetRange, NamedList<Object> resOuter)
      throws IOException, ParseException {

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
        default:
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "Unable to range facet on tried field of unexpected type:" + f);
      }
    } else if (ft instanceof DateField) {
      calc = new DateRangeEndpointCalculator(sf, null);
    } else if (ft instanceof SortableIntField) {
      calc = new IntegerRangeEndpointCalculator(sf);
    } else if (ft instanceof SortableLongField) {
      calc = new LongRangeEndpointCalculator(sf);
    } else if (ft instanceof SortableFloatField) {
      calc = new FloatRangeEndpointCalculator(sf);
    } else if (ft instanceof SortableDoubleField) {
      calc = new DoubleRangeEndpointCalculator(sf);
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
    final NamedList<Object> res = new SimpleOrderedMap<Object>();
    final NamedList<Integer> counts = new NamedList<Integer>();
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
    Query rangeQ = sf.getType().getRangeQuery(null, sf,low,high,iLow,iHigh);
    return searcher.numDocs(rangeQ ,base);
  }

  /**
   * @deprecated Use rangeCount(SchemaField,String,String,boolean,boolean) which is more generalized
   */
  @Deprecated
  protected int rangeCount(SchemaField sf, Date low, Date high,
                           boolean iLow, boolean iHigh) throws IOException {
    Query rangeQ = ((DateField)(sf.getType())).getRangeQuery(null, sf,low,high,iLow,iHigh);
    return searcher.numDocs(rangeQ ,base);
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
      return new Double(value.floatValue() + Double.valueOf(gap).floatValue());
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
      return new Long(value.intValue() + Long.valueOf(gap).intValue());
    }
  }
  private static class DateRangeEndpointCalculator 
    extends RangeEndpointCalculator<Date> {
    private final Date now;
    public DateRangeEndpointCalculator(final SchemaField f, 
                                       final Date now) { 
      super(f); 
      this.now = now;
      if (! (field.getType() instanceof DateField) ) {
        throw new IllegalArgumentException
          ("SchemaField must use filed type extending DateField");
      }
    }
    @Override
    public String formatValue(Date val) {
      return ((DateField)field.getType()).toExternal(val);
    }
    @Override
    protected Date parseVal(String rawval) {
      return ((DateField)field.getType()).parseMath(now, rawval);
    }
    @Override
    protected Object parseGap(final String rawval) {
      return rawval;
    }
    @Override
    public Date parseAndAddGap(Date value, String gap) throws java.text.ParseException {
      final DateMathParser dmp = new DateMathParser(DateField.UTC, Locale.US);
      dmp.setNow(value);
      return dmp.parseMath(gap);
    }
  }
  
}

