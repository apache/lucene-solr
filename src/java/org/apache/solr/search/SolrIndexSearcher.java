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

package org.apache.solr.search;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.OpenBitSet;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.core.SolrInfoMBean.Category;


/**
 * SolrIndexSearcher adds schema awareness and caching functionality
 * over the lucene IndexSearcher.
 *
 * @version $Id$
 * @since solr 0.9
 */

// Since the internal reader in IndexSearcher is
// package protected, I can't get to it by inheritance.
// For now, I am using delgation and creating the
// IndexReader to pass to the searcher myself.
// NOTE: as of Lucene 1.9, this has changed!

public class SolrIndexSearcher extends Searcher implements SolrInfoMBean {
  private static Logger log = Logger.getLogger(SolrIndexSearcher.class.getName());
  private final SolrCore core;
  private final IndexSchema schema;

  private final String name;
  private long openTime = System.currentTimeMillis();
  private long registerTime = 0;
  private final IndexSearcher searcher;
  private final IndexReader reader;
  private final boolean closeReader;

  private final int queryResultWindowSize;
  private final int queryResultMaxDocsCached;
  private final boolean useFilterForSortedQuery;
  public final boolean enableLazyFieldLoading;
  
  private final boolean cachingEnabled;
  private final SolrCache filterCache;
  private final SolrCache queryResultCache;
  private final SolrCache documentCache;

  private final LuceneQueryOptimizer optimizer;
  
  private final float HASHSET_INVERSE_LOAD_FACTOR;
  private final int HASHDOCSET_MAXSIZE;
  
  // map of generic caches - not synchronized since it's read-only after the constructor.
  private final HashMap<String, SolrCache> cacheMap;
  private static final HashMap<String, SolrCache> noGenericCaches=new HashMap<String,SolrCache>(0);

  // list of all caches associated with this searcher.
  private final SolrCache[] cacheList;
  private static final SolrCache[] noCaches = new SolrCache[0];

  /** Creates a searcher searching the index in the named directory. */
  public SolrIndexSearcher(SolrCore core, IndexSchema schema, String name, String path, boolean enableCache) throws IOException {
    this(core, schema,name,IndexReader.open(path), true, enableCache);
  }

  /** Creates a searcher searching the index in the provided directory. */
  public SolrIndexSearcher(SolrCore core, IndexSchema schema, String name, Directory directory, boolean enableCache) throws IOException {
    this(core, schema,name,IndexReader.open(directory), true, enableCache);
  }

  /** Creates a searcher searching the provided index. */
  public SolrIndexSearcher(SolrCore core, IndexSchema schema, String name, IndexReader r, boolean enableCache) {
    this(core, schema,name,r, false, enableCache);
  }

  private SolrIndexSearcher(SolrCore core, IndexSchema schema, String name, IndexReader r, boolean closeReader, boolean enableCache) {
    this.core = core;
    this.schema = schema;
    this.name = "Searcher@" + Integer.toHexString(hashCode()) + (name!=null ? " "+name : "");

    log.info("Opening " + this.name);

    reader = r;
    searcher = new IndexSearcher(r);
    this.closeReader = closeReader;
    searcher.setSimilarity(schema.getSimilarity());

    SolrConfig solrConfig = schema.getSolrConfig();
    queryResultWindowSize = solrConfig.queryResultWindowSize;
    queryResultMaxDocsCached = solrConfig.queryResultMaxDocsCached;
    useFilterForSortedQuery = solrConfig.useFilterForSortedQuery;
    enableLazyFieldLoading = solrConfig.enableLazyFieldLoading;
    
    cachingEnabled=enableCache;
    if (cachingEnabled) {
      ArrayList<SolrCache> clist = new ArrayList<SolrCache>();
      filterCache= solrConfig.filterCacheConfig==null ? null : solrConfig.filterCacheConfig.newInstance();
      if (filterCache!=null) clist.add(filterCache);
      queryResultCache = solrConfig.queryResultCacheConfig==null ? null : solrConfig.queryResultCacheConfig.newInstance();
      if (queryResultCache!=null) clist.add(queryResultCache);
      documentCache = solrConfig.documentCacheConfig==null ? null : solrConfig.documentCacheConfig.newInstance();
      if (documentCache!=null) clist.add(documentCache);

      if (solrConfig.userCacheConfigs == null) {
        cacheMap = noGenericCaches;
      } else {
        cacheMap = new HashMap<String,SolrCache>(solrConfig.userCacheConfigs.length);
        for (CacheConfig userCacheConfig : solrConfig.userCacheConfigs) {
          SolrCache cache = null;
          if (userCacheConfig != null) cache = userCacheConfig.newInstance();
          if (cache != null) {
            cacheMap.put(cache.name(), cache);
            clist.add(cache);
          }
        }
      }

      cacheList = clist.toArray(new SolrCache[clist.size()]);
    } else {
      filterCache=null;
      queryResultCache=null;
      documentCache=null;
      cacheMap = noGenericCaches;
      cacheList= noCaches;
    }
    optimizer = solrConfig.filtOptEnabled ? new LuceneQueryOptimizer(solrConfig.filtOptCacheSize,solrConfig.filtOptThreshold) : null;

    // for DocSets
    HASHSET_INVERSE_LOAD_FACTOR = solrConfig.hashSetInverseLoadFactor;
    HASHDOCSET_MAXSIZE = solrConfig.hashDocSetMaxSize;
    // register self
    core.getInfoRegistry().put(this.name, this);
  }


  public String toString() {
    return name;
  }


  /** Register sub-objects such as caches
   */
  public void register() {
    for (SolrCache cache : cacheList) {
      cache.setState(SolrCache.State.LIVE);
      core.getInfoRegistry().put(cache.name(), cache);
    }
    registerTime=System.currentTimeMillis();
  }

  /**
   * Free's resources associated with this searcher.
   *
   * In particular, the underlying reader and any cache's in use are closed.
   */
  public void close() throws IOException {
    // unregister first, so no management actions are tried on a closing searcher.
    core.getInfoRegistry().remove(name);

    if (cachingEnabled) {
      StringBuilder sb = new StringBuilder();
      sb.append("Closing ").append(name);
      for (SolrCache cache : cacheList) {
        sb.append("\n\t");
        sb.append(cache);
      }
      log.info(sb.toString());
    } else {
      log.fine("Closing " + name);
    }
    try {
      searcher.close();
    }
    finally {
      if(closeReader) reader.close();
      for (SolrCache cache : cacheList) {
        cache.close();
      }
    }
  }

  /** Direct access to the IndexReader used by this searcher */
  public IndexReader getReader() { return reader; }
  /** Direct access to the IndexSchema for use with this searcher */
  public IndexSchema getSchema() { return schema; }
  //
  // Set default regenerators on filter and query caches if they don't have any
  //
  public static void initRegenerators(SolrConfig solrConfig) {
    if (solrConfig.filterCacheConfig != null && solrConfig.filterCacheConfig.getRegenerator() == null) {
      solrConfig.filterCacheConfig.setRegenerator(
              new CacheRegenerator() {
                public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache newCache, SolrCache oldCache, Object oldKey, Object oldVal) throws IOException {
                  newSearcher.cacheDocSet((Query)oldKey, null, false);
                  return true;
                }
              }
      );
    }

    if (solrConfig.queryResultCacheConfig != null && solrConfig.queryResultCacheConfig.getRegenerator() == null) {
      final int queryResultWindowSize = solrConfig.queryResultWindowSize;
      solrConfig.queryResultCacheConfig.setRegenerator(
              new CacheRegenerator() {
                public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache newCache, SolrCache oldCache, Object oldKey, Object oldVal) throws IOException {
                  QueryResultKey key = (QueryResultKey)oldKey;
                  int nDocs=1;
                  // request 1 doc and let caching round up to the next window size...
                  // unless the window size is <=1, in which case we will pick
                  // the minimum of the number of documents requested last time and
                  // a reasonable number such as 40.
                  // TODO: make more configurable later...

                  if (queryResultWindowSize<=1) {
                    DocList oldList = (DocList)oldVal;
                    int oldnDocs = oldList.offset() + oldList.size();
                    // 40 has factors of 2,4,5,10,20
                    nDocs = Math.min(oldnDocs,40);
                  }

                  DocListAndSet ret = new DocListAndSet();
                  int flags=NO_CHECK_QCACHE | key.nc_flags;

                  newSearcher.getDocListC(ret, key.query, key.filters, null, key.sort, 0, nDocs, flags);
                  return true;
                }
              }
      );
    }
  }

  public Hits search(Query query, Filter filter, Sort sort) throws IOException {
    // todo - when Solr starts accepting filters, need to
    // change this conditional check (filter!=null) and create a new filter
    // that ANDs them together if it already exists.

    if (optimizer==null || filter!=null || !(query instanceof BooleanQuery)
    ) {
      return searcher.search(query,filter,sort);
    } else {
      Query[] newQuery = new Query[1];
      Filter[] newFilter = new Filter[1];
      optimizer.optimize((BooleanQuery)query, searcher, 0, newQuery, newFilter);

      return searcher.search(newQuery[0], newFilter[0], sort);
    }
  }

  public Hits search(Query query, Filter filter) throws IOException {
    return searcher.search(query, filter);
  }

  public Hits search(Query query, Sort sort) throws IOException {
    return searcher.search(query, sort);
  }

  public void search(Query query, HitCollector results) throws IOException {
    searcher.search(query, results);
  }

  public void setSimilarity(Similarity similarity) {
    searcher.setSimilarity(similarity);
  }

  public Similarity getSimilarity() {
    return searcher.getSimilarity();
  }

  public int docFreq(Term term) throws IOException {
    return searcher.docFreq(term);
  }

  /* ********************** Document retrieval *************************/
   
  /* Future optimizations (yonik)
   *
   * If no cache is present:
   *   - use NO_LOAD instead of LAZY_LOAD
   *   - use LOAD_AND_BREAK if a single field is begin retrieved
   */

  /**
   * FieldSelector which loads the specified fields, and load all other
   * field lazily.
   */
  class SetNonLazyFieldSelector implements FieldSelector {
    private Set<String> fieldsToLoad;
    SetNonLazyFieldSelector(Set<String> toLoad) {
      fieldsToLoad = toLoad;
    }
    public FieldSelectorResult accept(String fieldName) { 
      if(fieldsToLoad.contains(fieldName))
        return FieldSelectorResult.LOAD; 
      else
        return FieldSelectorResult.LAZY_LOAD;
    }
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   */
  public Document doc(int i) throws IOException {
    return doc(i, (Set<String>)null);
  }

  /** Retrieve a {@link Document} using a {@link org.apache.lucene.document.FieldSelector}
   * This method does not currently use the Solr document cache.
   * 
   * @see IndexReader#document(int, FieldSelector) */
  public Document doc(int n, FieldSelector fieldSelector) throws IOException {
    return searcher.getIndexReader().document(n, fieldSelector);
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   *
   * Note: The document will have all fields accessable, but if a field
   * filter is provided, only the provided fields will be loaded (the 
   * remainder will be available lazily).
   */
  public Document doc(int i, Set<String> fields) throws IOException {
    
    Document d;
    if (documentCache != null) {
      d = (Document)documentCache.get(i);
      if (d!=null) return d;
    }

    if(!enableLazyFieldLoading || fields == null) {
      d = searcher.getIndexReader().document(i);
    } else {
      d = searcher.getIndexReader().document(i, 
             new SetNonLazyFieldSelector(fields));
    }

    if (documentCache != null) {
      documentCache.put(i, d);
    }

    return d;
  }

  /**
   * Takes a list of docs (the doc ids actually), and reads them into an array 
   * of Documents.
   */
  public void readDocs(Document[] docs, DocList ids) throws IOException {
    readDocs(docs, ids, null);
  }
  /**
   * Takes a list of docs (the doc ids actually) and a set of fields to load,
   * and reads them into an array of Documents.
   */
  public void readDocs(Document[] docs, DocList ids, Set<String> fields) throws IOException {
    DocIterator iter = ids.iterator();
    for (int i=0; i<docs.length; i++) {
      docs[i] = doc(iter.nextDoc(), fields);
    }
  }

  /* ********************** end document retrieval *************************/

  public int maxDoc() throws IOException {
    return searcher.maxDoc();
  }

  public TopDocs search(Weight weight, Filter filter, int i) throws IOException {
    return searcher.search(weight, filter, i);
  }

  public void search(Weight weight, Filter filter, HitCollector hitCollector) throws IOException {
    searcher.search(weight, filter, hitCollector);
  }

  public Query rewrite(Query original) throws IOException {
    return searcher.rewrite(original);
  }

  public Explanation explain(Weight weight, int i) throws IOException {
    return searcher.explain(weight, i);
  }

  public TopFieldDocs search(Weight weight, Filter filter, int i, Sort sort) throws IOException {
    return searcher.search(weight, filter, i, sort);
  }

  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the first document number containing the term <code>t</code>
   * Returns -1 if no document was found.
   * This method is primarily intended for clients that want to fetch
   * documents using a unique identifier."
   * @param t
   * @return the first document number containing the term
   */
  public int getFirstMatch(Term t) throws IOException {
    TermDocs tdocs = null;
    try {
      tdocs = reader.termDocs(t);
      if (!tdocs.next()) return -1;
      return tdocs.doc();
    } finally {
      if (tdocs!=null) tdocs.close();
    }
  }


  /**
   * Compute and cache the DocSet that matches a query.
   * The normal usage is expected to be cacheDocSet(myQuery, null,false)
   * meaning that Solr will determine if the Query warrants caching, and
   * if so, will compute the DocSet that matches the Query and cache it.
   * If the answer to the query is already cached, nothing further will be done.
   * <p>
   * If the optionalAnswer DocSet is provided, it should *not* be modified
   * after this call.
   *
   * @param query           the lucene query that will act as the key
   * @param optionalAnswer   the DocSet to be cached - if null, it will be computed.
   * @param mustCache        if true, a best effort will be made to cache this entry.
   *                         if false, heuristics may be used to determine if it should be cached.
   */
  public void cacheDocSet(Query query, DocSet optionalAnswer, boolean mustCache) throws IOException {
    // Even if the cache is null, still compute the DocSet as it may serve to warm the Lucene
    // or OS disk cache.
    if (optionalAnswer != null) {
      if (filterCache!=null) {
        filterCache.put(query,optionalAnswer);
      }
      return;
    }

    // Throw away the result, relying on the fact that getDocSet
    // will currently always cache what it found.  If getDocSet() starts
    // using heuristics about what to cache, and mustCache==true, (or if we
    // want this method to start using heuristics too) then
    // this needs to change.
    getDocSet(query);
  }

  /**
   * Returns the set of document ids matching a query.
   * This method is cache-aware and attempts to retrieve the answer from the cache if possible.
   * If the answer was not cached, it may have been inserted into the cache as a result of this call.
   * This method can handle negative queries.
   * <p>
   * The DocSet returned should <b>not</b> be modified.
   */
  public DocSet getDocSet(Query query) throws IOException {
    // Get the absolute value (positive version) of this query.  If we
    // get back the same reference, we know it's positive.
    Query absQ = QueryUtils.getAbs(query);
    boolean positive = query==absQ;

    if (filterCache != null) {
      DocSet absAnswer = (DocSet)filterCache.get(absQ);
      if (absAnswer!=null) {
        if (positive) return absAnswer;
        else return getPositiveDocSet(matchAllDocsQuery).andNot(absAnswer);
      }
    }

    DocSet absAnswer = getDocSetNC(absQ, null);
    DocSet answer = positive ? absAnswer : getPositiveDocSet(matchAllDocsQuery).andNot(absAnswer);

    if (filterCache != null) {
      // cache negative queries as positive
      filterCache.put(absQ, absAnswer);
    }

    return answer;
  }

  // only handle positive (non negative) queries
  DocSet getPositiveDocSet(Query q) throws IOException {
    DocSet answer;
    if (filterCache != null) {
      answer = (DocSet)filterCache.get(q);
      if (answer!=null) return answer;
    }
    answer = getDocSetNC(q,null);
    if (filterCache != null) filterCache.put(q,answer);
    return answer;
  }


  private static Query matchAllDocsQuery = new MatchAllDocsQuery();


  protected DocSet getDocSet(List<Query> queries) throws IOException {
    if (queries==null) return null;
    if (queries.size()==1) return getDocSet(queries.get(0));
    DocSet answer=null;

    boolean[] neg = new boolean[queries.size()];
    DocSet[] sets = new DocSet[queries.size()];

    int smallestIndex = -1;
    int smallestCount = Integer.MAX_VALUE;
    for (int i=0; i<sets.length; i++) {
      Query q = queries.get(i);
      Query posQuery = QueryUtils.getAbs(q);
      sets[i] = getPositiveDocSet(posQuery);
      // Negative query if absolute value different from original
      if (q==posQuery) {
        neg[i] = false;
        // keep track of the smallest positive set.
        // This optimization is only worth it if size() is cached, which it would
        // be if we don't do any set operations.
        int sz = sets[i].size();
        if (sz<smallestCount) {
          smallestCount=sz;
          smallestIndex=i;
          answer = sets[i];
        }
      } else {
        neg[i] = true;
      }
    }

    // if no positive queries, start off with all docs
    if (answer==null) answer = getPositiveDocSet(matchAllDocsQuery);

    // do negative queries first to shrink set size
    for (int i=0; i<sets.length; i++) {
      if (neg[i]) answer = answer.andNot(sets[i]);
    }

    for (int i=0; i<sets.length; i++) {
      if (!neg[i] && i!=smallestIndex) answer = answer.intersection(sets[i]);
    }

    return answer;
  }

  // query must be positive
  protected DocSet getDocSetNC(Query query, DocSet filter) throws IOException {
    if (filter==null) {
      DocSetHitCollector hc = new DocSetHitCollector(HASHSET_INVERSE_LOAD_FACTOR, HASHDOCSET_MAXSIZE, maxDoc());
      if (query instanceof TermQuery) {
        Term t = ((TermQuery)query).getTerm();
        TermDocs tdocs = null;
        try {
          tdocs = reader.termDocs(t);
          while (tdocs.next()) hc.collect(tdocs.doc(),0.0f);
        } finally {
          if (tdocs!=null) tdocs.close();
        }
      } else {
        searcher.search(query,null,hc);
      }
      return hc.getDocSet();

    } else {
      // FUTURE: if the filter is sorted by docid, could use skipTo (SkipQueryFilter)
      final DocSetHitCollector hc = new DocSetHitCollector(HASHSET_INVERSE_LOAD_FACTOR, HASHDOCSET_MAXSIZE, maxDoc());
      final DocSet filt = filter;
      searcher.search(query, null, new HitCollector() {
        public void collect(int doc, float score) {
          if (filt.exists(doc)) hc.collect(doc,score);
        }
      }
      );
      return hc.getDocSet();
    }
  }


  /**
   * Returns the set of document ids matching both the query and the filter.
   * This method is cache-aware and attempts to retrieve the answer from the cache if possible.
   * If the answer was not cached, it may have been inserted into the cache as a result of this call.
   * <p>
   *
   * @param query
   * @param filter may be null
   * @return DocSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   */
  public DocSet getDocSet(Query query, DocSet filter) throws IOException {
    if (filter==null) return getDocSet(query);

    // Negative query if absolute value different from original
    Query absQ = QueryUtils.getAbs(query);
    boolean positive = absQ==query;

    DocSet first;
    if (filterCache != null) {
      first = (DocSet)filterCache.get(absQ);
      if (first==null) {
        first = getDocSetNC(absQ,null);
        filterCache.put(absQ,first);
      }
      return positive ? first.intersection(filter) : filter.andNot(first);
    }

    // If there isn't a cache, then do a single filtered query if positive.
    return positive ? getDocSetNC(absQ,filter) : filter.andNot(getPositiveDocSet(absQ));
  }


  /**
  * Converts a filter into a DocSet.
  * This method is not cache-aware and no caches are checked.
  */
  public DocSet convertFilter(Filter lfilter) throws IOException {
    BitSet bs = lfilter.bits(this.reader);
    OpenBitSet obs = new OpenBitSet(bs.size());
    for(int i=bs.nextSetBit(0); i>=0; i=bs.nextSetBit(i+1)) {
      obs.fastSet(i);
    }
    return new BitDocSet(obs);
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocList getDocList(Query query, Query filter, Sort lsort, int offset, int len) throws IOException {
    List<Query> filterList = null;
    if (filter != null) {
      filterList = new ArrayList<Query>(1);
      filterList.add(filter);
    }
    return getDocList(query, filterList, lsort, offset, len, 0);
  }


  /**
   * Returns documents matching both <code>query</code> and the 
   * intersection of the <code>filterList</code>, sorted by <code>sort</code>.
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param query
   * @param filterList may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocList getDocList(Query query, List<Query> filterList, Sort lsort, int offset, int len, int flags) throws IOException {
    DocListAndSet answer = new DocListAndSet();
    getDocListC(answer,query,filterList,null,lsort,offset,len,flags);
    return answer.docList;
  }


  private static final int NO_CHECK_QCACHE       = 0x80000000;
  private static final int GET_DOCSET            = 0x40000000;
  private static final int NO_CHECK_FILTERCACHE  = 0x20000000;

  public static final int GET_SCORES             =       0x01;

  /** getDocList version that uses+populates query and filter caches.
   * This should only be called using either filterList or filter, but not both.
   */
  private void getDocListC(DocListAndSet out, Query query, List<Query> filterList, DocSet filter, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryResultKey key=null;
    int maxDoc = offset + len;
    int supersetMaxDoc=maxDoc;
    DocList superset;


    // we can try and look up the complete query in the cache.
    // we can't do that if filter!=null though (we don't want to
    // do hashCode() and equals() for a big DocSet).
    if (queryResultCache != null && filter==null) {
        // all of the current flags can be reused during warming,
        // so set all of them on the cache key.
        key = new QueryResultKey(query, filterList, lsort, flags);
        if ((flags & NO_CHECK_QCACHE)==0) {
          superset = (DocList)queryResultCache.get(key);

          if (superset != null) {
            // check that the cache entry has scores recorded if we need them
            if ((flags & GET_SCORES)==0 || superset.hasScores()) {
              // NOTE: subset() returns null if the DocList has fewer docs than
              // requested
              out.docList = superset.subset(offset,len);
            }
          }
          if (out.docList != null) {
            // found the docList in the cache... now check if we need the docset too.
            // OPT: possible future optimization - if the doclist contains all the matches,
            // use it to make the docset instead of rerunning the query.
            if (out.docSet==null && ((flags & GET_DOCSET)!=0) ) {
              if (filterList==null) {
                out.docSet = getDocSet(query);
              } else {
                List<Query> newList = new ArrayList<Query>(filterList.size()+1);
                newList.add(query);
                newList.addAll(filterList);
                out.docSet = getDocSet(newList);
              }
            }
            return;
          }
        }

        // If we are going to generate the result, bump up to the
        // next resultWindowSize for better caching.

        // handle 0 special case as well as avoid idiv in the common case.
        if (maxDoc < queryResultWindowSize) {
          supersetMaxDoc=queryResultWindowSize;
        } else {
          supersetMaxDoc = ((maxDoc-1)/queryResultWindowSize + 1)*queryResultWindowSize;
        }
    }


    // OK, so now we need to generate an answer.
    // One way to do that would be to check if we have an unordered list
    // of results for the base query.  If so, we can apply the filters and then
    // sort by the resulting set.  This can only be used if:
    // - the sort doesn't contain score
    // - we don't want score returned.

    // check if we should try and use the filter cache
    boolean useFilterCache=false;
    if ((flags & (GET_SCORES|NO_CHECK_FILTERCACHE))==0 && useFilterForSortedQuery && lsort != null && filterCache != null) {
      useFilterCache=true;
      SortField[] sfields = lsort.getSort();
      for (SortField sf : sfields) {
        if (sf.getType() == SortField.SCORE) {
          useFilterCache=false;
          break;
        }
      }
    }

    if (useFilterCache) {
      // now actually use the filter cache.
      // for large filters that match few documents, this may be
      // slower than simply re-executing the query.
      if (out.docSet == null) {
        out.docSet = getDocSet(query,filter);
        DocSet bigFilt = getDocSet(filterList);
        if (bigFilt != null) out.docSet = out.docSet.intersection(bigFilt);
      }
      // todo: there could be a sortDocSet that could take a list of
      // the filters instead of anding them first...
      // perhaps there should be a multi-docset-iterator
      superset = sortDocSet(out.docSet,lsort,supersetMaxDoc);
      out.docList = superset.subset(offset,len);
    } else {
      // do it the normal way...
      DocSet theFilt = filter!=null ? filter : getDocSet(filterList);

      if ((flags & GET_DOCSET)!=0) {
        DocSet qDocSet = getDocListAndSetNC(out,query,theFilt,lsort,0,supersetMaxDoc,flags);
        // cache the docSet matching the query w/o filtering
        if (filterCache!=null) filterCache.put(query,qDocSet);
      } else {
        out.docList = getDocListNC(query,theFilt,lsort,0,supersetMaxDoc,flags);
      }
      superset = out.docList;
      out.docList = superset.subset(offset,len);
    }

    // lastly, put the superset in the cache if the size is less than or equal
    // to queryResultMaxDocsCached
    if (key != null && superset.size() <= queryResultMaxDocsCached) {
      queryResultCache.put(key, superset);
    }
  }



  private DocList getDocListNC(Query query, DocSet filter, Sort lsort, int offset, int len, int flags) throws IOException {
    final int lastDocRequested = offset+len;
    int nDocsReturned;
    int totalHits;
    float maxScore;
    int[] ids;
    float[] scores;

    query = QueryUtils.makeQueryable(query);

    // handle zero case...
    if (lastDocRequested<=0) {
      final DocSet filt = filter;
      final float[] topscore = new float[] { Float.NEGATIVE_INFINITY };
      final int[] numHits = new int[1];

      searcher.search(query, new HitCollector() {
        public void collect(int doc, float score) {
          if (filt!=null && !filt.exists(doc)) return;
          numHits[0]++;
          if (score > topscore[0]) topscore[0]=score;
        }
      }
      );

      nDocsReturned=0;
      ids = new int[nDocsReturned];
      scores = new float[nDocsReturned];
      totalHits = numHits[0];
      maxScore = totalHits>0 ? topscore[0] : 0.0f;
    } else if (lsort != null) {
      // can't use TopDocs if there is a sort since it
      // will do automatic score normalization.
      // NOTE: this changed late in Lucene 1.9

      final DocSet filt = filter;
      final int[] numHits = new int[1];
      final FieldSortedHitQueue hq = new FieldSortedHitQueue(reader, lsort.getSort(), offset+len);

      searcher.search(query, new HitCollector() {
        public void collect(int doc, float score) {
          if (filt!=null && !filt.exists(doc)) return;
          numHits[0]++;
          hq.insert(new FieldDoc(doc, score));
        }
      }
      );

      totalHits = numHits[0];
      maxScore = totalHits>0 ? hq.getMaxScore() : 0.0f;

      nDocsReturned = hq.size();
      ids = new int[nDocsReturned];
      scores = (flags&GET_SCORES)!=0 ? new float[nDocsReturned] : null;
      for (int i = nDocsReturned -1; i >= 0; i--) {
        FieldDoc fieldDoc = (FieldDoc)hq.pop();
        // fillFields is the point where score normalization happens
        // hq.fillFields(fieldDoc)
        ids[i] = fieldDoc.doc;
        if (scores != null) scores[i] = fieldDoc.score;
      }
    } else {
      // No Sort specified (sort by score descending)
      // This case could be done with TopDocs, but would currently require
      // getting a BitSet filter from a DocSet which may be inefficient.

      final DocSet filt = filter;
      final ScorePriorityQueue hq = new ScorePriorityQueue(lastDocRequested);
      final int[] numHits = new int[1];
      searcher.search(query, new HitCollector() {
        float minScore=Float.NEGATIVE_INFINITY;  // minimum score in the priority queue
        public void collect(int doc, float score) {
          if (filt!=null && !filt.exists(doc)) return;
          if (numHits[0]++ < lastDocRequested || score >= minScore) {
            // TODO: if docs are always delivered in order, we could use "score>minScore"
            // instead of "score>=minScore" and avoid tiebreaking scores
            // in the priority queue.
            // but might BooleanScorer14 might still be used and deliver docs out-of-order?
            hq.insert(new ScoreDoc(doc, score));
            minScore = ((ScoreDoc)hq.top()).score;
          }
        }
      }
      );

      totalHits = numHits[0];
      nDocsReturned = hq.size();
      ids = new int[nDocsReturned];
      scores = (flags&GET_SCORES)!=0 ? new float[nDocsReturned] : null;
      ScoreDoc sdoc =null;
      for (int i = nDocsReturned -1; i >= 0; i--) {
        sdoc = (ScoreDoc)hq.pop();
        ids[i] = sdoc.doc;
        if (scores != null) scores[i] = sdoc.score;
      }
      maxScore = sdoc ==null ? 0.0f : sdoc.score;
    }


    int sliceLen = Math.min(lastDocRequested,nDocsReturned) - offset;
    if (sliceLen < 0) sliceLen=0;
    return new DocSlice(offset,sliceLen,ids,scores,totalHits,maxScore);



    /**************** older implementation using TopDocs *******************


      Filter lfilter=null;
      if (filter != null) {
        final BitSet bits = filter.getBits();   // avoid if possible
        lfilter = new Filter() {
          public BitSet bits(IndexReader reader)  {
            return bits;
          }
        };
      }

      int lastDocRequested=offset+len;

      // lucene doesn't allow 0 to be passed for nDocs
      if (lastDocRequested==0) lastDocRequested=1;

      // TopFieldDocs sortedDocs;  // use TopDocs so both versions can use it
      TopDocs sortedDocs;
      if (lsort!=null) {
         sortedDocs = searcher.search(query, lfilter, lastDocRequested, lsort);
      } else {
         sortedDocs = searcher.search(query, lfilter, lastDocRequested);
      }

      int nDocsReturned = sortedDocs.scoreDocs.length;
      int[] docs = new int[nDocsReturned];
      for (int i=0; i<nDocsReturned; i++) {
        docs[i] = sortedDocs.scoreDocs[i].doc;
      }
      float[] scores=null;
      float maxScore=0.0f;
      if ((flags & GET_SCORES) != 0) {
        scores = new float[nDocsReturned];
        for (int i=0; i<nDocsReturned; i++) {
          scores[i] = sortedDocs.scoreDocs[i].score;
        }
        if (nDocsReturned>0) {
          maxScore=sortedDocs.scoreDocs[0].score;
        }
      }
      int sliceLen = Math.min(offset+len,nDocsReturned) - offset;
      if (sliceLen < 0) sliceLen=0;
      return new DocSlice(offset,sliceLen,docs,scores,sortedDocs.totalHits, maxScore);

    **********************************************************************************/

  }


  // the DocSet returned is for the query only, without any filtering... that way it may
  // be cached if desired.
  private DocSet getDocListAndSetNC(DocListAndSet out, Query query, DocSet filter, Sort lsort, int offset, int len, int flags) throws IOException {
    final int lastDocRequested = offset+len;
    int nDocsReturned;
    int totalHits;
    float maxScore;
    int[] ids;
    float[] scores;
    final DocSetHitCollector setHC = new DocSetHitCollector(HASHSET_INVERSE_LOAD_FACTOR, HASHDOCSET_MAXSIZE, maxDoc());

    query = QueryUtils.makeQueryable(query);

    // TODO: perhaps unify getDocListAndSetNC and getDocListNC without imposing a significant performance hit

    // Comment: gathering the set before the filter is applied allows one to cache
    // the resulting DocSet under the query.  The drawback is that it requires an
    // extra intersection with the filter at the end.  This will be a net win
    // for expensive queries.

    // Q: what if the final intersection results in a small set from two large
    // sets... it won't be a HashDocSet or other small set.  One way around
    // this would be to collect the resulting set as we go (the filter is
    // checked anyway).

    // handle zero case...
    if (lastDocRequested<=0) {
      final DocSet filt = filter;
      final float[] topscore = new float[] { Float.NEGATIVE_INFINITY };
      final int[] numHits = new int[1];

      searcher.search(query, new HitCollector() {
        public void collect(int doc, float score) {
          setHC.collect(doc,score);
          if (filt!=null && !filt.exists(doc)) return;
          numHits[0]++;
          if (score > topscore[0]) topscore[0]=score;
        }
      }
      );

      nDocsReturned=0;
      ids = new int[nDocsReturned];
      scores = new float[nDocsReturned];
      totalHits = numHits[0];
      maxScore = totalHits>0 ? topscore[0] : 0.0f;
    } else if (lsort != null) {
      // can't use TopDocs if there is a sort since it
      // will do automatic score normalization.
      // NOTE: this changed late in Lucene 1.9

      final DocSet filt = filter;
      final int[] numHits = new int[1];
      final FieldSortedHitQueue hq = new FieldSortedHitQueue(reader, lsort.getSort(), offset+len);

      searcher.search(query, new HitCollector() {
        public void collect(int doc, float score) {
          setHC.collect(doc,score);
          if (filt!=null && !filt.exists(doc)) return;
          numHits[0]++;
          hq.insert(new FieldDoc(doc, score));
        }
      }
      );

      totalHits = numHits[0];
      maxScore = totalHits>0 ? hq.getMaxScore() : 0.0f;

      nDocsReturned = hq.size();
      ids = new int[nDocsReturned];
      scores = (flags&GET_SCORES)!=0 ? new float[nDocsReturned] : null;
      for (int i = nDocsReturned -1; i >= 0; i--) {
        FieldDoc fieldDoc = (FieldDoc)hq.pop();
        // fillFields is the point where score normalization happens
        // hq.fillFields(fieldDoc)
        ids[i] = fieldDoc.doc;
        if (scores != null) scores[i] = fieldDoc.score;
      }
    } else {
      // No Sort specified (sort by score descending)
      // This case could be done with TopDocs, but would currently require
      // getting a BitSet filter from a DocSet which may be inefficient.

      final DocSet filt = filter;
      final ScorePriorityQueue hq = new ScorePriorityQueue(lastDocRequested);
      final int[] numHits = new int[1];
      searcher.search(query, new HitCollector() {
        float minScore=Float.NEGATIVE_INFINITY;  // minimum score in the priority queue
        public void collect(int doc, float score) {
          setHC.collect(doc,score);
          if (filt!=null && !filt.exists(doc)) return;
          if (numHits[0]++ < lastDocRequested || score >= minScore) {
            // if docs are always delivered in order, we could use "score>minScore"
            // but might BooleanScorer14 might still be used and deliver docs out-of-order?
            hq.insert(new ScoreDoc(doc, score));
            minScore = ((ScoreDoc)hq.top()).score;
          }
        }
      }
      );

      totalHits = numHits[0];
      nDocsReturned = hq.size();
      ids = new int[nDocsReturned];
      scores = (flags&GET_SCORES)!=0 ? new float[nDocsReturned] : null;
      ScoreDoc sdoc =null;
      for (int i = nDocsReturned -1; i >= 0; i--) {
        sdoc = (ScoreDoc)hq.pop();
        ids[i] = sdoc.doc;
        if (scores != null) scores[i] = sdoc.score;
      }
      maxScore = sdoc ==null ? 0.0f : sdoc.score;
    }


    int sliceLen = Math.min(lastDocRequested,nDocsReturned) - offset;
    if (sliceLen < 0) sliceLen=0;
    out.docList = new DocSlice(offset,sliceLen,ids,scores,totalHits,maxScore);
    DocSet qDocSet = setHC.getDocSet();
    out.docSet = filter==null ? qDocSet : qDocSet.intersection(filter);
    return qDocSet;
  }



  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocList getDocList(Query query, DocSet filter, Sort lsort, int offset, int len) throws IOException {
    DocListAndSet answer = new DocListAndSet();
    getDocListC(answer,query,null,filter,lsort,offset,len,0);
    return answer.docList;
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.  Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, Query filter, Sort lsort, int offset, int len) throws IOException {
    List<Query> filterList = buildQueryList(filter);
    return getDocListAndSet(query, filterList, lsort, offset, len);

  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.  Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, Query filter, Sort lsort, int offset, int len, int flags) throws IOException {
	List<Query> filterList = buildQueryList(filter);
	return getDocListAndSet(query, filterList, lsort, offset, len, flags);
  }
  
  /**
   * A simple utility method for to build a filterList from a query
   * @param filter
   */
  private List<Query> buildQueryList(Query filter) {
	List<Query> filterList = null;
	if (filter != null) {
	  filterList = new ArrayList<Query>(2);
	  filterList.add(filter);
	}
	return filterList;
  }

  /**
   * Returns documents matching both <code>query</code> and the intersection 
   * of <code>filterList</code>, sorted by <code>sort</code>.  
   * Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> 
   * (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filterList   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, List<Query> filterList, Sort lsort, int offset, int len) throws IOException {
    DocListAndSet ret = new DocListAndSet();
    getDocListC(ret,query,filterList,null,lsort,offset,len,GET_DOCSET);
    return ret;
  }

  /**
   * Returns documents matching both <code>query</code> and the intersection 
   * of <code>filterList</code>, sorted by <code>sort</code>.  
   * Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> 
   * (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filterList   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, List<Query> filterList, Sort lsort, int offset, int len, int flags) throws IOException {
	    DocListAndSet ret = new DocListAndSet();
	    getDocListC(ret,query,filterList,null,lsort,offset,len, flags |= GET_DOCSET);
	    return ret;
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>. Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, DocSet filter, Sort lsort, int offset, int len) throws IOException {
    DocListAndSet ret = new DocListAndSet();
    getDocListC(ret,query,null,filter,lsort,offset,len,GET_DOCSET);
    return ret;
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.  Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may make an insertion into the cache 
   * as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param query
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException
   */
  public DocListAndSet getDocListAndSet(Query query, DocSet filter, Sort lsort, int offset, int len, int flags) throws IOException {
	    DocListAndSet ret = new DocListAndSet();
	    getDocListC(ret,query,null,filter,lsort,offset,len, flags |= GET_DOCSET);
	    return ret;
	  }

  protected DocList sortDocSet(DocSet set, Sort sort, int nDocs) throws IOException {
    final FieldSortedHitQueue hq =
            new FieldSortedHitQueue(reader, sort.getSort(), nDocs);
    DocIterator iter = set.iterator();
    int hits=0;
    while(iter.hasNext()) {
      int doc = iter.nextDoc();
      hits++;   // could just use set.size(), but that would be slower for a bitset
      hq.insert(new FieldDoc(doc,1.0f));
    }

    int numCollected = hq.size();
    int[] ids = new int[numCollected];
    for (int i = numCollected-1; i >= 0; i--) {
      FieldDoc fieldDoc = (FieldDoc)hq.pop();
      // hq.fillFields(fieldDoc)  // optional, if we need that info
      ids[i] = fieldDoc.doc;
    }

    return new DocSlice(0,numCollected,ids,null,hits,0.0f);
  }



  /**
   * Returns the number of documents that match both <code>a</code> and <code>b</code>.
   * <p>
   * This method is cache-aware and may check as well as modify the cache.
   *
   * @param a
   * @param b
   * @return the numer of documents in the intersection between <code>a</code> and <code>b</code>.
   * @throws IOException
   */
  public int numDocs(Query a, DocSet b) throws IOException {
    // Negative query if absolute value different from original
    Query absQ = QueryUtils.getAbs(a);
    DocSet positiveA = getPositiveDocSet(absQ);
    return a==absQ ? b.intersectionSize(positiveA) : b.andNotSize(positiveA);
  }

   /**
   * Returns the number of documents that match both <code>a</code> and <code>b</code>.
   * <p>
   * This method is cache-aware and may check as well as modify the cache.
   *
   * @param a
   * @param b
   * @return the numer of documents in the intersection between <code>a</code> and <code>b</code>.
   * @throws IOException
   */
  public int numDocs(Query a, Query b) throws IOException {
    Query absA = QueryUtils.getAbs(a);
    Query absB = QueryUtils.getAbs(b);     
    DocSet positiveA = getPositiveDocSet(absA);
    DocSet positiveB = getPositiveDocSet(absB);

    // Negative query if absolute value different from original
    if (a==absA) {
      if (b==absB) return positiveA.intersectionSize(positiveB);
      return positiveA.andNotSize(positiveB);
    }
    if (b==absB) return positiveB.andNotSize(positiveA);

    // if both negative, we need to create a temp DocSet since we
    // don't have a counting method that takes three.
    DocSet all = getPositiveDocSet(matchAllDocsQuery);

    // -a -b == *:*.andNot(a).andNotSize(b) == *.*.andNotSize(a.union(b))
    // we use the last form since the intermediate DocSet should normally be smaller.
    return all.andNotSize(positiveA.union(positiveB));
  }


  /**
   * Takes a list of docs (the doc ids actually), and returns an array 
   * of Documents containing all of the stored fields.
   */
  public Document[] readDocs(DocList ids) throws IOException {
     Document[] docs = new Document[ids.size()];
     readDocs(docs,ids);
     return docs;
  }



  /**
   * Warm this searcher based on an old one (primarily for auto-cache warming).
   */
  public void warm(SolrIndexSearcher old) throws IOException {
    // Make sure this is first!  filters can help queryResults execute!
    boolean logme = log.isLoggable(Level.INFO);

    // warm the caches in order...
    for (int i=0; i<cacheList.length; i++) {
      if (logme) log.info("autowarming " + this + " from " + old + "\n\t" + old.cacheList[i]);
      this.cacheList[i].warm(this, old.cacheList[i]);
      if (logme) log.info("autowarming result for " + this + "\n\t" + this.cacheList[i]);
    }
  }


  /**
   * return the named generic cache
   */
  public SolrCache getCache(String cacheName) {
    return cacheMap.get(cacheName);
  }

  /**
   * lookup an entry in a generic cache
   */
  public Object cacheLookup(String cacheName, Object key) {
    SolrCache cache = cacheMap.get(cacheName);
    return cache==null ? null : cache.get(key);
  }

  /**
   * insert an entry in a generic cache
   */
  public Object cacheInsert(String cacheName, Object key, Object val) {
    SolrCache cache = cacheMap.get(cacheName);
    return cache==null ? null : cache.put(key,val);
  }


  /////////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  public String getName() {
    return SolrIndexSearcher.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "index searcher";
  }

  public Category getCategory() {
    return Category.CORE;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    lst.add("caching", cachingEnabled);
    lst.add("numDocs", reader.numDocs());
    lst.add("maxDoc", reader.maxDoc());
    lst.add("readerImpl", reader.getClass().getSimpleName());
    lst.add("readerDir", reader.directory());
    lst.add("indexVersion", reader.getVersion());
    lst.add("openedAt", new Date(openTime));
    if (registerTime!=0) lst.add("registeredAt", new Date(registerTime));
    return lst;
  }
}



// Lucene's HitQueue isn't public, so here is our own.
final class ScorePriorityQueue extends PriorityQueue {
  ScorePriorityQueue(int size) {
    initialize(size);
  }

  protected final boolean lessThan(Object o1, Object o2) {
    ScoreDoc sd1 = (ScoreDoc)o1;
    ScoreDoc sd2 = (ScoreDoc)o2;
    // use index order as a tiebreaker to make sorts stable
    return sd1.score < sd2.score || (sd1.score==sd2.score && sd1.doc > sd2.doc);
  }
}



