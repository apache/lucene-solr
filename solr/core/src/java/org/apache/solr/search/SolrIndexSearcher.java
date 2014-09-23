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

package org.apache.solr.search;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LazyDocument;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.request.UnInvertedField;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.SolrIndexConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SolrIndexSearcher adds schema awareness and caching functionality
 * over the lucene IndexSearcher.
 *
 *
 * @since solr 0.9
 */
public class SolrIndexSearcher extends IndexSearcher implements Closeable,SolrInfoMBean {

  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();


  private static Logger log = LoggerFactory.getLogger(SolrIndexSearcher.class);
  private final SolrCore core;
  private final IndexSchema schema;

  private boolean debug = log.isDebugEnabled();

  private final String name;
  private long openTime = System.currentTimeMillis();
  private long registerTime = 0;
  private long warmupTime = 0;
  private final DirectoryReader reader;
  private final boolean closeReader;

  private final int queryResultWindowSize;
  private final int queryResultMaxDocsCached;
  private final boolean useFilterForSortedQuery;
  public final boolean enableLazyFieldLoading;
  
  private final boolean cachingEnabled;
  private final SolrCache<Query,DocSet> filterCache;
  private final SolrCache<QueryResultKey,DocList> queryResultCache;
  private final SolrCache<Integer,StoredDocument> documentCache;
  private final SolrCache<String,UnInvertedField> fieldValueCache;

  private final LuceneQueryOptimizer optimizer;
  
  // map of generic caches - not synchronized since it's read-only after the constructor.
  private final HashMap<String, SolrCache> cacheMap;
  private static final HashMap<String, SolrCache> noGenericCaches=new HashMap<>(0);

  // list of all caches associated with this searcher.
  private final SolrCache[] cacheList;
  private static final SolrCache[] noCaches = new SolrCache[0];
  
  private final FieldInfos fieldInfos;
  // TODO: do we need this separate set of field names? we can just use the fieldinfos?
  private final Collection<String> fieldNames;
  private Collection<String> storedHighlightFieldNames;
  private DirectoryFactory directoryFactory;
  
  private final LeafReader leafReader;
  // only for addIndexes etc (no fieldcache)
  private final DirectoryReader rawReader;
  
  private String path;
  private final boolean reserveDirectory;
  private boolean createdDirectory; 
  
  private static DirectoryReader getReader(SolrCore core, SolrIndexConfig config, DirectoryFactory directoryFactory, String path) throws IOException {
    DirectoryReader reader = null;
    Directory dir = directoryFactory.get(path, DirContext.DEFAULT, config.lockType);
    try {
      reader = core.getIndexReaderFactory().newReader(dir, core);
    } catch (Exception e) {
      directoryFactory.release(dir);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error opening Reader", e);
    }
    return reader;
  }
  
  // TODO: wrap elsewhere and return a "map" from the schema that overrides get() ?
  // this reader supports reopen
  private static DirectoryReader wrapReader(SolrCore core, DirectoryReader reader) {
    assert reader != null;
    return UninvertingReader.wrap(reader, core.getLatestSchema().getUninversionMap(reader));
  }

  /**
   * Builds the neccessary collector chain (via delegate wrapping) and executes the query 
   * against it.  This method takes into consideration both the explicitly provided collector 
   * and postFilter as well as any needed collector wrappers for dealing with options 
   * specified in the QueryCOmmand.
   */
  private void buildAndRunCollectorChain(QueryResult qr, Query query, Filter luceneFilter,
      Collector collector, QueryCommand cmd, DelegatingCollector postFilter) throws IOException {
    
    final boolean terminateEarly = (cmd.getFlags() & TERMINATE_EARLY) == TERMINATE_EARLY;
    if (terminateEarly) {
      collector = new EarlyTerminatingCollector(collector, cmd.len);
    }

    final long timeAllowed = cmd.getTimeAllowed();
    if( timeAllowed > 0 ) {
      collector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), timeAllowed);
    }
    
    if (postFilter != null) {
      postFilter.setLastDelegate(collector);
      collector = postFilter;
    }
    
    try {
      super.search(query, luceneFilter, collector);
      if(collector instanceof DelegatingCollector) {
        ((DelegatingCollector)collector).finish();
      }
    }
    catch( TimeLimitingCollector.TimeExceededException x ) {
      log.warn( "Query: " + query + "; " + x.getMessage() );
      qr.setPartialResults(true);
    }        
  }
  
  public SolrIndexSearcher(SolrCore core, String path, IndexSchema schema, SolrIndexConfig config, String name, boolean enableCache, DirectoryFactory directoryFactory) throws IOException {
    // we don't need to reserve the directory because we get it from the factory
    this(core, path, schema, config, name, getReader(core, config, directoryFactory, path), true, enableCache, false, directoryFactory);
    this.createdDirectory = true;
  }

  public SolrIndexSearcher(SolrCore core, String path, IndexSchema schema, SolrIndexConfig config, String name, DirectoryReader r, boolean closeReader, boolean enableCache, boolean reserveDirectory, DirectoryFactory directoryFactory) throws IOException {
    super(wrapReader(core, r));

    this.path = path;
    this.directoryFactory = directoryFactory;
    this.reader = (DirectoryReader) super.readerContext.reader();
    this.rawReader = r;
    this.leafReader = SlowCompositeReaderWrapper.wrap(this.reader);
    this.core = core;
    this.schema = schema;
    this.name = "Searcher@" + Integer.toHexString(hashCode()) + "[" + core.getName() + "]" + (name != null ? " " + name : "");
    log.info("Opening " + this.name);

    if (directoryFactory.searchersReserveCommitPoints()) {
      // reserve commit point for life of searcher
      core.getDeletionPolicy().saveCommitPoint(
          reader.getIndexCommit().getGeneration());
    }
    
    Directory dir = getIndexReader().directory();
    
    this.reserveDirectory = reserveDirectory;
    if (reserveDirectory) {
      // keep the directory from being released while we use it
      directoryFactory.incRef(dir);
    }

    this.closeReader = closeReader;
    setSimilarity(schema.getSimilarity());

    SolrConfig solrConfig = core.getSolrConfig();
    queryResultWindowSize = solrConfig.queryResultWindowSize;
    queryResultMaxDocsCached = solrConfig.queryResultMaxDocsCached;
    useFilterForSortedQuery = solrConfig.useFilterForSortedQuery;
    enableLazyFieldLoading = solrConfig.enableLazyFieldLoading;
    
    cachingEnabled=enableCache;
    if (cachingEnabled) {
      ArrayList<SolrCache> clist = new ArrayList<>();
      fieldValueCache = solrConfig.fieldValueCacheConfig==null ? null : solrConfig.fieldValueCacheConfig.newInstance();
      if (fieldValueCache!=null) clist.add(fieldValueCache);
      filterCache= solrConfig.filterCacheConfig==null ? null : solrConfig.filterCacheConfig.newInstance();
      if (filterCache!=null) clist.add(filterCache);
      queryResultCache = solrConfig.queryResultCacheConfig==null ? null : solrConfig.queryResultCacheConfig.newInstance();
      if (queryResultCache!=null) clist.add(queryResultCache);
      documentCache = solrConfig.documentCacheConfig==null ? null : solrConfig.documentCacheConfig.newInstance();
      if (documentCache!=null) clist.add(documentCache);

      if (solrConfig.userCacheConfigs == null) {
        cacheMap = noGenericCaches;
      } else {
        cacheMap = new HashMap<>(solrConfig.userCacheConfigs.length);
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
      fieldValueCache=null;
      cacheMap = noGenericCaches;
      cacheList= noCaches;
    }
    
    // TODO: This option has been dead/noop since 3.1, should we re-enable it?
//    optimizer = solrConfig.filtOptEnabled ? new LuceneQueryOptimizer(solrConfig.filtOptCacheSize,solrConfig.filtOptThreshold) : null;
    optimizer = null;
    
    fieldNames = new HashSet<>();
    fieldInfos = leafReader.getFieldInfos();
    for(FieldInfo fieldInfo : fieldInfos) {
      fieldNames.add(fieldInfo.name);
    }

    // do this at the end since an exception in the constructor means we won't close    
    numOpens.incrementAndGet();
  }

  public boolean isCachingEnabled() { return cachingEnabled; }

  public String getPath() {
    return path;
  }

  @Override
  public String toString() {
    return name + "{" + reader + "}";
  }

  public SolrCore getCore() {
    return core;
  }

  public final int maxDoc() {
    return reader.maxDoc();
  }
  
  public final int docFreq(Term term) throws IOException {
    return reader.docFreq(term);
  }
  
  public final LeafReader getLeafReader() {
    return leafReader;
  }
  
  /** Raw reader (no fieldcaches etc). Useful for operations like addIndexes */
  public final DirectoryReader getRawReader() {
    return rawReader;
  }
  
  @Override
  public final DirectoryReader getIndexReader() {
    assert reader == super.getIndexReader();
    return reader; 
  }

  /** Register sub-objects such as caches
   */
  public void register() {
    // register self
    core.getInfoRegistry().put("searcher", this);
    core.getInfoRegistry().put(name, this);
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
  @Override
  public void close() throws IOException {
    if (debug) {
      if (cachingEnabled) {
        StringBuilder sb = new StringBuilder();
        sb.append("Closing ").append(name);
        for (SolrCache cache : cacheList) {
          sb.append("\n\t");
          sb.append(cache);
        }
        log.debug(sb.toString());
      } else {
        if (debug) log.debug("Closing " + name);
      }
    }

    core.getInfoRegistry().remove(name);

    // super.close();
    // can't use super.close() since it just calls reader.close() and that may only be called once
    // per reader (even if incRef() was previously called).
    
    long cpg = reader.getIndexCommit().getGeneration();
    try {
      if (closeReader) rawReader.decRef();
    } catch (Exception e) {
      SolrException.log(log, "Problem dec ref'ing reader", e);
    }

    if (directoryFactory.searchersReserveCommitPoints()) {
      core.getDeletionPolicy().releaseCommitPoint(cpg);
    }

    for (SolrCache cache : cacheList) {
      cache.close();
    }

    if (reserveDirectory) {
      directoryFactory.release(getIndexReader().directory());
    }
    if (createdDirectory) {
      directoryFactory.release(getIndexReader().directory());
    }
   
    
    // do this at the end so it only gets done if there are no exceptions
    numCloses.incrementAndGet();
  }

  /** Direct access to the IndexSchema for use with this searcher */
  public IndexSchema getSchema() { return schema; }
  
  /**
   * Returns a collection of all field names the index reader knows about.
   */
  public Collection<String> getFieldNames() {
    return fieldNames;
  }

  /**
   * Returns a collection of the names of all stored fields which can be
   * highlighted the index reader knows about.
   */
  public Collection<String> getStoredHighlightFieldNames() {
    synchronized (this) {
      if (storedHighlightFieldNames == null) {
        storedHighlightFieldNames = new LinkedList<>();
        for (String fieldName : fieldNames) {
          try {
            SchemaField field = schema.getField(fieldName);
            if (field.stored() &&
                ((field.getType() instanceof org.apache.solr.schema.TextField) ||
                    (field.getType() instanceof org.apache.solr.schema.StrField))) {
              storedHighlightFieldNames.add(fieldName);
            }
          } catch (RuntimeException e) { // getField() throws a SolrException, but it arrives as a RuntimeException
            log.warn("Field \"" + fieldName + "\" found in index, but not defined in schema.");
          }
        }
      }
      return storedHighlightFieldNames;
    }
  }
  //
  // Set default regenerators on filter and query caches if they don't have any
  //
  public static void initRegenerators(SolrConfig solrConfig) {
    if (solrConfig.fieldValueCacheConfig != null && solrConfig.fieldValueCacheConfig.getRegenerator() == null) {
      solrConfig.fieldValueCacheConfig.setRegenerator(
              new CacheRegenerator() {
                @Override
                public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache newCache, SolrCache oldCache, Object oldKey, Object oldVal) throws IOException {
                  if (oldVal instanceof UnInvertedField) {
                    UnInvertedField.getUnInvertedField((String)oldKey, newSearcher);
                  }
                  return true;
                }
              }
      );
    }

    if (solrConfig.filterCacheConfig != null && solrConfig.filterCacheConfig.getRegenerator() == null) {
      solrConfig.filterCacheConfig.setRegenerator(
              new CacheRegenerator() {
                @Override
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
                @Override
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

                  int flags=NO_CHECK_QCACHE | key.nc_flags;
                  QueryCommand qc = new QueryCommand();
                  qc.setQuery(key.query)
                    .setFilterList(key.filters)
                    .setSort(key.sort)
                    .setLen(nDocs)
                    .setSupersetMaxDoc(nDocs)
                    .setFlags(flags);
                  QueryResult qr = new QueryResult();
                  newSearcher.getDocListC(qr,qc);
                  return true;
                }
              }
      );
    }
  }

  public QueryResult search(QueryResult qr, QueryCommand cmd) throws IOException {
    getDocListC(qr,cmd);
    return qr;
  }

//  FIXME: This option has been dead/noop since 3.1, should we re-enable or remove it?
//  public Hits search(Query query, Filter filter, Sort sort) throws IOException {
//    // todo - when Solr starts accepting filters, need to
//    // change this conditional check (filter!=null) and create a new filter
//    // that ANDs them together if it already exists.
//
//    if (optimizer==null || filter!=null || !(query instanceof BooleanQuery)
//    ) {
//      return super.search(query,filter,sort);
//    } else {
//      Query[] newQuery = new Query[1];
//      Filter[] newFilter = new Filter[1];
//      optimizer.optimize((BooleanQuery)query, this, 0, newQuery, newFilter);
//
//      return super.search(newQuery[0], newFilter[0], sort);
//    }
//  }


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
  // TODO: can we just subclass DocumentStoredFieldVisitor?
  // need to open up access to its Document...
  static class SetNonLazyFieldSelector extends StoredFieldVisitor {
    private Set<String> fieldsToLoad;
    final StoredDocument doc = new StoredDocument();
    final LazyDocument lazyDoc;

    SetNonLazyFieldSelector(Set<String> toLoad, IndexReader reader, int docID) {
      fieldsToLoad = toLoad;
      lazyDoc = new LazyDocument(reader, docID);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
      if (fieldsToLoad.contains(fieldInfo.name)) {
        return Status.YES;
      } else {
        doc.add(lazyDoc.getField(fieldInfo));
        return Status.NO;
      }
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
      doc.add(new StoredField(fieldInfo.name, value));
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      final FieldType ft = new FieldType(TextField.TYPE_STORED);
      ft.setStoreTermVectors(fieldInfo.hasVectors());
      ft.setIndexed(fieldInfo.isIndexed());
      ft.setOmitNorms(fieldInfo.omitsNorms());
      ft.setIndexOptions(fieldInfo.getIndexOptions());
      doc.add(new Field(fieldInfo.name, value, ft));
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) {
      FieldType ft = new FieldType(IntField.TYPE_NOT_STORED);
      ft.setStored(true);
      ft.setIndexed(fieldInfo.isIndexed());
      doc.add(new IntField(fieldInfo.name, value, ft));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) {
      FieldType ft = new FieldType(LongField.TYPE_NOT_STORED);
      ft.setStored(true);
      ft.setIndexed(fieldInfo.isIndexed());
      doc.add(new LongField(fieldInfo.name, value, ft));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) {
      FieldType ft = new FieldType(FloatField.TYPE_NOT_STORED);
      ft.setStored(true);
      ft.setIndexed(fieldInfo.isIndexed());
      doc.add(new FloatField(fieldInfo.name, value, ft));
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) {
      FieldType ft = new FieldType(DoubleField.TYPE_NOT_STORED);
      ft.setStored(true);
      ft.setIndexed(fieldInfo.isIndexed());
      doc.add(new DoubleField(fieldInfo.name, value, ft));
    }
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   */
  @Override
  public StoredDocument doc(int i) throws IOException {
    return doc(i, (Set<String>)null);
  }

  /** Visit a document's fields using a {@link StoredFieldVisitor}
   *  This method does not currently add to the Solr document cache.
   * 
   * @see IndexReader#document(int, StoredFieldVisitor) */
  @Override
  public void doc(int n, StoredFieldVisitor visitor) throws IOException {
    if (documentCache != null) {
      StoredDocument cached = documentCache.get(n);
      if (cached != null) {
        visitFromCached(cached, visitor);
        return;
      }
    }
    getIndexReader().document(n, visitor);
  }
  
  /** Executes a stored field visitor against a hit from the document cache */
  private void visitFromCached(StoredDocument document, StoredFieldVisitor visitor) throws IOException {
    for (StorableField f : document) {
      FieldInfo info = fieldInfos.fieldInfo(f.name());
      switch(visitor.needsField(info)) {
        case YES:
          if (f.binaryValue() != null) {
            BytesRef binaryValue = f.binaryValue();
            byte copy[] = new byte[binaryValue.length];
            System.arraycopy(binaryValue.bytes, binaryValue.offset, copy, 0, copy.length);
            visitor.binaryField(info, copy);
          } else if (f.numericValue() != null) {
            Number numericValue = f.numericValue();
            if (numericValue instanceof Double) {
              visitor.doubleField(info, numericValue.doubleValue());
            } else if (numericValue instanceof Integer) {
              visitor.intField(info, numericValue.intValue());
            } else if (numericValue instanceof Float) {
              visitor.floatField(info, numericValue.floatValue());
            } else if (numericValue instanceof Long) {
              visitor.longField(info, numericValue.longValue());
            } else {
              throw new AssertionError();
            }
          } else {
            visitor.stringField(info, f.stringValue());
          }
          break;
        case NO:
          break;
        case STOP:
          return;
      }
    }
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   * <p>
   * Note: The document will have all fields accessible, but if a field
   * filter is provided, only the provided fields will be loaded (the 
   * remainder will be available lazily).
   */
  @Override
  public StoredDocument doc(int i, Set<String> fields) throws IOException {
    
    StoredDocument d;
    if (documentCache != null) {
      d = documentCache.get(i);
      if (d!=null) return d;
    }

    if(!enableLazyFieldLoading || fields == null) {
      d = getIndexReader().document(i);
    } else {
      final SetNonLazyFieldSelector visitor = new SetNonLazyFieldSelector(fields, getIndexReader(), i);
      getIndexReader().document(i, visitor);
      d = visitor.doc;
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
  public void readDocs(StoredDocument[] docs, DocList ids) throws IOException {
    readDocs(docs, ids, null);
  }
  /**
   * Takes a list of docs (the doc ids actually) and a set of fields to load,
   * and reads them into an array of Documents.
   */
  public void readDocs(StoredDocument[] docs, DocList ids, Set<String> fields) throws IOException {
    DocIterator iter = ids.iterator();
    for (int i=0; i<docs.length; i++) {
      docs[i] = doc(iter.nextDoc(), fields);
    }
  }

  /* ********************** end document retrieval *************************/

  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  /** expert: internal API, subject to change */
  public SolrCache<String,UnInvertedField> getFieldValueCache() {
    return fieldValueCache;
  }

  /** Returns a weighted sort according to this searcher */
  public Sort weightSort(Sort sort) throws IOException {
    return (sort != null) ? sort.rewrite(this) : null;
  }


  /**
   * Returns the first document number containing the term <code>t</code>
   * Returns -1 if no document was found.
   * This method is primarily intended for clients that want to fetch
   * documents using a unique identifier."
   * @return the first document number containing the term
   */
  public int getFirstMatch(Term t) throws IOException {
    Fields fields = leafReader.fields();
    if (fields == null) return -1;
    Terms terms = fields.terms(t.field());
    if (terms == null) return -1;
    BytesRef termBytes = t.bytes();
    final TermsEnum termsEnum = terms.iterator(null);
    if (!termsEnum.seekExact(termBytes)) {
      return -1;
    }
    DocsEnum docs = termsEnum.docs(leafReader.getLiveDocs(), null, DocsEnum.FLAG_NONE);
    if (docs == null) return -1;
    int id = docs.nextDoc();
    return id == DocIdSetIterator.NO_MORE_DOCS ? -1 : id;
  }

  /** lookup the docid by the unique key field, and return the id *within* the leaf reader in the low 32 bits, and the index of the leaf reader in the high 32 bits.
   * -1 is returned if not found.
   * @lucene.internal
   */
  public long lookupId(BytesRef idBytes) throws IOException {
    String field = schema.getUniqueKeyField().getName();

    for (int i=0, c=leafContexts.size(); i<c; i++) {
      final LeafReaderContext leaf = leafContexts.get(i);
      final LeafReader reader = leaf.reader();

      final Terms terms = reader.terms(field);
      if (terms == null) continue;
      
      TermsEnum te = terms.iterator(null);
      if (te.seekExact(idBytes)) {
        DocsEnum docs = te.docs(reader.getLiveDocs(), null, DocsEnum.FLAG_NONE);
        int id = docs.nextDoc();
        if (id == DocIdSetIterator.NO_MORE_DOCS) continue;
        assert docs.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;

        return (((long)i) << 32) | id;
      }
    }

    return -1;
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
    if (query instanceof ExtendedQuery) {
      ExtendedQuery eq = (ExtendedQuery)query;
      if (!eq.getCache()) {
        if (query instanceof WrappedQuery) {
          query = ((WrappedQuery)query).getWrappedQuery();
        }
        query = QueryUtils.makeQueryable(query);
        return getDocSetNC(query, null);
      }
    }

    // Get the absolute value (positive version) of this query.  If we
    // get back the same reference, we know it's positive.
    Query absQ = QueryUtils.getAbs(query);
    boolean positive = query==absQ;

    if (filterCache != null) {
      DocSet absAnswer = filterCache.get(absQ);
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
      answer = filterCache.get(q);
      if (answer!=null) return answer;
    }
    answer = getDocSetNC(q,null);
    if (filterCache != null) filterCache.put(
        q,answer);
    return answer;
  }

  private static Query matchAllDocsQuery = new MatchAllDocsQuery();


  public static class ProcessedFilter {
    public DocSet answer;  // the answer, if non-null
    public Filter filter;
    public DelegatingCollector postFilter;
  }


  private static Comparator<Query> sortByCost = new Comparator<Query>() {
    @Override
    public int compare(Query q1, Query q2) {
      return ((ExtendedQuery)q1).getCost() - ((ExtendedQuery)q2).getCost();
    }
  };

  private DocSet getDocSetScore(List<Query> queries) throws IOException {
    Query main = queries.remove(0);
    ProcessedFilter pf = getProcessedFilter(null, queries);
    DocSetCollector setCollector = new DocSetCollector(maxDoc()>>6, maxDoc());
    Collector collector = setCollector;
    if (pf.postFilter != null) {
      pf.postFilter.setLastDelegate(collector);
      collector = pf.postFilter;
    }

    search(main, pf.filter, collector);

    if(collector instanceof DelegatingCollector) {
      ((DelegatingCollector) collector).finish();
    }

    DocSet docSet = setCollector.getDocSet();
    return docSet;
  }

  /**
   * Returns the set of document ids matching all queries.
   * This method is cache-aware and attempts to retrieve the answer from the cache if possible.
   * If the answer was not cached, it may have been inserted into the cache as a result of this call.
   * This method can handle negative queries.
   * <p>
   * The DocSet returned should <b>not</b> be modified.
   */
  public DocSet getDocSet(List<Query> queries) throws IOException {

    if(queries != null) {
      for(Query q : queries) {
        if(q instanceof ScoreFilter) {
          return getDocSetScore(queries);
        }
      }
    }

    ProcessedFilter pf = getProcessedFilter(null, queries);
    if (pf.answer != null) return pf.answer;


    DocSetCollector setCollector = new DocSetCollector(maxDoc()>>6, maxDoc());
    Collector collector = setCollector;
    if (pf.postFilter != null) {
      pf.postFilter.setLastDelegate(collector);
      collector = pf.postFilter;
    }

    for (final LeafReaderContext leaf : leafContexts) {
      final LeafReader reader = leaf.reader();
      final Bits liveDocs = reader.getLiveDocs();   // TODO: the filter may already only have liveDocs...
      DocIdSet idSet = null;
      if (pf.filter != null) {
        idSet = pf.filter.getDocIdSet(leaf, liveDocs);
        if (idSet == null) continue;
      }
      DocIdSetIterator idIter = null;
      if (idSet != null) {
        idIter = idSet.iterator();
        if (idIter == null) continue;
      }

      final LeafCollector leafCollector = collector.getLeafCollector(leaf);
      int max = reader.maxDoc();

      if (idIter == null) {
        for (int docid = 0; docid<max; docid++) {
          if (liveDocs != null && !liveDocs.get(docid)) continue;
          leafCollector.collect(docid);
        }
      } else {
        for (int docid = -1; (docid = idIter.advance(docid+1)) < max; ) {
          leafCollector.collect(docid);
        }
      }
    }

    if(collector instanceof DelegatingCollector) {
      ((DelegatingCollector) collector).finish();
    }

    return setCollector.getDocSet();
  }


  public ProcessedFilter getProcessedFilter(DocSet setFilter, List<Query> queries) throws IOException {
    ProcessedFilter pf = new ProcessedFilter();
    if (queries==null || queries.size()==0) {
      if (setFilter != null)
        pf.filter = setFilter.getTopFilter();
      return pf;
    }

    DocSet answer=null;

    boolean[] neg = new boolean[queries.size()+1];
    DocSet[] sets = new DocSet[queries.size()+1];
    List<Query> notCached = null;
    List<Query> postFilters = null;

    int end = 0;
    int smallestIndex = -1;

    if (setFilter != null) {
      answer = sets[end++] = setFilter;
      smallestIndex = end;
    }

    int smallestCount = Integer.MAX_VALUE;
    for (Query q : queries) {
      if (q instanceof ExtendedQuery) {
        ExtendedQuery eq = (ExtendedQuery)q;
        if (!eq.getCache()) {
          if (eq.getCost() >= 100 && eq instanceof PostFilter) {
            if (postFilters == null) postFilters = new ArrayList<>(sets.length-end);
            postFilters.add(q);
          } else {
            if (notCached == null) notCached = new ArrayList<>(sets.length-end);
            notCached.add(q);
          }
          continue;
        }
      } 
      
      if (filterCache == null) {
        // there is no cache: don't pull bitsets
        if (notCached == null) notCached = new ArrayList<>(sets.length-end);
        WrappedQuery uncached = new WrappedQuery(q);
        uncached.setCache(false);
        notCached.add(uncached);
        continue;
      }

      Query posQuery = QueryUtils.getAbs(q);
      sets[end] = getPositiveDocSet(posQuery);
      // Negative query if absolute value different from original
      if (q==posQuery) {
        neg[end] = false;
        // keep track of the smallest positive set.
        // This optimization is only worth it if size() is cached, which it would
        // be if we don't do any set operations.
        int sz = sets[end].size();
        if (sz<smallestCount) {
          smallestCount=sz;
          smallestIndex=end;
          answer = sets[end];
        }
      } else {
        neg[end] = true;
      }

      end++;
    }

    // Are all of our normal cached filters negative?
    if (end > 0 && answer==null) {
      answer = getPositiveDocSet(matchAllDocsQuery);
    }

    // do negative queries first to shrink set size
    for (int i=0; i<end; i++) {
      if (neg[i]) answer = answer.andNot(sets[i]);
    }

    for (int i=0; i<end; i++) {
      if (!neg[i] && i!=smallestIndex) answer = answer.intersection(sets[i]);
    }

    if (notCached != null) {
      Collections.sort(notCached, sortByCost);
      List<Weight> weights = new ArrayList<>(notCached.size());
      for (Query q : notCached) {
        Query qq = QueryUtils.makeQueryable(q);
        weights.add(createNormalizedWeight(qq));
      }
      pf.filter = new FilterImpl(answer, weights);
    } else {
      if (postFilters == null) {
        if (answer == null) {
          answer = getPositiveDocSet(matchAllDocsQuery);
        }
        // "answer" is the only part of the filter, so set it.
        pf.answer = answer;
      }

      if (answer != null) {
        pf.filter = answer.getTopFilter();
      }
    }

    if (postFilters != null) {
      Collections.sort(postFilters, sortByCost);
      for (int i=postFilters.size()-1; i>=0; i--) {
        DelegatingCollector prev = pf.postFilter;
        pf.postFilter = ((PostFilter)postFilters.get(i)).getFilterCollector(this);
        if (prev != null) pf.postFilter.setDelegate(prev);
      }
    }

    return pf;
  }

  /** lucene.internal */
  public DocSet getDocSet(DocsEnumState deState) throws IOException {
    int largestPossible = deState.termsEnum.docFreq();
    boolean useCache = filterCache != null && largestPossible >= deState.minSetSizeCached;
    TermQuery key = null;

    if (useCache) {
      key = new TermQuery(new Term(deState.fieldName, BytesRef.deepCopyOf(deState.termsEnum.term())));
      DocSet result = filterCache.get(key);
      if (result != null) return result;
    }

    int smallSetSize = maxDoc()>>6;
    int scratchSize = Math.min(smallSetSize, largestPossible);
    if (deState.scratch == null || deState.scratch.length < scratchSize)
      deState.scratch = new int[scratchSize];

    final int[] docs = deState.scratch;
    int upto = 0;
    int bitsSet = 0;
    FixedBitSet fbs = null;

    DocsEnum docsEnum = deState.termsEnum.docs(deState.liveDocs, deState.docsEnum, DocsEnum.FLAG_NONE);
    if (deState.docsEnum == null) {
      deState.docsEnum = docsEnum;
    }

    if (docsEnum instanceof MultiDocsEnum) {
      MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
      int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
      for (int subindex = 0; subindex<numSubs; subindex++) {
        MultiDocsEnum.EnumWithSlice sub = subs[subindex];
        if (sub.docsEnum == null) continue;
        int base = sub.slice.start;
        int docid;
        
        if (largestPossible > docs.length) {
          if (fbs == null) fbs = new FixedBitSet(maxDoc());
          while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            fbs.set(docid + base);
            bitsSet++;
          }
        } else {
          while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            docs[upto++] = docid + base;
          }
        }
      }
    } else {
      int docid;
      if (largestPossible > docs.length) {
        if (fbs == null) fbs = new FixedBitSet(maxDoc());
        while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          fbs.set(docid);
          bitsSet++;
        }
      } else {
        while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          docs[upto++] = docid;
        }
      }
    }

    DocSet result;
    if (fbs != null) {
      for (int i=0; i<upto; i++) {
        fbs.set(docs[i]);  
      }
      bitsSet += upto;
      result = new BitDocSet(fbs, bitsSet);
    } else {
      result = upto==0 ? DocSet.EMPTY : new SortedIntDocSet(Arrays.copyOf(docs, upto));
    }

    if (useCache) {
      filterCache.put(key, result);
    }
    
    return result;
  }

  // query must be positive
  protected DocSet getDocSetNC(Query query, DocSet filter) throws IOException {
    DocSetCollector collector = new DocSetCollector(maxDoc()>>6, maxDoc());

    if (filter==null) {
      super.search(query,null,collector);
    } else {
      Filter luceneFilter = filter.getTopFilter();
      super.search(query, luceneFilter, collector);
    }
    return collector.getDocSet();
  }


  /**
   * Returns the set of document ids matching both the query and the filter.
   * This method is cache-aware and attempts to retrieve the answer from the cache if possible.
   * If the answer was not cached, it may have been inserted into the cache as a result of this call.
   * <p>
   *
   * @param filter may be null
   * @return DocSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   */
  public DocSet getDocSet(Query query, DocSet filter) throws IOException {
    if (filter==null) return getDocSet(query);

    if (query instanceof ExtendedQuery) {
      ExtendedQuery eq = (ExtendedQuery)query;
      if (!eq.getCache()) {
        if (query instanceof WrappedQuery) {
          query = ((WrappedQuery)query).getWrappedQuery();
        }
        query = QueryUtils.makeQueryable(query);
        return getDocSetNC(query, filter);
      }
    }

    // Negative query if absolute value different from original
    Query absQ = QueryUtils.getAbs(query);
    boolean positive = absQ==query;

    DocSet first;
    if (filterCache != null) {
      first = filterCache.get(absQ);
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
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocList getDocList(Query query, Query filter, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilterList(filter)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocList();
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
   * @param filterList may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocList getDocList(Query query, List<Query> filterList, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilterList(filterList)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len)
      .setFlags(flags);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocList();
  }

  static final int NO_CHECK_QCACHE       = 0x80000000;
  public static final int GET_DOCSET            = 0x40000000;
  static final int NO_CHECK_FILTERCACHE  = 0x20000000;
  static final int NO_SET_QCACHE         = 0x10000000;
  public static final int TERMINATE_EARLY = 0x04;
  public static final int GET_DOCLIST           =        0x02; // get the documents actually returned in a response
  public static final int GET_SCORES             =       0x01;


  /**
   * getDocList version that uses+populates query and filter caches.
   * In the event of a timeout, the cache is not populated.
   */
  private void getDocListC(QueryResult qr, QueryCommand cmd) throws IOException {
    DocListAndSet out = new DocListAndSet();
    qr.setDocListAndSet(out);
    QueryResultKey key=null;
    int maxDocRequested = cmd.getOffset() + cmd.getLen();
    // check for overflow, and check for # docs in index
    if (maxDocRequested < 0 || maxDocRequested > maxDoc()) maxDocRequested = maxDoc();
    int supersetMaxDoc= maxDocRequested;
    DocList superset = null;

    int flags = cmd.getFlags();
    Query q = cmd.getQuery();
    if (q instanceof ExtendedQuery) {
      ExtendedQuery eq = (ExtendedQuery)q;
      if (!eq.getCache()) {
        flags |= (NO_CHECK_QCACHE | NO_SET_QCACHE | NO_CHECK_FILTERCACHE);
      }
    }


    // we can try and look up the complete query in the cache.
    // we can't do that if filter!=null though (we don't want to
    // do hashCode() and equals() for a big DocSet).
    if (queryResultCache != null && cmd.getFilter()==null
        && (flags & (NO_CHECK_QCACHE|NO_SET_QCACHE)) != ((NO_CHECK_QCACHE|NO_SET_QCACHE)))
    {
        // all of the current flags can be reused during warming,
        // so set all of them on the cache key.
        key = new QueryResultKey(q, cmd.getFilterList(), cmd.getSort(), flags);
        if ((flags & NO_CHECK_QCACHE)==0) {
          superset = queryResultCache.get(key);

          if (superset != null) {
            // check that the cache entry has scores recorded if we need them
            if ((flags & GET_SCORES)==0 || superset.hasScores()) {
              // NOTE: subset() returns null if the DocList has fewer docs than
              // requested
              out.docList = superset.subset(cmd.getOffset(),cmd.getLen());
            }
          }
          if (out.docList != null) {
            // found the docList in the cache... now check if we need the docset too.
            // OPT: possible future optimization - if the doclist contains all the matches,
            // use it to make the docset instead of rerunning the query.
            if (out.docSet==null && ((flags & GET_DOCSET)!=0) ) {
              if (cmd.getFilterList()==null) {
                out.docSet = getDocSet(cmd.getQuery());
              } else {
                List<Query> newList = new ArrayList<>(cmd.getFilterList().size()+1);
                newList.add(cmd.getQuery());
                newList.addAll(cmd.getFilterList());
                out.docSet = getDocSet(newList);
              }
            }
            return;
          }
        }

      // If we are going to generate the result, bump up to the
      // next resultWindowSize for better caching.

      if ((flags & NO_SET_QCACHE) == 0) {
        // handle 0 special case as well as avoid idiv in the common case.
        if (maxDocRequested < queryResultWindowSize) {
          supersetMaxDoc=queryResultWindowSize;
        } else {
          supersetMaxDoc = ((maxDocRequested -1)/queryResultWindowSize + 1)*queryResultWindowSize;
          if (supersetMaxDoc < 0) supersetMaxDoc=maxDocRequested;
        }
      } else {
        key = null;  // we won't be caching the result
      }
    }
    cmd.setSupersetMaxDoc(supersetMaxDoc);


    // OK, so now we need to generate an answer.
    // One way to do that would be to check if we have an unordered list
    // of results for the base query.  If so, we can apply the filters and then
    // sort by the resulting set.  This can only be used if:
    // - the sort doesn't contain score
    // - we don't want score returned.

    // check if we should try and use the filter cache
    boolean useFilterCache=false;
    if ((flags & (GET_SCORES|NO_CHECK_FILTERCACHE))==0 && useFilterForSortedQuery && cmd.getSort() != null && filterCache != null) {
      useFilterCache=true;
      SortField[] sfields = cmd.getSort().getSort();
      for (SortField sf : sfields) {
        if (sf.getType() == SortField.Type.SCORE) {
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
        out.docSet = getDocSet(cmd.getQuery(),cmd.getFilter());
        DocSet bigFilt = getDocSet(cmd.getFilterList());
        if (bigFilt != null) out.docSet = out.docSet.intersection(bigFilt);
      }
      // todo: there could be a sortDocSet that could take a list of
      // the filters instead of anding them first...
      // perhaps there should be a multi-docset-iterator
      sortDocSet(qr, cmd);
    } else {
      // do it the normal way...
      if ((flags & GET_DOCSET)!=0) {
        // this currently conflates returning the docset for the base query vs
        // the base query and all filters.
        DocSet qDocSet = getDocListAndSetNC(qr,cmd);
        // cache the docSet matching the query w/o filtering
        if (qDocSet!=null && filterCache!=null && !qr.isPartialResults()) filterCache.put(cmd.getQuery(),qDocSet);
      } else {
        getDocListNC(qr,cmd);
      }
      assert null != out.docList : "docList is null";
    }

    if (null == cmd.getCursorMark()) {
      // Kludge...
      // we can't use DocSlice.subset, even though it should be an identity op
      // because it gets confused by situations where there are lots of matches, but
      // less docs in the slice then were requested, (due to the cursor)
      // so we have to short circuit the call.
      // None of which is really a problem since we can't use caching with
      // cursors anyway, but it still looks weird to have to special case this
      // behavior based on this condition - hence the long explanation.
      superset = out.docList;
      out.docList = superset.subset(cmd.getOffset(),cmd.getLen());
    } else {
      // sanity check our cursor assumptions
      assert null == superset : "cursor: superset isn't null";
      assert 0 == cmd.getOffset() : "cursor: command offset mismatch";
      assert 0 == out.docList.offset() : "cursor: docList offset mismatch";
      assert cmd.getLen() >= supersetMaxDoc : "cursor: superset len mismatch: " +
        cmd.getLen() + " vs " + supersetMaxDoc;
    }

    // lastly, put the superset in the cache if the size is less than or equal
    // to queryResultMaxDocsCached
    if (key != null && superset.size() <= queryResultMaxDocsCached && !qr.isPartialResults()) {
      queryResultCache.put(key, superset);
    }
  }

  /**
   * Helper method for extracting the {@link FieldDoc} sort values from a 
   * {@link TopFieldDocs} when available and making the appropriate call to 
   * {@link QueryResult#setNextCursorMark} when applicable.
   *
   * @param qr <code>QueryResult</code> to modify
   * @param qc <code>QueryCommand</code> for context of method
   * @param topDocs May or may not be a <code>TopFieldDocs</code> 
   */
  private void populateNextCursorMarkFromTopDocs(QueryResult qr, QueryCommand qc, 
                                                 TopDocs topDocs) {
    // TODO: would be nice to rename & generalize this method for non-cursor cases...
    // ...would be handy to reuse the ScoreDoc/FieldDoc sort vals directly in distrib sort
    // ...but that has non-trivial queryResultCache implications
    // See: SOLR-5595
    
    if (null == qc.getCursorMark()) {
      // nothing to do, short circuit out
      return;
    }

    final CursorMark lastCursorMark = qc.getCursorMark();
    
    // if we have a cursor, then we have a sort that at minimum involves uniqueKey..
    // so we must have a TopFieldDocs containing FieldDoc[]
    assert topDocs instanceof TopFieldDocs : "TopFieldDocs cursor constraint violated";
    final TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;
    final ScoreDoc[] scoreDocs = topFieldDocs.scoreDocs;

    if (0 == scoreDocs.length) {
      // no docs on this page, re-use existing cursor mark
      qr.setNextCursorMark(lastCursorMark);
    } else {
      ScoreDoc lastDoc = scoreDocs[scoreDocs.length-1];
      assert lastDoc instanceof FieldDoc : "FieldDoc cursor constraint violated";
      
      List<Object> lastFields = Arrays.<Object>asList(((FieldDoc)lastDoc).fields);
      CursorMark nextCursorMark = lastCursorMark.createNext(lastFields);
      assert null != nextCursorMark : "null nextCursorMark";
      qr.setNextCursorMark(nextCursorMark);
    }
  }

  /**
   * Helper method for inspecting QueryCommand and creating the appropriate 
   * {@link TopDocsCollector}
   *
   * @param len the number of docs to return
   * @param cmd The Command whose properties should determine the type of 
   *        TopDocsCollector to use.
   */
  private TopDocsCollector buildTopDocsCollector(int len, QueryCommand cmd) throws IOException {

    Query q = cmd.getQuery();
    if(q instanceof RankQuery) {
      RankQuery rq = (RankQuery)q;
      return rq.getTopDocsCollector(len, cmd, this);
    }

    if (null == cmd.getSort()) {
      assert null == cmd.getCursorMark() : "have cursor but no sort";
      return TopScoreDocCollector.create(len, true);
    } else {
      // we have a sort
      final boolean needScores = (cmd.getFlags() & GET_SCORES) != 0;
      final Sort weightedSort = weightSort(cmd.getSort());
      final CursorMark cursor = cmd.getCursorMark();

      // :TODO: make fillFields it's own QueryCommand flag? ...
      // ... see comments in populateNextCursorMarkFromTopDocs for cache issues (SOLR-5595)
      final boolean fillFields = (null != cursor);
      final FieldDoc searchAfter = (null != cursor ? cursor.getSearchAfterFieldDoc() : null);
      return TopFieldCollector.create(weightedSort, len, searchAfter,
                                      fillFields, needScores, needScores, true); 
    }
  }

  private void getDocListNC(QueryResult qr,QueryCommand cmd) throws IOException {
    int len = cmd.getSupersetMaxDoc();
    int last = len;
    if (last < 0 || last > maxDoc()) last=maxDoc();
    final int lastDocRequested = last;
    int nDocsReturned;
    int totalHits;
    float maxScore;
    int[] ids;
    float[] scores;

    boolean needScores = (cmd.getFlags() & GET_SCORES) != 0;
    
    Query query = QueryUtils.makeQueryable(cmd.getQuery());

    ProcessedFilter pf = getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
    final Filter luceneFilter = pf.filter;

    // handle zero case...
    if (lastDocRequested<=0) {
      final float[] topscore = new float[] { Float.NEGATIVE_INFINITY };
      final int[] numHits = new int[1];

      Collector collector;

      if (!needScores) {
        collector = new SimpleCollector () {
          @Override
          public void collect(int doc) {
            numHits[0]++;
          }
          @Override
          public boolean acceptsDocsOutOfOrder() {
            return true;
          }
        };
      } else {
        collector = new SimpleCollector() {
          Scorer scorer;
          @Override
          public void setScorer(Scorer scorer) {
            this.scorer = scorer;
          }
          @Override
          public void collect(int doc) throws IOException {
            numHits[0]++;
            float score = scorer.score();
            if (score > topscore[0]) topscore[0]=score;            
          }
          @Override
          public boolean acceptsDocsOutOfOrder() {
            return true;
          }
        };
      }
      
      buildAndRunCollectorChain(qr, query, luceneFilter, collector, cmd, pf.postFilter);

      nDocsReturned=0;
      ids = new int[nDocsReturned];
      scores = new float[nDocsReturned];
      totalHits = numHits[0];
      maxScore = totalHits>0 ? topscore[0] : 0.0f;
      // no docs on this page, so cursor doesn't change
      qr.setNextCursorMark(cmd.getCursorMark());
    } else {
      final TopDocsCollector topCollector = buildTopDocsCollector(len, cmd);
      Collector collector = topCollector;
      buildAndRunCollectorChain(qr, query, luceneFilter, collector, cmd, pf.postFilter);

      totalHits = topCollector.getTotalHits();
      TopDocs topDocs = topCollector.topDocs(0, len);
      populateNextCursorMarkFromTopDocs(qr, cmd, topDocs);

      maxScore = totalHits>0 ? topDocs.getMaxScore() : 0.0f;
      nDocsReturned = topDocs.scoreDocs.length;
      ids = new int[nDocsReturned];
      scores = (cmd.getFlags()&GET_SCORES)!=0 ? new float[nDocsReturned] : null;
      for (int i=0; i<nDocsReturned; i++) {
        ScoreDoc scoreDoc = topDocs.scoreDocs[i];
        ids[i] = scoreDoc.doc;
        if (scores != null) scores[i] = scoreDoc.score;
      }
    }

    int sliceLen = Math.min(lastDocRequested,nDocsReturned);
    if (sliceLen < 0) sliceLen=0;
    qr.setDocList(new DocSlice(0,sliceLen,ids,scores,totalHits,maxScore));
  }

  // any DocSet returned is for the query only, without any filtering... that way it may
  // be cached if desired.
  private DocSet getDocListAndSetNC(QueryResult qr,QueryCommand cmd) throws IOException {
    int len = cmd.getSupersetMaxDoc();
    int last = len;
    if (last < 0 || last > maxDoc()) last=maxDoc();
    final int lastDocRequested = last;
    int nDocsReturned;
    int totalHits;
    float maxScore;
    int[] ids;
    float[] scores;
    DocSet set;

    boolean needScores = (cmd.getFlags() & GET_SCORES) != 0;
    int maxDoc = maxDoc();
    int smallSetSize = maxDoc>>6;

    ProcessedFilter pf = getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
    final Filter luceneFilter = pf.filter;

    Query query = QueryUtils.makeQueryable(cmd.getQuery());

    // handle zero case...
    if (lastDocRequested<=0) {
      final float[] topscore = new float[] { Float.NEGATIVE_INFINITY };

      Collector collector;
      final DocSetCollector setCollector = new DocSetCollector(smallSetSize, maxDoc);

       if (!needScores) {
         collector = setCollector;
       } else {
         final Collector topScoreCollector = new SimpleCollector() {
          
           Scorer scorer;
           
           @Override
          public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
          }
           
          @Override
          public void collect(int doc) throws IOException {
            float score = scorer.score();
            if (score > topscore[0]) topscore[0] = score;
          }
          
          @Override
          public boolean acceptsDocsOutOfOrder() {
            return true;
          }
        };
        
        collector = MultiCollector.wrap(setCollector, topScoreCollector);
       }
       
       buildAndRunCollectorChain(qr, query, luceneFilter, collector, cmd, pf.postFilter);

      set = setCollector.getDocSet();

      nDocsReturned = 0;
      ids = new int[nDocsReturned];
      scores = new float[nDocsReturned];
      totalHits = set.size();
      maxScore = totalHits>0 ? topscore[0] : 0.0f;
      // no docs on this page, so cursor doesn't change
      qr.setNextCursorMark(cmd.getCursorMark());
    } else {

      final TopDocsCollector topCollector = buildTopDocsCollector(len, cmd);
      DocSetCollector setCollector = new DocSetCollector(maxDoc>>6, maxDoc);
      Collector collector = MultiCollector.wrap(topCollector, setCollector);

      buildAndRunCollectorChain(qr, query, luceneFilter, collector, cmd, pf.postFilter);

      set = setCollector.getDocSet();      

      totalHits = topCollector.getTotalHits();
      assert(totalHits == set.size());

      TopDocs topDocs = topCollector.topDocs(0, len);
      populateNextCursorMarkFromTopDocs(qr, cmd, topDocs);
      maxScore = totalHits>0 ? topDocs.getMaxScore() : 0.0f;
      nDocsReturned = topDocs.scoreDocs.length;

      ids = new int[nDocsReturned];
      scores = (cmd.getFlags()&GET_SCORES)!=0 ? new float[nDocsReturned] : null;
      for (int i=0; i<nDocsReturned; i++) {
        ScoreDoc scoreDoc = topDocs.scoreDocs[i];
        ids[i] = scoreDoc.doc;
        if (scores != null) scores[i] = scoreDoc.score;
      }
    }

    int sliceLen = Math.min(lastDocRequested,nDocsReturned);
    if (sliceLen < 0) sliceLen=0;

    qr.setDocList(new DocSlice(0,sliceLen,ids,scores,totalHits,maxScore));
    // TODO: if we collect results before the filter, we just need to intersect with
    // that filter to generate the DocSet for qr.setDocSet()
    qr.setDocSet(set);

    // TODO: currently we don't generate the DocSet for the base query,
    // but the QueryDocSet == CompleteDocSet if filter==null.
    return pf.filter==null && pf.postFilter==null ? qr.getDocSet() : null;
  }


  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocList getDocList(Query query, DocSet filter, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilter(filter)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocList();
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>.  Also returns the complete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * This method is cache aware and may retrieve <code>filter</code> from
   * the cache or make an insertion into the cache as a result of this call.
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   * <p>
   * The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocListAndSet getDocListAndSet(Query query, Query filter, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilterList(filter)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len)
      .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
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
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocListAndSet getDocListAndSet(Query query, Query filter, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilterList(filter)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len)
      .setFlags(flags)
      .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
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
   * @param filterList   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocListAndSet getDocListAndSet(Query query, List<Query> filterList, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilterList(filterList)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len)
      .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
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
   * @param filterList   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocListAndSet getDocListAndSet(Query query, List<Query> filterList, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilterList(filterList)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len)
      .setFlags(flags)
      .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code>
   * and sorted by <code>sort</code>. Also returns the compete set of documents
   * matching <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   * <p>
   * FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocListAndSet getDocListAndSet(Query query, DocSet filter, Sort lsort, int offset, int len) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilter(filter)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len)
      .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
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
   * @param filter   may be null
   * @param lsort    criteria by which to sort (if null, query relevance is used)
   * @param offset   offset into the list of documents to return
   * @param len      maximum number of documents to return
   * @param flags    user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  public DocListAndSet getDocListAndSet(Query query, DocSet filter, Sort lsort, int offset, int len, int flags) throws IOException {
    QueryCommand qc = new QueryCommand();
    qc.setQuery(query)
      .setFilter(filter)
      .setSort(lsort)
      .setOffset(offset)
      .setLen(len)
      .setFlags(flags)
      .setNeedDocSet(true);
    QueryResult qr = new QueryResult();
    search(qr,qc);
    return qr.getDocListAndSet();
  }

  protected void sortDocSet(QueryResult qr, QueryCommand cmd) throws IOException {
    DocSet set = qr.getDocListAndSet().docSet;
    int nDocs = cmd.getSupersetMaxDoc();
    if (nDocs == 0) {
      // SOLR-2923
      qr.getDocListAndSet().docList = new DocSlice(0, 0, new int[0], null, set.size(), 0f);
      qr.setNextCursorMark(cmd.getCursorMark());
      return;
    }


    // bit of a hack to tell if a set is sorted - do it better in the future.
    boolean inOrder = set instanceof BitDocSet || set instanceof SortedIntDocSet;

    TopDocsCollector topCollector = buildTopDocsCollector(nDocs, cmd);

    DocIterator iter = set.iterator();
    int base=0;
    int end=0;
    int readerIndex = 0;

    while (iter.hasNext()) {
      int doc = iter.nextDoc();
      while (doc>=end) {
        LeafReaderContext leaf = leafContexts.get(readerIndex++);
        base = leaf.docBase;
        end = base + leaf.reader().maxDoc();
        topCollector.getLeafCollector(leaf);
        // we should never need to set the scorer given the settings for the collector
      }
      topCollector.collect(doc-base);
    }
    
    TopDocs topDocs = topCollector.topDocs(0, nDocs);

    int nDocsReturned = topDocs.scoreDocs.length;
    int[] ids = new int[nDocsReturned];

    for (int i=0; i<nDocsReturned; i++) {
      ScoreDoc scoreDoc = topDocs.scoreDocs[i];
      ids[i] = scoreDoc.doc;
    }

    qr.getDocListAndSet().docList = new DocSlice(0,nDocsReturned,ids,null,topDocs.totalHits,0.0f);
    populateNextCursorMarkFromTopDocs(qr, cmd, topDocs);
  }



  /**
   * Returns the number of documents that match both <code>a</code> and <code>b</code>.
   * <p>
   * This method is cache-aware and may check as well as modify the cache.
   *
   * @return the number of documents in the intersection between <code>a</code> and <code>b</code>.
   * @throws IOException If there is a low-level I/O error.
   */
  public int numDocs(Query a, DocSet b) throws IOException {
    if (filterCache != null) {
      // Negative query if absolute value different from original
      Query absQ = QueryUtils.getAbs(a);
      DocSet positiveA = getPositiveDocSet(absQ);
      return a==absQ ? b.intersectionSize(positiveA) : b.andNotSize(positiveA);
    } else {
      // If there isn't a cache, then do a single filtered query
      // NOTE: we cannot use FilteredQuery, because BitDocSet assumes it will never 
      // have deleted documents, but UninvertedField's doNegative has sets with deleted docs
      TotalHitCountCollector collector = new TotalHitCountCollector();
      BooleanQuery bq = new BooleanQuery();
      bq.add(QueryUtils.makeQueryable(a), BooleanClause.Occur.MUST);
      bq.add(new ConstantScoreQuery(b.getTopFilter()), BooleanClause.Occur.MUST);
      super.search(bq, null, collector);
      return collector.getTotalHits();
    }
  }

  /** @lucene.internal */
  public int numDocs(DocSet a, DocsEnumState deState) throws IOException {
    // Negative query if absolute value different from original
    return a.intersectionSize(getDocSet(deState));
  }

  public static class DocsEnumState {
    public String fieldName;  // currently interned for as long as lucene requires it
    public TermsEnum termsEnum;
    public Bits liveDocs;
    public DocsEnum docsEnum;

    public int minSetSizeCached;

    public int[] scratch;
  }

   /**
   * Returns the number of documents that match both <code>a</code> and <code>b</code>.
   * <p>
   * This method is cache-aware and may check as well as modify the cache.
   *
   * @return the number of documents in the intersection between <code>a</code> and <code>b</code>.
   * @throws IOException If there is a low-level I/O error.
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
  public StoredDocument[] readDocs(DocList ids) throws IOException {
     StoredDocument[] docs = new StoredDocument[ids.size()];
     readDocs(docs,ids);
     return docs;
  }



  /**
   * Warm this searcher based on an old one (primarily for auto-cache warming).
   */
  public void warm(SolrIndexSearcher old) throws IOException {
    // Make sure this is first!  filters can help queryResults execute!
    long warmingStartTime = System.nanoTime();
    // warm the caches in order...
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("warming","true");
    for (int i=0; i<cacheList.length; i++) {
      if (debug) log.debug("autowarming " + this + " from " + old + "\n\t" + old.cacheList[i]);


      SolrQueryRequest req = new LocalSolrQueryRequest(core,params) {
        @Override public SolrIndexSearcher getSearcher() { return SolrIndexSearcher.this; }
        @Override public void close() { }
      };

      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      try {
        this.cacheList[i].warm(this, old.cacheList[i]);
      } finally {
        try {
          req.close();
        } finally {
          SolrRequestInfo.clearRequestInfo();
        }
      }

      if (debug) log.debug("autowarming result for " + this + "\n\t" + this.cacheList[i]);
    }
    warmupTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - warmingStartTime, TimeUnit.NANOSECONDS);
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

  public long getOpenTime() {
    return openTime;
  }

  @Override
  public Explanation explain(Query query, int doc) throws IOException {
    return super.explain(QueryUtils.makeQueryable(query), doc);
  }

  /////////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  @Override
  public String getName() {
    return SolrIndexSearcher.class.getName();
  }

  @Override
  public String getVersion() {
    return SolrCore.version;
  }

  @Override
  public String getDescription() {
    return "index searcher";
  }

  @Override
  public Category getCategory() {
    return Category.CORE;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  @Override
  public NamedList<Object> getStatistics() {
    NamedList<Object> lst = new SimpleOrderedMap<>();
    lst.add("searcherName", name);
    lst.add("caching", cachingEnabled);
    lst.add("numDocs", reader.numDocs());
    lst.add("maxDoc", reader.maxDoc());
    lst.add("deletedDocs", reader.maxDoc() - reader.numDocs());
    lst.add("reader", reader.toString());
    lst.add("readerDir", reader.directory());
    lst.add("indexVersion", reader.getVersion());
    lst.add("openedAt", new Date(openTime));
    if (registerTime!=0) lst.add("registeredAt", new Date(registerTime));
    lst.add("warmupTime", warmupTime);
    return lst;
  }

  /**
   * A query request command to avoid having to change the method signatures
   * if we want to pass additional information to the searcher.
   */
  public static class QueryCommand {
    private Query query;
    private List<Query> filterList;
    private DocSet filter;
    private Sort sort;
    private int offset;
    private int len;
    private int supersetMaxDoc;
    private int flags;
    private long timeAllowed = -1;
    private CursorMark cursorMark;
    
    public CursorMark getCursorMark() {
      return cursorMark;
    }
    public QueryCommand setCursorMark(CursorMark cursorMark) {
      this.cursorMark = cursorMark;
      if (null != cursorMark) {
        // If we're using a cursor then we can't allow queryResult caching because the 
        // cache keys don't know anything about the collector used.
        //
        // in theory, we could enhance the cache keys to be aware of the searchAfter
        // FieldDoc but then there would still be complexity around things like the cache
        // window size that would need to be worked out
        //
        // we *can* however allow the use of checking the filterCache for non-score based
        // sorts, because that still runs our paging collector over the entire DocSet
        this.flags |= (NO_CHECK_QCACHE | NO_SET_QCACHE);
      }
      return this;
    }

    public Query getQuery() { return query; }
    public QueryCommand setQuery(Query query) {
      this.query = query;
      return this;
    }
    
    public List<Query> getFilterList() { return filterList; }
    /**
     * @throws IllegalArgumentException if filter is not null.
     */
    public QueryCommand setFilterList(List<Query> filterList) {
      if( filter != null ) {
        throw new IllegalArgumentException( "Either filter or filterList may be set in the QueryCommand, but not both." );
      }
      this.filterList = filterList;
      return this;
    }
    /**
     * A simple setter to build a filterList from a query
     * @throws IllegalArgumentException if filter is not null.
     */
    public QueryCommand setFilterList(Query f) {
      if( filter != null ) {
        throw new IllegalArgumentException( "Either filter or filterList may be set in the QueryCommand, but not both." );
      }
      filterList = null;
      if (f != null) {
        filterList = new ArrayList<>(2);
        filterList.add(f);
      }
      return this;
    }
    
    public DocSet getFilter() { return filter; }
    /**
     * @throws IllegalArgumentException if filterList is not null.
     */
    public QueryCommand setFilter(DocSet filter) {
      if( filterList != null ) {
        throw new IllegalArgumentException( "Either filter or filterList may be set in the QueryCommand, but not both." );
      }
      this.filter = filter;
      return this;
    }

    public Sort getSort() { return sort; }
    public QueryCommand setSort(Sort sort) {
      this.sort = sort;
      return this;
    }
    
    public int getOffset() { return offset; }
    public QueryCommand setOffset(int offset) {
      this.offset = offset;
      return this;
    }
    
    public int getLen() { return len; }
    public QueryCommand setLen(int len) {
      this.len = len;
      return this;
    }
    
    public int getSupersetMaxDoc() { return supersetMaxDoc; }
    public QueryCommand setSupersetMaxDoc(int supersetMaxDoc) {
      this.supersetMaxDoc = supersetMaxDoc;
      return this;
    }

    public int getFlags() {
      return flags;
    }

    public QueryCommand replaceFlags(int flags) {
      this.flags = flags;
      return this;
    }

    public QueryCommand setFlags(int flags) {
      this.flags |= flags;
      return this;
    }

    public QueryCommand clearFlags(int flags) {
      this.flags &= ~flags;
      return this;
    }

    public long getTimeAllowed() { return timeAllowed; }
    public QueryCommand setTimeAllowed(long timeAllowed) {
      this.timeAllowed = timeAllowed;
      return this;
    }
    
    public boolean isNeedDocSet() { return (flags & GET_DOCSET) != 0; }
    public QueryCommand setNeedDocSet(boolean needDocSet) {
      return needDocSet ? setFlags(GET_DOCSET) : clearFlags(GET_DOCSET);
    }
  }


  /**
   * The result of a search.
   */
  public static class QueryResult {
    private boolean partialResults;
    private DocListAndSet docListAndSet;
    private CursorMark nextCursorMark;

    public Object groupedResults;   // TODO: currently for testing
    
    public DocList getDocList() { return docListAndSet.docList; }
    public void setDocList(DocList list) {
      if( docListAndSet == null ) {
        docListAndSet = new DocListAndSet();
      }
      docListAndSet.docList = list;
    }

    public DocSet getDocSet() { return docListAndSet.docSet; }
    public void setDocSet(DocSet set) {
      if( docListAndSet == null ) {
        docListAndSet = new DocListAndSet();
      }
      docListAndSet.docSet = set;
    }

    public boolean isPartialResults() { return partialResults; }
    public void setPartialResults(boolean partialResults) { this.partialResults = partialResults; }

    public void setDocListAndSet( DocListAndSet listSet ) { docListAndSet = listSet; }
    public DocListAndSet getDocListAndSet() { return docListAndSet; }

    public void setNextCursorMark(CursorMark next) {
      this.nextCursorMark = next;
    }
    public CursorMark getNextCursorMark() {
      return nextCursorMark;
    }
  }

}


class FilterImpl extends Filter {
  final DocSet filter;
  final Filter topFilter;
  final List<Weight> weights;

  public FilterImpl(DocSet filter, List<Weight> weights) {
    this.filter = filter;
    this.weights = weights;
    this.topFilter = filter == null ? null : filter.getTopFilter();
  }

  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
    DocIdSet sub = topFilter == null ? null : topFilter.getDocIdSet(context, acceptDocs);
    if (weights.size() == 0) return sub;
    return new FilterSet(sub, context);
  }

  private class FilterSet extends DocIdSet {
    DocIdSet docIdSet;
    LeafReaderContext context;

    public FilterSet(DocIdSet docIdSet, LeafReaderContext context) {
      this.docIdSet = docIdSet;
      this.context = context;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      List<DocIdSetIterator> iterators = new ArrayList<>(weights.size()+1);
      if (docIdSet != null) {
        DocIdSetIterator iter = docIdSet.iterator();
        if (iter == null) return null;
        iterators.add(iter);
      }
      for (Weight w : weights) {
        Scorer scorer = w.scorer(context, context.reader().getLiveDocs());
        if (scorer == null) return null;
        iterators.add(scorer);
      }
      if (iterators.size()==0) return null;
      if (iterators.size()==1) return iterators.get(0);
      if (iterators.size()==2) return new DualFilterIterator(iterators.get(0), iterators.get(1));
      return new FilterIterator(iterators.toArray(new DocIdSetIterator[iterators.size()]));
    }

    @Override
    public Bits bits() throws IOException {
      return null;  // don't use random access
    }

    @Override
    public long ramBytesUsed() {
      return docIdSet != null ? docIdSet.ramBytesUsed() : 0L;
    }
  }

  private static class FilterIterator extends DocIdSetIterator {
    final DocIdSetIterator[] iterators;
    final DocIdSetIterator first;

    public FilterIterator(DocIdSetIterator[] iterators) {
      this.iterators = iterators;
      this.first = iterators[0];
    }

    @Override
    public int docID() {
      return first.docID();
    }

    private int doNext(int doc) throws IOException {
      int which=0;  // index of the iterator with the highest id
      int i=1;
      outer: for(;;) {
        for (; i<iterators.length; i++) {
          if (i == which) continue;
          DocIdSetIterator iter = iterators[i];
          int next = iter.advance(doc);
          if (next != doc) {
            doc = next;
            which = i;
            i = 0;
            continue outer;
          }
        }
        return doc;
      }
    }


    @Override
    public int nextDoc() throws IOException {
      return doNext(first.nextDoc());
    }

    @Override
    public int advance(int target) throws IOException {
      return doNext(first.advance(target));
    }

    @Override
    public long cost() {
      return first.cost();
    }
  }

  private static class DualFilterIterator extends DocIdSetIterator {
    final DocIdSetIterator a;
    final DocIdSetIterator b;

    public DualFilterIterator(DocIdSetIterator a, DocIdSetIterator b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public int docID() {
      return a.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int doc = a.nextDoc();
      for(;;) {
        int other = b.advance(doc);
        if (other == doc) return doc;
        doc = a.advance(other);
        if (other == doc) return doc;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = a.advance(target);
      for(;;) {
        int other = b.advance(doc);
        if (other == doc) return doc;
        doc = a.advance(other);
        if (other == doc) return doc;
      }
    }

    @Override
    public long cost() {
      return Math.min(a.cost(), b.cost());
    }
  }

}
