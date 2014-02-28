package org.apache.lucene.server;

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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.document.Document;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.CachedOrdinalsReader;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDocument;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.PersistentSnapshotDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherLifetimeManager;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.server.handlers.BuildSuggestHandler;
import org.apache.lucene.server.handlers.LiveSettingsHandler;
import org.apache.lucene.server.handlers.RegisterFieldHandler;
import org.apache.lucene.server.handlers.SettingsHandler;
import org.apache.lucene.server.params.BooleanType;
import org.apache.lucene.server.params.FloatType;
import org.apache.lucene.server.params.IntType;
import org.apache.lucene.server.params.LongType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import org.apache.lucene.server.params.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.RateLimitedDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.packed.PackedInts;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyleIdent;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

/** Holds all state associated with one index.  On startup
 *  and on creating a new index, the index loads its state
 *  but does not start itself.  At this point, setting can
 *  be changed, and then the index must be {@link #start}ed
 *  before it can be used for indexing and searching, at
 *  which point only live settings may be changed.
 *
 *  Filesystem state: each index has its own rootDir,
 *  specified when the index is created.  Under each
 *  rootDir:
 *  <ul>
 *    <li> {@code index} has the Lucene index
 *    <li> {@code taxonomy} has the taxonomy index (empty if
 *     no facets are indexed)
 *   <li> {@code state} has all current settings
 *   <li> {@code state/state.N} gen files holds all settings
 *   <li> {@code state/saveLoadRefCounts.N} gen files holds
 *     all reference counts from live snapshots
 *  </ul>
 */

public class IndexState implements Closeable {
  /** Default {@link Version} for {@code matchVersion}. */
  public Version matchVersion = Version.LUCENE_46;

  /** Creates directories */
  DirectoryFactory df = DirectoryFactory.get("FSDirectory");

  /** Loads all resources for analysis components */
  public final ResourceLoader resourceLoader;

  /** Where all index state is saved */
  public final File rootDir;

  /** Base directory */
  public Directory origIndexDir;

  /** Possibly NRTCachingDir wrap of origIndexDir */
  Directory indexDir;

  /** Taxonomy directory */
  Directory taxoDir;

  /** Wrapped IndexWriter wrapped so
   *  ControlledRealTimeReopenThread can trackany changes */
  public TrackingIndexWriter writer;

  /** Taxonomy writer */
  DirectoryTaxonomyWriter taxoWriter;

  /** Internal IndexWriter used by DirectoryTaxonomyWriter;
   *  we pull this out so we can .deleteUnusedFiles after
   *  a snapshot is removed. */
  public IndexWriter taxoInternalWriter;

  private final GlobalState globalState;

  /** Which norms format to use for all indexed fields. */
  public String normsFormat = "Lucene42";

  /** If normsFormat is Lucene42, what
   *  acceptableOverheadRatio to pass. */
  public float normsAcceptableOverheadRatio = PackedInts.FASTEST;

  /** All live values fields. */
  public final Map<String,StringLiveFieldValues> liveFieldValues = new ConcurrentHashMap<String,StringLiveFieldValues>();

  /** Maps snapshot gen -> version. */
  public final Map<Long,Long> snapshotGenToVersion = new ConcurrentHashMap<Long,Long>();

  /** Built suggest implementations */
  public final Map<String,Lookup> suggesters = new ConcurrentHashMap<String,Lookup>();

  /** All cached filters. */
  public final Map<String,CachingWrapperFilter> cachedFilters = new ConcurrentHashMap<String,CachingWrapperFilter>();

  /** Holds pending save state, written to state.N file on
   *  commit. */
  // nocommit move these to their own obj, make it sync'd,
  // instead of syncing on IndexState instance:
  final JSONObject settingsSaveState = new JSONObject();
  final JSONObject liveSettingsSaveState = new JSONObject();
  final JSONObject suggestSaveState = new JSONObject();
  final JSONObject fieldsSaveState = new JSONObject();

  /** Holds cached ordinals; doesn't use any RAM unless it's
   *  actually used when a caller sets useOrdsCache=true. */
  public final Map<String,OrdinalsReader> ordsCache = new HashMap<String,OrdinalsReader>();

  /** The schema (registerField) */
  private final Map<String,FieldDef> fields = new ConcurrentHashMap<String,FieldDef>();

  /** Contains fields set as facetIndexFieldName. */
  public final Set<String> internalFacetFieldNames = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

  public final FacetsConfig facetsConfig = new FacetsConfig();

  /** {@link Bindings} to pass when evaluating expressions. */
  public final Bindings exprBindings = new FieldDefBindings(fields);

  /** Tracks snapshot references to generations. */
  private static class SaveLoadRefCounts extends GenFileUtil<Map<Long,Integer>> {

    public SaveLoadRefCounts(Directory dir) {
      super(dir, "stateRefCounts");
    }
    
    @Override
    protected void saveOne(IndexOutput out, Map<Long,Integer> refCounts) throws IOException {
      JSONObject o = new JSONObject();
      for(Map.Entry<Long,Integer> ent : refCounts.entrySet()) {
        o.put(ent.getKey().toString(), ent.getValue());
      }
      byte[] bytes = IndexState.toUTF8(o.toString());
      out.writeBytes(bytes, 0, bytes.length);
    }

    @Override
    protected Map<Long,Integer> loadOne(IndexInput in) throws IOException {
      int numBytes = (int) in.length();
      byte[] bytes = new byte[numBytes];
      in.readBytes(bytes, 0, bytes.length);
      String s = IndexState.fromUTF8(bytes);
      JSONObject o = null;
      try {
        o = (JSONObject) JSONValue.parseStrict(s);
      } catch (ParseException pe) {
        // Change to IOExc so gen logic will fallback:
        throw new IOException("invalid JSON when parsing refCounts", pe);
      }
      Map<Long,Integer> refCounts = new HashMap<Long,Integer>();      
      for(Map.Entry<String,Object> ent : o.entrySet()) {
        refCounts.put(Long.parseLong(ent.getKey()), ((Integer) ent.getValue()).intValue());
      }
      return refCounts;
    }
  }

  private class SaveLoadState extends GenFileUtil<JSONObject> {

    public SaveLoadState(Directory dir) {
      super(dir, "state");
    }
    
    @Override
    protected void saveOne(IndexOutput out, JSONObject state) throws IOException {
      // Pretty print:
      String pretty = state.toJSONString(new JSONStyleIdent());
      byte[] bytes = IndexState.toUTF8(pretty.toString());
      out.writeBytes(bytes, 0, bytes.length);
    }

    @Override
    protected JSONObject loadOne(IndexInput in) throws IOException {
      int numBytes = (int) in.length();
      byte[] bytes = new byte[numBytes];
      in.readBytes(bytes, 0, numBytes);
      String s = IndexState.fromUTF8(bytes);
      JSONObject ret = null;
      try {
        ret = (JSONObject) JSONValue.parseStrict(s);
      } catch (ParseException pe) {
        // Change to IOExc so gen logic will fallback:
        throw new IOException("invalid JSON when parsing refCounts", pe);
      }
      return ret;
    }

    @Override
    protected boolean canDelete(long gen) {
      return !hasRef(gen);
    }
  }

  /** Which snapshots (List<Long>) are referencing which
   *  save state generations. */
  Map<Long,Integer> genRefCounts;
  SaveLoadRefCounts saveLoadGenRefCounts;

  SaveLoadState saveLoadState;

  /** Enables lookup of previously used searchers, so
   *  follow-on actions (next page, drill down/sideways/up,
   *  etc.) use the same searcher as the original search, as
   *  long as that searcher hasn't expired. */
  public final SearcherLifetimeManager slm = new SearcherLifetimeManager();

  /** Indexes changes, and provides the live searcher,
   *  possibly searching a specific generation. */
  public SearcherTaxonomyManager manager;

  /** Thread to periodically reopen the index. */
  public ControlledRealTimeReopenThread<SearcherAndTaxonomy> reopenThread;

  /** Periodically wakes up and prunes old searchers from
   *  slm. */
  Thread searcherPruningThread;

  /** Per-field wrapper that provides the analyzer
   *  configured in the FieldDef */

  private static final Analyzer keywordAnalyzer = new KeywordAnalyzer();

  /** Index-time analyzer. */
  public final Analyzer indexAnalyzer = new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
        @Override
        public Analyzer getWrappedAnalyzer(String name) {
          FieldDef fd = getField(name);
          if (fd.valueType.equals("atom")) {
            return keywordAnalyzer;
          }
          if (fd.indexAnalyzer == null) {
            throw new IllegalArgumentException("field \"" + name + "\" did not specify analyzer or indexAnalyzer");
          }
          return fd.indexAnalyzer;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
          return components;
        }
      };

  /** Search-time analyzer. */
  public final Analyzer searchAnalyzer = new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
        @Override
        public Analyzer getWrappedAnalyzer(String name) {
          FieldDef fd = getField(name);
          if (fd.valueType.equals("atom")) {
            return keywordAnalyzer;
          }
          if (fd.searchAnalyzer == null) {
            throw new IllegalArgumentException("field \"" + name + "\" did not specify analyzer or searchAnalyzer");
          }
          return fd.searchAnalyzer;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
          return components;
        }
      };

  /** Per-field wrapper that provides the similarity
   *  configured in the FieldDef */
  final Similarity sim = new PerFieldSimilarityWrapper() {
      final Similarity defaultSim = new DefaultSimilarity();

      @Override
      public Similarity get(String name) {
        if (internalFacetFieldNames.contains(name)) {
          return defaultSim;
        }
        return getField(name).sim;
      }
    };

  /** When a search is waiting on a specific generation, we
   *  will wait at most this many seconds before reopening
   *  (default 50 msec). */
  volatile double minRefreshSec = .05f;

  /** When no searcher is waiting on a specific generation, we
   *  will wait at most this many seconds before proactively
   *  reopening (default 1 sec). */
  volatile double maxRefreshSec = 1.0f;

  /** Once a searcher becomes stale (i.e., a new searcher is
   *  opened), we will close it after this much time
   *  (default: 60 seconds).  If this is too small, it means
   *  that old searches returning for a follow-on query may
   *  find their searcher pruned (lease expired). */
  volatile double maxSearcherAgeSec = 60;

  /** RAM buffer size passed to {@link
   *  IndexWriterConfig#setRAMBufferSizeMB}. */
  volatile double indexRAMBufferSizeMB = 16;

  /** Holds the persistent snapshots */
  public PersistentSnapshotDeletionPolicy snapshots;

  /** Holds the persistent taxonomy snapshots */
  public PersistentSnapshotDeletionPolicy taxoSnapshots;

  /** Name of this index. */
  public final String name;

  /** True if this is a new index. */
  private final boolean doCreate;

  /** Sole constructor; creates a new index or loads an
   *  existing one if it exists, but does not start the
   *  index. */
  public IndexState(GlobalState globalState, String name, File rootDir, boolean doCreate) throws IOException {
    //System.out.println("IndexState.init name=" + name + " this=" + this + " rootDir=" + rootDir);
    this.globalState = globalState;
    this.name = name;
    this.rootDir = rootDir;
    if (rootDir != null) {
      if (!rootDir.exists()) {
        rootDir.mkdirs();
      }
      this.resourceLoader = new FilesystemResourceLoader(rootDir);
    } else {
      // nocommit can/should we make a DirectoryResourceLoader?
      this.resourceLoader = null;
    }

    this.doCreate = doCreate;
    if (doCreate == false) {
      initSaveLoadState();
    }
  }

  /** Context to hold state for a single indexing request. */
  public static class AddDocumentContext {

    /** How many documents were added. */
    public final AtomicInteger addCount = new AtomicInteger();

    /** Any indexing errors that occurred. */
    public final List<String> errors = new ArrayList<String>();

    /** Which document index hit each error. */
    public final List<Integer> errorIndex = new ArrayList<Integer>();

    /** Sole constructor. */
    public AddDocumentContext() {
    }

    /** Record an exception. */
    public synchronized void addException(int index, Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      pw.flush();
      errors.add(sw.toString());
      errorIndex.add(index);
    }
  }

  /** Holds state for a single document add/update; not
   *  static because it uses this.writer. */
  class AddDocumentJob implements Callable<Long> {
    private final Term updateTerm;
    private final Document doc;
    private final AddDocumentContext ctx;

    // Position of this document in the bulk request:
    private final int index;

    /** Sole constructor. */
    public AddDocumentJob(int index, Term updateTerm, Document doc, AddDocumentContext ctx) {
      this.updateTerm = updateTerm;
      this.doc = doc;
      this.ctx = ctx;
      this.index = index;
    }

    @Override
    public Long call() throws Exception {
      long gen = -1;

      try {
        IndexDocument idoc = facetsConfig.build(taxoWriter, doc);
        if (updateTerm == null) {
          gen = writer.addDocument(idoc);
        } else {
          gen = writer.updateDocument(updateTerm, idoc);
        }

        if (!liveFieldValues.isEmpty()) {
          // TODO: wasteful!
          for(StorableField f : doc.getFields()) {
            FieldDef fd = getField(f.name());
            if (fd.liveValuesIDField != null) {
              // TODO: O(N):
              String id = doc.get(fd.liveValuesIDField);
              if (id != null) {
                liveFieldValues.get(f.name()).add(id, f.stringValue());
              }
            }
          }
        }
      } catch (Exception e) {
        ctx.addException(index, e);
      } finally {
        ctx.addCount.incrementAndGet();
      }

      return gen;
    }
  }

  /** Retrieve the field's type.
   *
   *  @throws IllegalArgumentException if the field was not
   *     registered. */
  public FieldDef getField(String fieldName) {
    FieldDef fd = fields.get(fieldName);
    if (fd == null) {
      String message = "field \"" + fieldName + "\" is unknown: it was not registered with registerField";
      throw new IllegalArgumentException(message);
    }
    return fd;
  }

  /** Lookup the value of {@code paramName} in the {@link
   *  Request} and then passes that field name to {@link
   *  #getField(String)}.
   *
   *  @throws IllegalArgumentException if the field was not
   *     registered. */
  public FieldDef getField(Request r, String paramName) {
    String fieldName = r.getString(paramName);
    FieldDef fd = fields.get(fieldName);
    if (fd == null) {
      String message = "field \"" + fieldName + "\" is unknown: it was not registered with registerField";
      r.fail(paramName, message);
    }

    return fd;
  }

  /** Returns all field names that are indexed and analyzed. */
  public List<String> getIndexedAnalyzedFields() {
    List<String> result = new ArrayList<String>();
    for(FieldDef fd : fields.values()) {
      // TODO: should we default to include numeric fields too...?
      if (fd.fieldType != null && fd.fieldType.indexed() && fd.searchAnalyzer != null) {
        result.add(fd.name);
      }
    }

    return result;
  }

  public Map<String,FieldDef> getAllFields() {
    return Collections.unmodifiableMap(fields);
  }

  /** True if this index has at least one commit. */
  public boolean hasCommit() throws IOException {
    return saveLoadState.getNextWriteGen() != 0;
  }

  /** Returns cached ordinals for the specified index field
   *  name. */
  public synchronized OrdinalsReader getOrdsCache(String indexFieldName) {
    OrdinalsReader ords = ordsCache.get(indexFieldName);
    if (ords == null) {
      ords = new CachedOrdinalsReader(new DocValuesOrdinalsReader(indexFieldName));
      ordsCache.put(indexFieldName, ords);
    }

    return ords;
  }

  /** Record that this snapshot id refers to the current
   *  generation, returning it. */
  public synchronized long incRefLastCommitGen() throws IOException {
    long result;
    long nextGen = saveLoadState.getNextWriteGen();
    //System.out.println("state nextGen=" + nextGen);
    if (nextGen == 0) {
      throw new IllegalStateException("no commit exists");
    }
    result = nextGen-1;
    if (result != -1) {
      incRef(result);
    }
    return result;
  }

  private synchronized void incRef(long stateGen) throws IOException {
    Integer rc = genRefCounts.get(stateGen);
    if (rc == null) {
      genRefCounts.put(stateGen, 1);
    } else {
      genRefCounts.put(stateGen, 1+rc.intValue());
    }
    saveLoadGenRefCounts.save(genRefCounts);
  }

  /** Drop this snapshot from the references. */
  public synchronized void decRef(long stateGen) throws IOException {
    Integer rc = genRefCounts.get(stateGen);
    if (rc == null) {
      throw new IllegalArgumentException("stateGen=" + stateGen + " is not held by a snapshot");
    }
    assert rc.intValue() > 0;
    if (rc.intValue() == 1) {
      genRefCounts.remove(stateGen);
    } else {
      genRefCounts.put(stateGen, rc.intValue()-1);
    }
    saveLoadGenRefCounts.save(genRefCounts);
  }

  /** True if this generation is still referenced by at
   *  least one snapshot. */
  public synchronized boolean hasRef(long gen) {
    Integer rc = genRefCounts.get(gen);
    if (rc == null) {
      return false;
    } else {
      assert rc.intValue() > 0;
      return true;
    }
  }

  /** Records a new field in the internal {@code fields} state. */
  public synchronized void addField(FieldDef fd, JSONObject json) {
    if (fields.containsKey(fd.name)) {
      throw new IllegalArgumentException("field \"" + fd.name + "\" was already registered");
    }
    fields.put(fd.name, fd);
    if (fd.liveValuesIDField != null) {
      liveFieldValues.put(fd.name, new StringLiveFieldValues(manager, fd.liveValuesIDField, fd.name));
    }
    assert !fieldsSaveState.containsKey(fd.name);
    fieldsSaveState.put(fd.name, json);
    if (fd.faceted != null && fd.faceted.equals("no") == false && fd.faceted.equals("numericRange") == false) {
      internalFacetFieldNames.add(facetsConfig.getDimConfig(fd.name).indexFieldName);
    }
  }

  /** Returns JSON representation of all registered fields. */
  public synchronized String getAllFieldsJSON() {
    return fieldsSaveState.toString();
  }

  /** Returns JSON representation of all settings. */
  public synchronized String getSettingsJSON() {
    return settingsSaveState.toString();
  }

  /** Returns JSON representation of all live settings. */
  public synchronized String getLiveSettingsJSON() {
    return liveSettingsSaveState.toString();
  }

  /** Records a new suggestor state. */
  public synchronized void addSuggest(String name, JSONObject o) {
    suggestSaveState.put(name, o);
  }

  /** Job for a single block addDocuments call. */
  class AddDocumentsJob implements Callable<Long> {
    private final Term updateTerm;
    private final Iterable<Document> docs;
    private final AddDocumentContext ctx;

    // Position of this document in the bulk request:
    private final int index;

    /** Sole constructor. */
    public AddDocumentsJob(int index, Term updateTerm, Iterable<Document> docs, AddDocumentContext ctx) {
      this.updateTerm = updateTerm;
      this.docs = docs;
      this.ctx = ctx;
      this.index = index;
    }

    @Override
    public Long call() throws Exception {
      long gen = -1;
      try {
        List<IndexDocument> justDocs = new ArrayList<IndexDocument>();
        for(Document doc : docs) {
          // Translate any FacetFields:
          justDocs.add(facetsConfig.build(taxoWriter, doc));
        }

        //System.out.println(Thread.currentThread().getName() + ": add; " + docs);
        if (updateTerm == null) {
          gen = writer.addDocuments(justDocs);
        } else {
          gen = writer.updateDocuments(updateTerm, justDocs);
        }

        if (!liveFieldValues.isEmpty()) {
          for(Document doc : docs) {
            // TODO: wasteful!
            for(StorableField f : doc.getFields()) {
              FieldDef fd = getField(f.name());
              if (fd.liveValuesIDField != null) {
                // TODO: O(N):
                String id = doc.get(fd.liveValuesIDField);
                if (id != null) {
                  liveFieldValues.get(f.name()).add(id, f.stringValue());
                }
              }
            }
          }
        }

      } catch (Exception e) {
        ctx.addException(index, e);
      } finally {
        ctx.addCount.incrementAndGet();
      }

      return gen;
    }
  }

  /** Create a new {@code AddDocumentJob}. */
  public Callable<Long> getAddDocumentJob(int index, Term term, Document doc, AddDocumentContext ctx) {
    return new AddDocumentJob(index, term, doc, ctx);
  }

  /** Create a new {@code AddDocumentsJob}. */
  public Callable<Long> getAddDocumentsJob(int index, Term term, Iterable<Document> docs, AddDocumentContext ctx) {
    return new AddDocumentsJob(index, term, docs, ctx);
  }

  /** Restarts the reopen thread (called when the live
   *  settings have changed). */
  public void restartReopenThread() {
    // nocommit sync
    if (manager == null) {
      return;
    }
    if (reopenThread != null) {
      reopenThread.close();
    }
    reopenThread = new ControlledRealTimeReopenThread<SearcherAndTaxonomy>(writer, manager, maxRefreshSec, minRefreshSec);
    reopenThread.setName("LuceneNRTReopen-" + name);
    reopenThread.start();
  }

  /** Fold in new non-live settings from the incoming request into
   *  the stored settings. */
  public synchronized void mergeSimpleSettings(Request r) {
    if (writer != null) {
      throw new IllegalStateException("index \"" + name + "\" was already started (cannot change non-live settings)");
    }
    StructType topType = r.getType();
    Iterator<Map.Entry<String,Object>> it = r.getParams();
    while(it.hasNext()) {
      Map.Entry<String,Object> ent = it.next();
      String paramName = ent.getKey();
      Param p = topType.params.get(paramName);
      if (p == null) {
        throw new IllegalArgumentException("unrecognized parameter \"" + paramName + "\"");
      }

      Type type = p.type;

      if (paramName.equals("matchVersion")) {
        Object value = ent.getValue();
        try {
          type.validate(value);
        } catch (IllegalArgumentException iae) {
          r.fail(paramName, iae.toString());
        }
        matchVersion = RegisterFieldHandler.getVersion((String) value);
        it.remove();
      } else if (type instanceof StringType || type instanceof IntType || type instanceof LongType || type instanceof FloatType || type instanceof BooleanType) {
        Object value = ent.getValue();
        try {
          type.validate(value);
        } catch (IllegalArgumentException iae) {
          r.fail(paramName, iae.toString());
        }
        // TODO: do more stringent validation?  eg
        // nrtCachingDirMaxMergeSizeMB must be >= 0.0
        settingsSaveState.put(paramName, value);
        it.remove();
      } else {
        r.fail(paramName, "unhandled parameter");
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    //System.out.println("IndexState.close name=" + name);
    commit();
    List<Closeable> closeables = new ArrayList<Closeable>();
    // nocommit catch exc & rollback:
    if (writer != null) {
      closeables.add(reopenThread);
      closeables.add(manager);
      closeables.add(writer.getIndexWriter());
      closeables.add(taxoWriter);
      closeables.add(slm);
      closeables.add(indexDir);
      closeables.add(taxoDir);
      writer = null;
    }

    for(Lookup suggester : suggesters.values()) {
      if (suggester instanceof Closeable) {
        closeables.add((Closeable) suggester);
      }
    }
    IOUtils.close(closeables);

    // nocommit should we remove this instance?  if app
    // starts again ... should we re-use the current
    // instance?  seems ... risky?
    // nocommit this is dangerous .. eg Server iterates
    // all IS.indices and closes ...:
    // nocommit need sync:

    globalState.indices.remove(name);
  }

  /** True if this index is started. */
  public boolean started() {
    return writer != null;
  }

  /** Live setting: et the mininum refresh time (seconds), which is the
   *  longest amount of time a client may wait for a
   *  searcher to reopen. */
  public synchronized void setMinRefreshSec(double min) {
    minRefreshSec = min;
    liveSettingsSaveState.put("minRefreshSec", min);
    restartReopenThread();
  }

  /** Live setting: set the maximum refresh time (seconds), which is the
   *  amount of time before we reopen the searcher
   *  proactively (when no search client is waiting). */
  public synchronized void setMaxRefreshSec(double max) {
    maxRefreshSec = max;
    liveSettingsSaveState.put("maxRefreshSec", max);
    restartReopenThread();
  }

  /** Live setting: nce a searcher becomes stale, we will close it after
   *  this many seconds. */
  public synchronized void setMaxSearcherAgeSec(double d) {
    maxSearcherAgeSec = d;
    liveSettingsSaveState.put("maxSearcherAgeSec", d);
  }

  /** Live setting: how much RAM to use for buffered documents during
   *  indexing (passed to {@link
   *  IndexWriterConfig#setRAMBufferSizeMB}. */
  public synchronized void setIndexRAMBufferSizeMB(double d) {
    indexRAMBufferSizeMB = d;
    liveSettingsSaveState.put("indexRAMBufferSizeMB", d);

    // nocommit sync: what if closeIndex is happening in
    // another thread:
    if (writer != null) {
      // Propogate the change to the open IndexWriter
      writer.getIndexWriter().getConfig().setRAMBufferSizeMB(d);
    }
  }

  /** Record the {@link DirectoryFactory} to use for this
   *  index. */
  public synchronized void setDirectoryFactory(DirectoryFactory df, Object saveState) {
    if (writer != null) {
      throw new IllegalStateException("index \"" + name + "\": cannot change Directory when the index is running");
    }
    settingsSaveState.put("directory", saveState);
    this.df = df;
  }

  public void setNormsFormat(String format, float acceptableOverheadRatio) {
    this.normsFormat = format;
    this.normsAcceptableOverheadRatio = acceptableOverheadRatio;
  }

  /** Get the current save state. */
  public synchronized JSONObject getSaveState() throws IOException {
    JSONObject o = new JSONObject();
    o.put("settings", settingsSaveState);
    o.put("liveSettings", liveSettingsSaveState);
    o.put("fields", fieldsSaveState);
    o.put("suggest", suggestSaveState);
    return o;
  }

  /** Commit all state. */
  public synchronized void commit() throws IOException {
    if (writer != null) {
      // nocommit: two phase commit?
      taxoWriter.commit();
      writer.getIndexWriter().commit();
    }

    // nocommit needs test case that creates index, changes
    // some settings, closes it w/o ever starting it:
    // settings changes are lost then?
    //System.out.println("IndexState.commit name=" + name + " saveLoadState=" + saveLoadState + " this=" + this);

    if (saveLoadState == null) {
      initSaveLoadState();
    }

    JSONObject saveState = new JSONObject();
    saveState.put("state", getSaveState());
    saveLoadState.save(saveState);
    //System.out.println("IndexState.commit name=" + name + " state=" + saveState.toJSONString(new JSONStyleIdent()));
  }

  /** Load all previously saved state. */
  public synchronized void load(JSONObject o) throws IOException {

    // To load, we invoke each handler from the save state,
    // as if the app had just done so from a fresh index,
    // except for suggesters which uses a dedicated load
    // method:

    try {
      // Global settings:
      JSONObject settingsState = (JSONObject) o.get("settings");
      //System.out.println("load: state=" + o);
      Request r = new Request(null, null, settingsState, SettingsHandler.TYPE);
      FinishRequest fr = ((SettingsHandler) globalState.getHandler("settings")).handle(this, r, null);
      fr.finish();
      assert !Request.anythingLeft(settingsState): settingsState.toString();

      JSONObject liveSettingsState = (JSONObject) o.get("settings");
      r = new Request(null, null, liveSettingsState, LiveSettingsHandler.TYPE);
      fr = ((LiveSettingsHandler) globalState.getHandler("liveSettings")).handle(this, r, null);
      fr.finish();
      assert !Request.anythingLeft(liveSettingsState): liveSettingsState.toString();

      // Field defs:
      JSONObject fieldsState = (JSONObject) o.get("fields");
      JSONObject top = new JSONObject();
      top.put("fields", fieldsState);
      r = new Request(null, null, top, RegisterFieldHandler.TYPE);
      fr = ((RegisterFieldHandler) globalState.getHandler("registerFields")).handle(this, r, null);
      assert !Request.anythingLeft(top): top;
      fr.finish();

      for(FieldDef fd : fields.values()) {
        if (fd.liveValuesIDField != null) {
          liveFieldValues.put(fd.name, new StringLiveFieldValues(manager, fd.liveValuesIDField, fd.name));
        }
      }

      // Suggesters:
      ((BuildSuggestHandler) globalState.getHandler("buildSuggest")).load(this, (JSONObject) o.get("suggest"));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /** Prunes stale searchers. */
  private class SearcherPruningThread extends Thread {
    private final CountDownLatch shutdownNow;

    /** Sole constructor. */
    public SearcherPruningThread(CountDownLatch shutdownNow) {
      this.shutdownNow = shutdownNow;
    }

    @Override
    public void run() {
      while(true) {
        try {
          final SearcherLifetimeManager.Pruner byAge = new SearcherLifetimeManager.PruneByAge(maxSearcherAgeSec);
          final Set<Long> snapshots = new HashSet<Long>(snapshotGenToVersion.values());
          slm.prune(new SearcherLifetimeManager.Pruner() {
              @Override
              public boolean doPrune(double ageSec, IndexSearcher searcher) {
                long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
                if (snapshots.contains(version)) {
                  // Never time-out searcher for a snapshot:
                  return false;
                } else {
                  return byAge.doPrune(ageSec, searcher);
                }
              }
            });
        } catch (IOException ioe) {
          // nocommit log
        }
        try {
          if (shutdownNow.await(1, TimeUnit.SECONDS)) {
            break;
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
        if (writer == null) {
          break;
        }
      }
    }
  }

  private synchronized double getDoubleSetting(String name) {
    Object o = settingsSaveState.get(name);
    if (o == null) {
      Param p = SettingsHandler.TYPE.params.get(name);
      assert p != null;
      o = p.defaultValue;
    }
    return ((Number) o).doubleValue();
  }

  private synchronized int getIntSetting(String name) {
    Object o = settingsSaveState.get(name);
    if (o == null) {
      Param p = SettingsHandler.TYPE.params.get(name);
      assert p != null;
      o = p.defaultValue;
    }
    return ((Number) o).intValue();
  }

  private synchronized boolean getBooleanSetting(String name) {
    Object o = settingsSaveState.get(name);
    if (o == null) {
      Param p = SettingsHandler.TYPE.params.get(name);
      assert p != null;
      o = p.defaultValue;
    }
    return (Boolean) o;
  }

  private void initSaveLoadState() throws IOException {
    File stateDirFile;
    if (rootDir != null) {
      stateDirFile = new File(rootDir, "state");
      //if (!stateDirFile.exists()) {
      //stateDirFile.mkdirs();
      //}
    } else {
      stateDirFile = null;
    }

    Directory stateDir = df.open(stateDirFile);

    saveLoadGenRefCounts = new SaveLoadRefCounts(stateDir);

    // Snapshot ref counts:
    genRefCounts = saveLoadGenRefCounts.load();
    if (genRefCounts == null) {
      genRefCounts = new HashMap<Long,Integer>();
    }

    saveLoadState = new SaveLoadState(stateDir);

    JSONObject priorState = saveLoadState.load();
    if (priorState != null) {
      load((JSONObject) priorState.get("state"));
    }
  }

  /** Start this index. */
  public synchronized void start() throws Exception {
    if (writer != null) {
      // throw new IllegalStateException("index \"" + name + "\" was already started");
      return;
    }

    //System.out.println("IndexState.start name=" + name);

    boolean success = false;

    try {

      if (saveLoadState == null) {
        initSaveLoadState();
      }

      File indexDirFile = new File(rootDir, "index");
      origIndexDir = df.open(indexDirFile);

      if (!(origIndexDir instanceof RAMDirectory)) {
        double maxMergeSizeMB = getDoubleSetting("nrtCachingDirectory.maxMergeSizeMB");
        double maxSizeMB = getDoubleSetting("nrtCachingDirectory.maxSizeMB");
        if (maxMergeSizeMB > 0 && maxSizeMB > 0) {
          indexDir = new NRTCachingDirectory(origIndexDir, maxMergeSizeMB, maxSizeMB);
        } else {
          indexDir = origIndexDir;
        }
      } else {
        indexDir = origIndexDir;
      }

      // Install rate limiting for merges:
      if (settingsSaveState.containsKey("mergeMaxMBPerSec")) {
        RateLimitedDirectoryWrapper rateLimitDir = new RateLimitedDirectoryWrapper(indexDir);
        rateLimitDir.setMaxWriteMBPerSec(getDoubleSetting("mergeMaxMBPerSec"), IOContext.Context.MERGE);
        indexDir = rateLimitDir;
      }

      // Rather than rely on IndexWriter/TaxonomyWriter to
      // figure out if an index is new or not by passing
      // CREATE_OR_APPEND (which can be dangerous), we
      // alyready know the intention from the app (whether
      // it called createIndex vs openIndex), so we make it
      // explicit here:
      IndexWriterConfig.OpenMode openMode;
      if (doCreate) {
        openMode = IndexWriterConfig.OpenMode.CREATE;
      } else {
        openMode = IndexWriterConfig.OpenMode.APPEND;
      }

      File taxoDirFile = new File(rootDir, "taxonomy");
      taxoDir = df.open(taxoDirFile);

      taxoSnapshots = new PersistentSnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy(),
                                                           taxoDir,
                                                           openMode);

      taxoWriter = new DirectoryTaxonomyWriter(taxoDir, openMode) {
          @Override
          protected IndexWriterConfig createIndexWriterConfig(OpenMode openMode) {
            IndexWriterConfig iwc = super.createIndexWriterConfig(openMode);
            iwc.setIndexDeletionPolicy(taxoSnapshots);
            return iwc;
          }

          @Override
          protected IndexWriter openIndexWriter(Directory dir, IndexWriterConfig iwc) throws IOException {
            IndexWriter w = super.openIndexWriter(dir, iwc);
            taxoInternalWriter = w;
            return w;
          }
        };

      // Must re-pull from IW because IW clones the IDP on init:
      taxoSnapshots = (PersistentSnapshotDeletionPolicy) taxoInternalWriter.getConfig().getIndexDeletionPolicy();

      IndexWriterConfig iwc = new IndexWriterConfig(matchVersion, indexAnalyzer);
      if (getBooleanSetting("index.verbose")) {
        iwc.setInfoStream(new PrintStreamInfoStream(System.out));
      }

      iwc.setSimilarity(sim);
      iwc.setOpenMode(openMode);
      iwc.setRAMBufferSizeMB(indexRAMBufferSizeMB);
      iwc.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(iwc.getInfoStream()));

      ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
      cms.setMaxMergesAndThreads(getIntSetting("concurrentMergeScheduler.maxMergeCount"),
                                 getIntSetting("concurrentMergeScheduler.maxThreadCount"));

      snapshots = new PersistentSnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy(),
                                                       origIndexDir,
                                                       IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

      iwc.setIndexDeletionPolicy(snapshots);

      iwc.setCodec(new ServerCodec(this));

      writer = new TrackingIndexWriter(new IndexWriter(indexDir, iwc));

      // Must re-pull from IW because IW clones the IDP on init:
      snapshots = (PersistentSnapshotDeletionPolicy) writer.getIndexWriter().getConfig().getIndexDeletionPolicy();

      // NOTE: must do this after writer, because SDP only
      // loads its commits after writer calls .onInit:
      for(IndexCommit c : snapshots.getSnapshots()) {
        long gen = c.getGeneration();
        SegmentInfos sis = new SegmentInfos();
        sis.read(origIndexDir, IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
        snapshotGenToVersion.put(c.getGeneration(), sis.getVersion());
      }

      // nocommit must also pull snapshots for taxoReader?

      // TODO: we could defer starting searcher until first search...
      manager = new SearcherTaxonomyManager(writer.getIndexWriter(), true, new SearcherFactory() {
          @Override
          public IndexSearcher newSearcher(IndexReader r) throws IOException {
            IndexSearcher searcher = new MyIndexSearcher(r, IndexState.this);
            searcher.setSimilarity(sim);
            return searcher;
          }
        }, taxoWriter);

      restartReopenThread();

      startSearcherPruningThread(globalState.shutdownNow);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(reopenThread,
                                            manager,
                                            writer == null ? null : writer.getIndexWriter(),
                                            taxoWriter,
                                            slm,
                                            indexDir,
                                            taxoDir);
        writer = null;
      }
    }
  }

  /** Start the searcher pruning thread. */
  public void startSearcherPruningThread(CountDownLatch shutdownNow) {
    searcherPruningThread = new SearcherPruningThread(shutdownNow);
    searcherPruningThread.setName("LuceneSearcherPruning-" + name);
    searcherPruningThread.start();
  }

  /** String -> UTF8 byte[]. */
  public static byte[] toUTF8(String s) {
    CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
    // Make sure we catch any invalid UTF16:
    encoder.onMalformedInput(CodingErrorAction.REPORT);
    encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      ByteBuffer bb = encoder.encode(CharBuffer.wrap(s));
      byte[] bytes = new byte[bb.limit()];
      bb.position(0);
      bb.get(bytes, 0, bytes.length);
      return bytes;
    } catch (CharacterCodingException cce) {
      throw new IllegalArgumentException(cce);
    }
  }

  /** UTF8 byte[] -> String. */
  public static String fromUTF8(byte[] bytes) {
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    // Make sure we catch any invalid UTF8:
    decoder.onMalformedInput(CodingErrorAction.REPORT);
    decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      return decoder.decode(ByteBuffer.wrap(bytes)).toString();
    } catch (CharacterCodingException cce) {
      throw new IllegalArgumentException(cce);
    }
  }

  /** UTF8 byte[], offset, length -> char[]. */
  public static char[] utf8ToCharArray(byte[] bytes, int offset, int length) {
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    // Make sure we catch any invalid UTF8:
    decoder.onMalformedInput(CodingErrorAction.REPORT);
    decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    CharBuffer buffer;
    try {
      buffer = decoder.decode(ByteBuffer.wrap(bytes, offset, length));
    } catch (CharacterCodingException cce) {
      throw new IllegalArgumentException(cce);
    }
    char[] arr = new char[buffer.remaining()];
    buffer.get(arr);
    return arr;
  }

  private final static Pattern reSimpleName = Pattern.compile("^[a-zA-Z_][a-zA-Z_0-9]*$");

  /** Verifies this name doesn't use any "exotic"
   *  characters. */
  public static boolean isSimpleName(String name) {
    return reSimpleName.matcher(name).matches();
  }

  /** Find the most recent generation in the directory for
   *  this prefix. */
  public static long getLastGen(File dir, String prefix) {
    assert isSimpleName(prefix);
    prefix += '.';
    long lastGen = -1;
    if (dir.exists()) {
      for(String subFile : dir.list()) {
        if (subFile.startsWith(prefix)) {
          lastGen = Math.max(lastGen, Long.parseLong(subFile.substring(prefix.length())));
        }
      }
    }

    return lastGen;
  }

  private static void deleteAllFiles(File dir) throws IOException {
    if (dir.exists()) {
      if (dir.isFile()) {
        if (!dir.delete()) {
          throw new IOException("could not delete " + dir);
        }
      } else {
        for (File f : dir.listFiles()) {
          if (f.isDirectory()) {
            deleteAllFiles(f);
          } else {
            if (!f.delete()) {
              throw new IOException("could not delete " + f);
            }
          }
        }
        if (!dir.delete()) {
          throw new IOException("could not delete " + dir);
        }
      }
    }
  }

  /** Delete this index. */
  public void deleteIndex() throws IOException {
    if (rootDir != null) {
      deleteAllFiles(rootDir);
    }
    globalState.deleteIndex(name);
  }

  /** Holds metadata for one snapshot, including its id, and
   *  the index, taxonomy and state generations. */
  public static class Gens {

    /** Index generation. */
    public final long indexGen;

    /** Taxonomy index generation. */
    public final long taxoGen;

    /** State generation. */
    public final long stateGen;

    /** Snapshot id. */
    public final String id;

    /** Sole constructor. */
    public Gens(Request r, String param) {
      id = r.getString(param);
      final String[] gens = id.split(":");
      if (gens.length != 3) {
        r.fail(param, "invalid snapshot id \"" + id + "\": must be format N:M:O");
      }
      long indexGen1 = -1;
      long taxoGen1 = -1;
      long stateGen1 = -1;
      try {
        indexGen1 = Long.parseLong(gens[0]);
        taxoGen1 = Long.parseLong(gens[1]);
        stateGen1 = Long.parseLong(gens[2]);
      } catch (Exception e) {
        r.fail(param, "invalid snapshot id \"" + id + "\": must be format N:M:O");
      }      
      indexGen = indexGen1;
      taxoGen = taxoGen1;
      stateGen = stateGen1;
    }
  }

  public void verifyStarted(Request r) {
    if (started() == false) {
      String message = "index '" + name + "' isn't started; call startIndex first";
      if (r == null) {
        throw new IllegalStateException(message);
      } else {
        r.fail("indexName", message);
      }
    }
  }
}
