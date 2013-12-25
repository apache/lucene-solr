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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.search.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.facet.search.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyleIdent;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

/** Holds all state associated with one index. */

public class IndexState implements Closeable {
  // nocommit settingsHandler should set this
  public Version matchVersion = Version.LUCENE_46;

  /** Creates directories */
  DirectoryFactory df = DirectoryFactory.get("FSDirectory");

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

  public final Map<String,StringLiveFieldValues> liveFieldValues = new ConcurrentHashMap<String,StringLiveFieldValues>();

  /** Maps gen -> version */
  public final Map<Long,Long> snapshotGenToVersion = new ConcurrentHashMap<Long,Long>();

  /** Built suggest implementations */
  public final Map<String,Lookup> suggesters = new ConcurrentHashMap<String,Lookup>();

  public final Map<String,CachingWrapperFilter> cachedFilters = new ConcurrentHashMap<String,CachingWrapperFilter>();

  /** Holds pending save state, written to state.N file on
   *  commit. */
  // nocommit move these to their own obj, make it sync'd,
  // instead of syncing on IndexState instance:
  final JSONObject settingsSaveState = new JSONObject();
  final JSONObject liveSettingsSaveState = new JSONObject();
  final JSONObject suggestSaveState = new JSONObject();
  final JSONObject fieldsSaveState = new JSONObject();

  /** The schema (registerField) */
  private final Map<String,FieldDef> fields = new ConcurrentHashMap<String,FieldDef>();

  // TODO: need per-field control:
  CategoryListParams clp = new CategoryListParams("$facets") {
      @Override
      public OrdinalPolicy getOrdinalPolicy(String fieldName) {
        FieldDef fd = getField(fieldName);
        if (fd.faceted.equals("no")) {
          assert false;
          return null;
        } else if (fd.faceted.equals("flat")) {
          // Don't do NO_PARENTS, so we avoid the
          // [pointless] rollup cost:
          return OrdinalPolicy.ALL_BUT_DIMENSION;
        } else if (fd.singleValued) {
          // Index only the exact ord, and do rollup in the
          // end:
          return OrdinalPolicy.NO_PARENTS;
        } else {
          // Index all parents, de-duping ancestor ords at
          // indexing time:
          return OrdinalPolicy.ALL_PARENTS;
        }
      }
    };

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
  final SaveLoadRefCounts saveLoadGenRefCounts;

  final SaveLoadState saveLoadState;

  public FacetIndexingParams facetIndexingParams = new FacetIndexingParams(clp);

  /** Enables lookup of previously used searchers, so
   *  follow-on actions (next page, drill down/sideways/up,
   *  etc.) use the same searcher as the original search, as
   *  long as that searcher hasn't expired. */
  public final SearcherLifetimeManager slm = new SearcherLifetimeManager();

  /** Indexes changes, and provides the live searcher,
   *  possibly searching a specific generation. */
  public SearcherTaxonomyManager manager;
  public ControlledRealTimeReopenThread<SearcherAndTaxonomy> reopenThread;

  /** Periodically wakes up and prunes old searchers from
   *  slm. */
  Thread searcherPruningThread;

  /** Per-field wrapper that provides the analyzer
   *  configured in the FieldDef */

  private static final Analyzer keywordAnalyzer = new KeywordAnalyzer();

  public final Analyzer indexAnalyzer = new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
        @Override
        public Analyzer getWrappedAnalyzer(String name) {
          // nocommit hackish:
          if (name.equals("$facets")) {
            return RegisterFieldHandler.dummyAnalyzer;
          }
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

  public final Analyzer searchAnalyzer = new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
        @Override
        public Analyzer getWrappedAnalyzer(String name) {
          // nocommit hackish:
          if (name.equals("$facets")) {
            return RegisterFieldHandler.dummyAnalyzer;
          }
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
        // nocommit hacky that i have to do this!!
        if (name.equals("$facets")) {
          return defaultSim;
        }
        return getField(name).sim;
      }
    };

  /** When a search is waiting on a specific generation, we
   *  will wait at most this many seconds before reopening */
  volatile double minRefreshSec = .05f;

  /** When no searcher is waiting on a specific generation, we
   *  will wait at most this many seconds before proactively
   * reopening */
  volatile double maxRefreshSec = 1.0f;

  /** Once a searcher becomes stale (i.e., a new searcher is
   *  opened), we will close it after this many seconds.  If
   *  this is too small, it means that old searches returning
   *  for a follow-on query may find their searcher pruned
   *  (lease expired). */
  volatile double maxSearcherAgeSec = 60;

  volatile double indexRAMBufferSizeMB = 16;

  /** Holds the persistent snapshots */
  public PersistentSnapshotDeletionPolicy snapshots;

  /** Holds the persistent taxonomy snapshots */
  public PersistentSnapshotDeletionPolicy taxoSnapshots;

  public final String name;

  private final boolean isNew;

  public IndexState(GlobalState globalState, String name, File rootDir) throws Exception {
    this.globalState = globalState;
    this.name = name;
    this.rootDir = rootDir;

    File stateDirFile = new File(rootDir, "state");
    if (!stateDirFile.exists()) {
      stateDirFile.mkdirs();
      isNew = true;
    } else {
      isNew = false;
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

  public static class AddDocumentContext {
    public final AtomicInteger addCount = new AtomicInteger();
    public final List<String> errors = new ArrayList<String>();
    public final List<Integer> errorIndex = new ArrayList<Integer>();

    public synchronized void addException(int index, Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      pw.flush();
      errors.add(sw.toString());
      errorIndex.add(index);
    }
  }

  class AddDocumentJob implements Callable<Long> {
    private final Term updateTerm;
    private final DocumentAndFacets doc;
    private final AddDocumentContext ctx;

    // Position of this document in the bulk request:
    private final int index;

    public AddDocumentJob(int index, Term updateTerm, DocumentAndFacets doc, AddDocumentContext ctx) {
      this.updateTerm = updateTerm;
      this.doc = doc;
      this.ctx = ctx;
      this.index = index;
    }

    @Override
    public Long call() throws Exception {
      long gen = -1;

      try {
        if (doc.facets != null && !doc.facets.isEmpty()) {
          FacetFields ff = new FacetFields(taxoWriter, facetIndexingParams);
          ff.addFields(doc.doc, doc.facets);
        }
        if (updateTerm == null) {
          gen = writer.addDocument(doc.doc);
        } else {
          gen = writer.updateDocument(updateTerm, doc.doc);
        }

        if (!liveFieldValues.isEmpty()) {
          // TODO: wasteful!
          for(StorableField f : doc.doc.getFields()) {
            FieldDef fd = getField(f.name());
            if (fd.liveValuesIDField != null) {
              // TODO: O(N):
              String id = doc.doc.get(fd.liveValuesIDField);
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

  public FieldDef getField(String fieldName) {
    FieldDef fd = fields.get(fieldName);
    if (fd == null) {
      String message = "field \"" + fieldName + "\" is unknown: it was not registered with registerField";
      throw new IllegalArgumentException(message);
    }
    return fd;
  }

  public FieldDef getField(Request r, String paramName) {
    String fieldName = r.getString(paramName);
    FieldDef fd = fields.get(fieldName);
    if (fd == null) {
      String message = "field \"" + fieldName + "\" is unknown: it was not registered with registerField";
      r.fail(paramName, message);
    }

    return fd;
  }

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

  public boolean hasCommit() throws IOException {
    return saveLoadState.getNextWriteGen() != 0;
  }

  /** Record that this snapshot id refers to the current
   *  generation, returning it. */
  public long incRefLastCommitGen() throws IOException {
    long result;
    synchronized(saveLoadState) {
      long nextGen = saveLoadState.getNextWriteGen();
      //System.out.println("state nextGen=" + nextGen);
      if (nextGen == 0) {
        throw new IllegalStateException("no commit exists");
      }
      result = nextGen-1;
      if (result != -1) {
        incRef(result);
      }
    }

    return result;
  }

  private void incRef(long stateGen) throws IOException {
    synchronized(saveLoadGenRefCounts) {
      Integer rc = genRefCounts.get(stateGen);
      if (rc == null) {
        genRefCounts.put(stateGen, 1);
      } else {
        genRefCounts.put(stateGen, 1+rc.intValue());
      }
      saveLoadGenRefCounts.save(genRefCounts);
    }
  }

  /** Drop this snapshot from the references. */
  public void decRef(long stateGen) throws IOException {
    synchronized(saveLoadGenRefCounts) {
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
  }

  public boolean hasRef(long gen) {
    synchronized(genRefCounts) {
      Integer rc = genRefCounts.get(gen);
      if (rc == null) {
        return false;
      } else {
        assert rc.intValue() > 0;
        return true;
      }
    }
  }

  public void addField(FieldDef fd, JSONObject json) {
    synchronized(fields) {
      if (fields.containsKey(fd.name)) {
        throw new IllegalArgumentException("field \"" + fd.name + "\" was already registered");
      }
      fields.put(fd.name, fd);
      if (fd.liveValuesIDField != null) {
        liveFieldValues.put(fd.name, new StringLiveFieldValues(manager, fd.liveValuesIDField, fd.name));
      }
      assert !fieldsSaveState.containsKey(fd.name);
      fieldsSaveState.put(fd.name, json);
    }
  }

  public String getAllFieldsJSON() {
    synchronized(fieldsSaveState) {
      return fieldsSaveState.toString();
    }
  }

  public String getSettingsJSON() {
    synchronized(settingsSaveState) {
      return settingsSaveState.toString();
    }
  }

  public String getLiveSettingsJSON() {
    synchronized(liveSettingsSaveState) {
      return liveSettingsSaveState.toString();
    }
  }

  public void addSuggest(String name, JSONObject o) {
    synchronized(suggestSaveState) {
      suggestSaveState.put(name, o);
    }
  }

  class AddDocumentsJob implements Callable<Long> {
    private final Term updateTerm;
    private final Iterable<DocumentAndFacets> docs;
    private final AddDocumentContext ctx;

    // Position of this document in the bulk request:
    private final int index;

    public AddDocumentsJob(int index, Term updateTerm, Iterable<DocumentAndFacets> docs, AddDocumentContext ctx) {
      this.updateTerm = updateTerm;
      this.docs = docs;
      this.ctx = ctx;
      this.index = index;
    }

    @Override
    public Long call() throws Exception {
      long gen = -1;
      try {
        List<Document> justDocs = new ArrayList<Document>();
        for(DocumentAndFacets daf : docs) {
          if (daf.facets != null && !daf.facets.isEmpty()) {
            FacetFields ff = new FacetFields(taxoWriter, facetIndexingParams);
            ff.addFields(daf.doc, daf.facets);
          }
          justDocs.add(daf.doc);
        }

        //System.out.println(Thread.currentThread().getName() + ": add; " + docs);
        if (updateTerm == null) {
          gen = writer.addDocuments(justDocs);
        } else {
          gen = writer.updateDocuments(updateTerm, justDocs);
        }

        if (!liveFieldValues.isEmpty()) {
          for(Document doc : justDocs) {
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

  public static class DocumentAndFacets {
    public final Document doc = new Document();
    public List<CategoryPath> facets;
  }

  public Callable<Long> getAddDocumentJob(int index, Term term, DocumentAndFacets doc, AddDocumentContext ctx) {
    return new AddDocumentJob(index, term, doc, ctx);
  }

  public Callable<Long> getAddDocumentsJob(int index, Term term, Iterable<DocumentAndFacets> docs, AddDocumentContext ctx) {
    return new AddDocumentsJob(index, term, docs, ctx);
  }

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
    // nocommit catch exc & rollback:
    if (writer != null) {
      commit();
      IOUtils.close(reopenThread,
                    manager,
                    writer.getIndexWriter(),
                    taxoWriter,
                    slm,
                    indexDir,
                    taxoDir);
      writer = null;
      // nocommit this is dangerous .. eg Server iterates
      // all IS.indices and closes ...:
      synchronized(globalState.indices) {
        globalState.indices.remove(name);
      }
    }
  }

  public boolean started() {
    return writer != null;
  }

  public void setMinRefreshSec(double min) {
    minRefreshSec = min;
    synchronized(liveSettingsSaveState) {
      liveSettingsSaveState.put("minRefreshSec", min);
    }
    restartReopenThread();
  }

  public void setMaxRefreshSec(double max) {
    maxRefreshSec = max;
    synchronized(liveSettingsSaveState) {
      liveSettingsSaveState.put("maxRefreshSec", max);
    }
    restartReopenThread();
  }

  public void setMaxSearcherAgeSec(double d) {
    maxSearcherAgeSec = d;
    synchronized(liveSettingsSaveState) {
      liveSettingsSaveState.put("maxSearcherAgeSec", d);
    }
  }

  public void setIndexRAMBufferSizeMB(double d) {
    indexRAMBufferSizeMB = d;
    synchronized(liveSettingsSaveState) {
      liveSettingsSaveState.put("indexRAMBufferSizeMB", d);
    }
    if (writer != null) {
      writer.getIndexWriter().getConfig().setRAMBufferSizeMB(d);
    }
  }

  public void setDirectoryFactory(DirectoryFactory df, Object saveState) {
    synchronized(settingsSaveState) {
      if (writer != null) {
        throw new IllegalStateException("index \"" + name + "\": cannot change Directory when the index is running");
      }
      settingsSaveState.put("directory", saveState);
      this.df = df;
      // nocommit run through & verify no other indices have
      // this same rootDir!?  or share a prefix between the
      // two!?
    }
  }
  
  public synchronized JSONObject getSaveState() throws IOException {
    JSONObject o = new JSONObject();
    o.put("settings", settingsSaveState);
    o.put("liveSettings", liveSettingsSaveState);
    o.put("fields", fieldsSaveState);
    o.put("suggest", suggestSaveState);
    return o;
  }

  public synchronized void commit() throws IOException {
    if (writer == null) {
      throw new IllegalStateException("index \"" + name + "\" isn't started: cannot commit");
    }
    // hmm: two phase commit?
    writer.getIndexWriter().commit();
    taxoWriter.commit();

    JSONObject saveState = new JSONObject();
    saveState.put("state", getSaveState());
    saveLoadState.save(saveState);
    //System.out.println("DONE saveLoadState.save: " + Arrays.toString(new File(rootDir, "state").listFiles()));
  }

  public synchronized void load(JSONObject o) throws Exception {
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
  }

  private class SearcherPruningThread extends Thread {
    private final CountDownLatch shutdownNow;

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

  public synchronized void start() throws IOException {
    if (writer != null) {
      // throw new IllegalStateException("index \"" + name + "\" was already started");
      return;
    }

    boolean success = false;

    try {

      File indexDirFile = new File(rootDir, "index");
      if (isNew) {
        indexDirFile.mkdirs();
      }
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

      File taxoDirFile = new File(rootDir, "taxonomy");
      if (isNew) {
        taxoDirFile.mkdirs();
      }
      taxoDir = df.open(taxoDirFile);

      taxoSnapshots = new PersistentSnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy(),
                                                           taxoDir,
                                                           IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

      taxoWriter = new DirectoryTaxonomyWriter(taxoDir) {
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
      iwc.setOpenMode(isNew ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
      iwc.setRAMBufferSizeMB(indexRAMBufferSizeMB);
      iwc.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(iwc.getInfoStream()));

      ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
      cms.setMaxMergesAndThreads(getIntSetting("concurrentMergeScheduler.maxMergeCount"),
                                 getIntSetting("concurrentMergeScheduler.maxThreadCount"));

      snapshots = new PersistentSnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy(),
                                                       origIndexDir,
                                                       IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

      iwc.setIndexDeletionPolicy(snapshots);

      iwc.setCodec(new Lucene46Codec() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            String pf;
            if (field.equals("$facets")) {
              // nocommit we have to do this because $facets
              // is a "fake" field ... which is sort of
              // weird; we need to make this (which PF is
              // used for this "fake" field) controllable;
              // prolly we need to make it a "real" field
              pf = "Lucene41";
            } else {
              pf = getField(field).postingsFormat;
            }
            return PostingsFormat.forName(pf);
          }

          @Override
          public DocValuesFormat getDocValuesFormatForField(String field) {
            String dvf;
            if (field.equals("$facets")) {
              dvf = "Lucene45";
              //dvf = "Direct";
            } else {
              dvf = getField(field).docValuesFormat;
            }
            return DocValuesFormat.forName(dvf);
          }
        });

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
            IndexSearcher searcher = new MyIndexSearcher(r);
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

  public void startSearcherPruningThread(CountDownLatch shutdownNow) {
    searcherPruningThread = new SearcherPruningThread(shutdownNow);
    searcherPruningThread.setName("LuceneSearcherPruning-" + name);
    searcherPruningThread.start();
  }

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

  public static boolean isSimpleName(String name) {
    return reSimpleName.matcher(name).matches();
  }

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

  public void deleteIndex() throws IOException {
    deleteAllFiles(rootDir);
    globalState.deleteIndex(name);
  }

  public static class Gens {
    public final long indexGen;
    public final long taxoGen;
    public final long stateGen;
    public final String id;

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
}
