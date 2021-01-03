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
package org.apache.lucene.benchmark.byTask;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.FacetSource;
import org.apache.lucene.benchmark.byTask.feeds.QueryMaker;
import org.apache.lucene.benchmark.byTask.stats.Points;
import org.apache.lucene.benchmark.byTask.tasks.NewAnalyzerTask;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.tasks.ReadTask;
import org.apache.lucene.benchmark.byTask.tasks.SearchTask;
import org.apache.lucene.benchmark.byTask.utils.AnalyzerFactory;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/**
 * Data maintained by a performance test run.
 *
 * <p>Data includes:
 *
 * <ul>
 *   <li>Configuration.
 *   <li>Directory, Writer, Reader.
 *   <li>Taxonomy Directory, Writer, Reader.
 *   <li>DocMaker, FacetSource and a few instances of QueryMaker.
 *   <li>Named AnalysisFactories.
 *   <li>Analyzer.
 *   <li>Statistics data which updated during the run.
 * </ul>
 *
 * Config properties:
 *
 * <ul>
 *   <li><b>work.dir</b>=&lt;path to root of docs and index dirs| Default: work&gt;
 *   <li><b>analyzer</b>=&lt;class name for analyzer| Default: StandardAnalyzer&gt;
 *   <li><b>doc.maker</b>=&lt;class name for doc-maker| Default: DocMaker&gt;
 *   <li><b>facet.source</b>=&lt;class name for facet-source| Default: RandomFacetSource&gt;
 *   <li><b>query.maker</b>=&lt;class name for query-maker| Default: SimpleQueryMaker&gt;
 *   <li><b>log.queries</b>=&lt;whether queries should be printed| Default: false&gt;
 *   <li><b>directory</b>=&lt;type of directory to use for the index| Default:
 *       ByteBuffersDirectory&gt;
 *   <li><b>taxonomy.directory</b>=&lt;type of directory for taxonomy index| Default:
 *       ByteBuffersDirectory&gt;
 * </ul>
 */
public class PerfRunData implements Closeable {

  private static final String DEFAULT_DIRECTORY = "ByteBuffersDirectory";
  private Points points;

  // objects used during performance test run
  // directory, analyzer, docMaker - created at startup.
  // reader, writer, searcher - maintained by basic tasks.
  private Directory directory;
  private Map<String, AnalyzerFactory> analyzerFactories = new HashMap<>();
  private Analyzer analyzer;
  private DocMaker docMaker;
  private ContentSource contentSource;
  private FacetSource facetSource;
  private Locale locale;

  private Directory taxonomyDir;
  private TaxonomyWriter taxonomyWriter;
  private TaxonomyReader taxonomyReader;

  // we use separate (identical) instances for each "read" task type, so each can iterate the quries
  // separately.
  private HashMap<Class<? extends ReadTask>, QueryMaker> readTaskQueryMaker;
  private Class<? extends QueryMaker> qmkrClass;

  private DirectoryReader indexReader;
  private IndexSearcher indexSearcher;
  private IndexWriter indexWriter;
  private Config config;
  private long startTimeMillis;

  private final HashMap<String, Object> perfObjects = new HashMap<>();

  // constructor
  public PerfRunData(Config config) throws Exception {
    this.config = config;
    // analyzer (default is standard analyzer)
    analyzer =
        NewAnalyzerTask.createAnalyzer(
            config.get("analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer"));

    // content source
    String sourceClass =
        config.get("content.source", "org.apache.lucene.benchmark.byTask.feeds.SingleDocSource");
    contentSource =
        Class.forName(sourceClass).asSubclass(ContentSource.class).getConstructor().newInstance();
    contentSource.setConfig(config);

    // doc maker
    docMaker =
        Class.forName(config.get("doc.maker", "org.apache.lucene.benchmark.byTask.feeds.DocMaker"))
            .asSubclass(DocMaker.class)
            .getConstructor()
            .newInstance();
    docMaker.setConfig(config, contentSource);
    // facet source
    facetSource =
        Class.forName(
                config.get(
                    "facet.source", "org.apache.lucene.benchmark.byTask.feeds.RandomFacetSource"))
            .asSubclass(FacetSource.class)
            .getConstructor()
            .newInstance();
    facetSource.setConfig(config);
    // query makers
    readTaskQueryMaker = new HashMap<>();
    qmkrClass =
        Class.forName(
                config.get(
                    "query.maker", "org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker"))
            .asSubclass(QueryMaker.class);

    // index stuff
    reinit(false);

    // statistic points
    points = new Points(config);

    if (Boolean.valueOf(config.get("log.queries", "false")).booleanValue()) {
      System.out.println("------------> queries:");
      System.out.println(getQueryMaker(new SearchTask(this)).printQueries());
    }
  }

  @Override
  public void close() throws IOException {
    if (indexWriter != null) {
      indexWriter.close();
    }
    IOUtils.close(
        indexReader,
        directory,
        taxonomyWriter,
        taxonomyReader,
        taxonomyDir,
        docMaker,
        facetSource,
        contentSource);

    // close all perf objects that are closeable.
    ArrayList<Closeable> perfObjectsToClose = new ArrayList<>();
    for (Object obj : perfObjects.values()) {
      if (obj instanceof Closeable) {
        perfObjectsToClose.add((Closeable) obj);
      }
    }
    IOUtils.close(perfObjectsToClose);
  }

  // clean old stuff, reopen
  public void reinit(boolean eraseIndex) throws Exception {

    // cleanup index
    if (indexWriter != null) {
      indexWriter.close();
    }
    IOUtils.close(indexReader, directory);
    indexWriter = null;
    indexReader = null;

    IOUtils.close(taxonomyWriter, taxonomyReader, taxonomyDir);
    taxonomyWriter = null;
    taxonomyReader = null;

    // directory (default is ram-dir).
    directory = createDirectory(eraseIndex, "index", "directory");
    taxonomyDir = createDirectory(eraseIndex, "taxo", "taxonomy.directory");

    // inputs
    resetInputs();

    // release unused stuff
    System.runFinalization();
    System.gc();

    // Re-init clock
    setStartTimeMillis();
  }

  private Directory createDirectory(boolean eraseIndex, String dirName, String dirParam)
      throws IOException {
    String dirImpl = config.get(dirParam, DEFAULT_DIRECTORY);
    if ("FSDirectory".equals(dirImpl)) {
      Path workDir = Paths.get(config.get("work.dir", "work"));
      Path indexDir = workDir.resolve(dirName);
      if (eraseIndex && Files.exists(indexDir)) {
        IOUtils.rm(indexDir);
      }
      Files.createDirectories(indexDir);
      return FSDirectory.open(indexDir);
    }

    if ("RAMDirectory".equals(dirImpl)) {
      throw new IOException("RAMDirectory has been removed, use ByteBuffersDirectory.");
    }

    if ("ByteBuffersDirectory".equals(dirImpl)) {
      return new ByteBuffersDirectory();
    }

    throw new IOException("Directory type not supported: " + dirImpl);
  }

  /** Returns an object that was previously set by {@link #setPerfObject(String, Object)}. */
  public synchronized Object getPerfObject(String key) {
    return perfObjects.get(key);
  }

  /**
   * Sets an object that is required by {@link PerfTask}s, keyed by the given {@code key}. If the
   * object implements {@link Closeable}, it will be closed by {@link #close()}.
   */
  public synchronized void setPerfObject(String key, Object obj) {
    perfObjects.put(key, obj);
  }

  public long setStartTimeMillis() {
    startTimeMillis = System.currentTimeMillis();
    return startTimeMillis;
  }

  /** @return Start time in milliseconds */
  public long getStartTimeMillis() {
    return startTimeMillis;
  }

  /** @return Returns the points. */
  public Points getPoints() {
    return points;
  }

  /** @return Returns the directory. */
  public Directory getDirectory() {
    return directory;
  }

  /** @param directory The directory to set. */
  public void setDirectory(Directory directory) {
    this.directory = directory;
  }

  /** @return Returns the taxonomy directory */
  public Directory getTaxonomyDir() {
    return taxonomyDir;
  }

  /**
   * Set the taxonomy reader. Takes ownership of that taxonomy reader, that is, internally performs
   * taxoReader.incRef() (If caller no longer needs that reader it should decRef()/close() it after
   * calling this method, otherwise, the reader will remain open).
   *
   * @param taxoReader The taxonomy reader to set.
   */
  public synchronized void setTaxonomyReader(TaxonomyReader taxoReader) throws IOException {
    if (taxoReader == this.taxonomyReader) {
      return;
    }
    if (taxonomyReader != null) {
      taxonomyReader.decRef();
    }

    if (taxoReader != null) {
      taxoReader.incRef();
    }
    this.taxonomyReader = taxoReader;
  }

  /**
   * @return Returns the taxonomyReader. NOTE: this returns a reference. You must call
   *     TaxonomyReader.decRef() when you're done.
   */
  public synchronized TaxonomyReader getTaxonomyReader() {
    if (taxonomyReader != null) {
      taxonomyReader.incRef();
    }
    return taxonomyReader;
  }

  /** @param taxoWriter The taxonomy writer to set. */
  public void setTaxonomyWriter(TaxonomyWriter taxoWriter) {
    this.taxonomyWriter = taxoWriter;
  }

  public TaxonomyWriter getTaxonomyWriter() {
    return taxonomyWriter;
  }

  /**
   * @return Returns the indexReader. NOTE: this returns a reference. You must call
   *     IndexReader.decRef() when you're done.
   */
  public synchronized DirectoryReader getIndexReader() {
    if (indexReader != null) {
      indexReader.incRef();
    }
    return indexReader;
  }

  /**
   * @return Returns the indexSearcher. NOTE: this returns a reference to the underlying
   *     IndexReader. You must call IndexReader.decRef() when you're done.
   */
  public synchronized IndexSearcher getIndexSearcher() {
    if (indexReader != null) {
      indexReader.incRef();
    }
    return indexSearcher;
  }

  /**
   * Set the index reader. Takes ownership of that index reader, that is, internally performs
   * indexReader.incRef() (If caller no longer needs that reader it should decRef()/close() it after
   * calling this method, otherwise, the reader will remain open).
   *
   * @param indexReader The indexReader to set.
   */
  public synchronized void setIndexReader(DirectoryReader indexReader) throws IOException {
    if (indexReader == this.indexReader) {
      return;
    }

    if (this.indexReader != null) {
      // Release current IR
      this.indexReader.decRef();
    }

    this.indexReader = indexReader;
    if (indexReader != null) {
      // Hold reference to new IR
      indexReader.incRef();
      indexSearcher = new IndexSearcher(indexReader);
      // TODO Some day we should make the query cache in this module configurable and control
      // clearing the cache
      indexSearcher.setQueryCache(null);
    } else {
      indexSearcher = null;
    }
  }

  /** @return Returns the indexWriter. */
  public IndexWriter getIndexWriter() {
    return indexWriter;
  }

  /** @param indexWriter The indexWriter to set. */
  public void setIndexWriter(IndexWriter indexWriter) {
    this.indexWriter = indexWriter;
  }

  /** @return Returns the analyzer. */
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  /** Returns the ContentSource. */
  public ContentSource getContentSource() {
    return contentSource;
  }

  /** Returns the DocMaker. */
  public DocMaker getDocMaker() {
    return docMaker;
  }

  /** Returns the facet source. */
  public FacetSource getFacetSource() {
    return facetSource;
  }

  /** @return the locale */
  public Locale getLocale() {
    return locale;
  }

  /** @param locale the locale to set */
  public void setLocale(Locale locale) {
    this.locale = locale;
  }

  /** @return Returns the config. */
  public Config getConfig() {
    return config;
  }

  public void resetInputs() throws IOException {
    contentSource.resetInputs();
    docMaker.resetInputs();
    facetSource.resetInputs();
    for (final QueryMaker queryMaker : readTaskQueryMaker.values()) {
      try {
        queryMaker.resetInputs();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** @return Returns the queryMaker by read task type (class) */
  public synchronized QueryMaker getQueryMaker(ReadTask readTask) {
    // mapping the query maker by task class allows extending/adding new search/read tasks
    // without needing to modify this class.
    Class<? extends ReadTask> readTaskClass = readTask.getClass();
    QueryMaker qm = readTaskQueryMaker.get(readTaskClass);
    if (qm == null) {
      try {
        qm = qmkrClass.getConstructor().newInstance();
        qm.setConfig(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      readTaskQueryMaker.put(readTaskClass, qm);
    }
    return qm;
  }

  public Map<String, AnalyzerFactory> getAnalyzerFactories() {
    return analyzerFactories;
  }
}
