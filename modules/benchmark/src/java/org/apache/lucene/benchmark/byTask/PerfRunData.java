package org.apache.lucene.benchmark.byTask;

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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.QueryMaker;
import org.apache.lucene.benchmark.byTask.stats.Points;
import org.apache.lucene.benchmark.byTask.tasks.ReadTask;
import org.apache.lucene.benchmark.byTask.tasks.SearchTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.FileUtils;
import org.apache.lucene.benchmark.byTask.tasks.NewAnalyzerTask;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

/**
 * Data maintained by a performance test run.
 * <p>
 * Data includes:
 * <ul>
 *  <li>Configuration.
 *  <li>Directory, Writer, Reader.
 *  <li>Docmaker and a few instances of QueryMaker.
 *  <li>Analyzer.
 *  <li>Statistics data which updated during the run.
 * </ul>
 * Config properties: work.dir=&lt;path to root of docs and index dirs| Default: work&gt;
 * </ul>
 */
public class PerfRunData {

  private Points points;
  
  // objects used during performance test run
  // directory, analyzer, docMaker - created at startup.
  // reader, writer, searcher - maintained by basic tasks. 
  private Directory directory;
  private Analyzer analyzer;
  private DocMaker docMaker;
  private Locale locale;
  
  // we use separate (identical) instances for each "read" task type, so each can iterate the quries separately.
  private HashMap<Class<? extends ReadTask>,QueryMaker> readTaskQueryMaker;
  private Class<? extends QueryMaker> qmkrClass;

  private IndexReader indexReader;
  private IndexSearcher indexSearcher;
  private IndexWriter indexWriter;
  private Config config;
  private long startTimeMillis;
  
  // constructor
  public PerfRunData (Config config) throws Exception {
    this.config = config;
    // analyzer (default is standard analyzer)
    analyzer = NewAnalyzerTask.createAnalyzer(config.get("analyzer",
        "org.apache.lucene.analysis.standard.StandardAnalyzer"));
    // doc maker
    docMaker = Class.forName(config.get("doc.maker",
        "org.apache.lucene.benchmark.byTask.feeds.DocMaker")).asSubclass(DocMaker.class).newInstance();
    docMaker.setConfig(config);
    // query makers
    readTaskQueryMaker = new HashMap<Class<? extends ReadTask>,QueryMaker>();
    qmkrClass = Class.forName(config.get("query.maker","org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker")).asSubclass(QueryMaker.class);

    // index stuff
    reinit(false);
    
    // statistic points
    points = new Points(config);
    
    if (Boolean.valueOf(config.get("log.queries","false")).booleanValue()) {
      System.out.println("------------> queries:");
      System.out.println(getQueryMaker(new SearchTask(this)).printQueries());
    }
  }

  // clean old stuff, reopen 
  public void reinit(boolean eraseIndex) throws Exception {

    // cleanup index
    if (indexWriter!=null) {
      indexWriter.close();
      indexWriter = null;
    }
    if (indexReader!=null) {
      indexReader.close();
      indexReader = null;
    }
    if (directory!=null) {
      directory.close();
    }
    
    // directory (default is ram-dir).
    if ("FSDirectory".equals(config.get("directory","RAMDirectory"))) {
      File workDir = new File(config.get("work.dir","work"));
      File indexDir = new File(workDir,"index");
      if (eraseIndex && indexDir.exists()) {
        FileUtils.fullyDelete(indexDir);
      }
      indexDir.mkdirs();
      directory = FSDirectory.open(indexDir);
    } else {
      directory = new RAMDirectory();
    }

    // inputs
    resetInputs();
    
    // release unused stuff
    System.runFinalization();
    System.gc();

    // Re-init clock
    setStartTimeMillis();
  }
  
  public long setStartTimeMillis() {
    startTimeMillis = System.currentTimeMillis();
    return startTimeMillis;
  }

  /**
   * @return Start time in milliseconds
   */
  public long getStartTimeMillis() {
    return startTimeMillis;
  }

  /**
   * @return Returns the points.
   */
  public Points getPoints() {
    return points;
  }

  /**
   * @return Returns the directory.
   */
  public Directory getDirectory() {
    return directory;
  }

  /**
   * @param directory The directory to set.
   */
  public void setDirectory(Directory directory) {
    this.directory = directory;
  }

  /**
   * @return Returns the indexReader.  NOTE: this returns a
   * reference.  You must call IndexReader.decRef() when
   * you're done.
   */
  public synchronized IndexReader getIndexReader() {
    if (indexReader != null) {
      indexReader.incRef();
    }
    return indexReader;
  }

  /**
   * @return Returns the indexSearcher.  NOTE: this returns
   * a reference to the underlying IndexReader.  You must
   * call IndexReader.decRef() when you're done.
   */
  public synchronized IndexSearcher getIndexSearcher() {
    if (indexReader != null) {
      indexReader.incRef();
    }
    return indexSearcher;
  }

  /**
   * @param indexReader The indexReader to set.
   */
  public synchronized void setIndexReader(IndexReader indexReader) throws IOException {
    if (this.indexReader != null) {
      // Release current IR
      this.indexReader.decRef();
    }
    this.indexReader = indexReader;
    if (indexReader != null) {
      // Hold reference to new IR
      indexReader.incRef();
      indexSearcher = new IndexSearcher(indexReader);
    } else {
      indexSearcher = null;
    }
  }

  /**
   * @return Returns the indexWriter.
   */
  public IndexWriter getIndexWriter() {
    return indexWriter;
  }

  /**
   * @param indexWriter The indexWriter to set.
   */
  public void setIndexWriter(IndexWriter indexWriter) {
    this.indexWriter = indexWriter;
  }

  /**
   * @return Returns the anlyzer.
   */
  public Analyzer getAnalyzer() {
    return analyzer;
  }


  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  /** Returns the docMaker. */
  public DocMaker getDocMaker() {
    return docMaker;
  }

  /**
   * @return the locale
   */
  public Locale getLocale() {
    return locale;
  }

  /**
   * @param locale the locale to set
   */
  public void setLocale(Locale locale) {
    this.locale = locale;
  }

  /**
   * @return Returns the config.
   */
  public Config getConfig() {
    return config;
  }

  public void resetInputs() throws IOException {
    docMaker.resetInputs();
    for (final QueryMaker queryMaker : readTaskQueryMaker.values()) {
      queryMaker.resetInputs();
    }
  }

  /**
   * @return Returns the queryMaker by read task type (class)
   */
  synchronized public QueryMaker getQueryMaker(ReadTask readTask) {
    // mapping the query maker by task class allows extending/adding new search/read tasks
    // without needing to modify this class.
    Class<? extends ReadTask> readTaskClass = readTask.getClass();
    QueryMaker qm = readTaskQueryMaker.get(readTaskClass);
    if (qm == null) {
      try {
        qm = qmkrClass.newInstance();
        qm.setConfig(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      readTaskQueryMaker.put(readTaskClass,qm);
    }
    return qm;
  }

}
