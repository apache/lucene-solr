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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.HTMLParser;
import org.apache.lucene.benchmark.byTask.feeds.QueryMaker;
import org.apache.lucene.benchmark.byTask.stats.Points;
import org.apache.lucene.benchmark.byTask.tasks.ReadTask;
import org.apache.lucene.benchmark.byTask.tasks.SearchTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.FileUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;


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
  private HTMLParser htmlParser;
  
  // we use separate (identical) instances for each "read" task type, so each can iterate the quries separately.
  private HashMap readTaskQueryMaker;
  private Class qmkrClass;

  private IndexReader indexReader;
  private IndexWriter indexWriter;
  private Config config;
  private long startTimeMillis;
  
  // constructor
  public PerfRunData (Config config) throws Exception {
    this.config = config;
    // analyzer (default is standard analyzer)
    analyzer = (Analyzer) Class.forName(config.get("analyzer",
        "org.apache.lucene.analysis.standard.StandardAnalyzer")).newInstance();
    // doc maker
    docMaker = (DocMaker) Class.forName(config.get("doc.maker",
        "org.apache.lucene.benchmark.byTask.feeds.SimpleDocMaker")).newInstance();
    docMaker.setConfig(config);
    // query makers
    readTaskQueryMaker = new HashMap();
    qmkrClass = Class.forName(config.get("query.maker","org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker"));
    // html parser, used for some doc makers
    htmlParser = (HTMLParser) Class.forName(config.get("html.parser","org.apache.lucene.benchmark.byTask.feeds.DemoHTMLParser")).newInstance();
    docMaker.setHTMLParser(htmlParser);

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
      directory = FSDirectory.getDirectory(indexDir);
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
   * @return Returns the indexReader.
   */
  public IndexReader getIndexReader() {
    return indexReader;
  }

  /**
   * @param indexReader The indexReader to set.
   */
  public void setIndexReader(IndexReader indexReader) {
    this.indexReader = indexReader;
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

  /**
   * @return Returns the docMaker.
   */
  public DocMaker getDocMaker() {
    return docMaker;
  }

  /**
   * @return Returns the config.
   */
  public Config getConfig() {
    return config;
  }

  public void resetInputs() {
    docMaker.resetInputs();
    Iterator it = readTaskQueryMaker.values().iterator();
    while (it.hasNext()) {
      ((QueryMaker) it.next()).resetInputs();
    }
  }

  /**
   * @return Returns the queryMaker by read task type (class)
   */
  synchronized public QueryMaker getQueryMaker(ReadTask readTask) {
    // mapping the query maker by task class allows extending/adding new search/read tasks
    // without needing to modify this class.
    Class readTaskClass = readTask.getClass();
    QueryMaker qm = (QueryMaker) readTaskQueryMaker.get(readTaskClass);
    if (qm == null) {
      try {
        qm = (QueryMaker) qmkrClass.newInstance();
        qm.setConfig(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      readTaskQueryMaker.put(readTaskClass,qm);
    }
    return qm;
  }

  /**
   * @return Returns the htmlParser.
   */
  public HTMLParser getHtmlParser() {
    return htmlParser;
  }

}
