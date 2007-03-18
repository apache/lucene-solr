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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.QueryMaker;
import org.apache.lucene.benchmark.byTask.stats.Points;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.FileUtils;


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
 */
public class PerfRunData {

  private Points points;
  
  // objects used during performance test run
  // directory, analyzer, docMaker - created at startup.
  // reader, writer, searcher - maintained by basic tasks. 
  private Directory directory;
  private Analyzer analyzer;
  private DocMaker docMaker;
  private QueryMaker searchQueryMaker;
  private QueryMaker searchTravQueryMaker;
  private QueryMaker searchTravRetQueryMaker;

  private IndexReader indexReader;
  private IndexWriter indexWriter;
  private Config config;
  
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
    // we use separate (identical) instances for each "read" task type, so each can iterate the quries separately.
    Class qmkrClass = Class.forName(config.get("query.maker","org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker"));
    searchQueryMaker = (QueryMaker) qmkrClass.newInstance();
    searchQueryMaker.setConfig(config);
    searchTravQueryMaker = (QueryMaker) qmkrClass.newInstance();
    searchTravQueryMaker.setConfig(config);
    searchTravRetQueryMaker = (QueryMaker) qmkrClass.newInstance();
    searchTravRetQueryMaker.setConfig(config);
    // index stuff
    reinit(false);
    
    // statistic points
    points = new Points(config);
    
    if (Boolean.valueOf(config.get("log.queries","false")).booleanValue()) {
      System.out.println("------------> queries:");
      System.out.println(getSearchQueryMaker().printQueries());
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
      File workDir = new File("work");
      File indexDir = new File(workDir,"index");
      if (eraseIndex && indexDir.exists()) {
        FileUtils.fullyDelete(indexDir);
      }
      indexDir.mkdirs();
      directory = FSDirectory.getDirectory(indexDir, eraseIndex);
    } else {
      directory = new RAMDirectory();
    }

    // inputs
    resetInputs();
    
    // release unused stuff
    System.runFinalization();
    System.gc();
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
    searchQueryMaker.resetInputs();
    searchTravQueryMaker.resetInputs();
    searchTravRetQueryMaker.resetInputs();
  }

  /**
   * @return Returns the searchQueryMaker.
   */
  public QueryMaker getSearchQueryMaker() {
    return searchQueryMaker;
  }

  public QueryMaker getSearchTravQueryMaker() {
    return searchTravQueryMaker;
  }

  public QueryMaker getSearchTravRetQueryMaker() {
    return searchTravRetQueryMaker;
  }

}
