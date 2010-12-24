package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Properties;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.AddDocTask;
import org.apache.lucene.benchmark.byTask.tasks.CloseIndexTask;
import org.apache.lucene.benchmark.byTask.tasks.CreateIndexTask;
import org.apache.lucene.benchmark.byTask.tasks.TaskSequence;
import org.apache.lucene.benchmark.byTask.tasks.WriteLineDocTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

/** Tests the functionality of {@link LineDocSource}. */
public class LineDocSourceTest extends BenchmarkTestCase {

  private static final CompressorStreamFactory csFactory = new CompressorStreamFactory();

  private void createBZ2LineFile(File file) throws Exception {
    OutputStream out = new FileOutputStream(file);
    out = csFactory.createCompressorOutputStream("bzip2", out);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "utf-8"));
    StringBuilder doc = new StringBuilder();
    doc.append("title").append(WriteLineDocTask.SEP).append("date").append(WriteLineDocTask.SEP).append("body");
    writer.write(doc.toString());
    writer.newLine();
    writer.close();
  }

  private void createRegularLineFile(File file) throws Exception {
    OutputStream out = new FileOutputStream(file);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "utf-8"));
    StringBuilder doc = new StringBuilder();
    doc.append("title").append(WriteLineDocTask.SEP).append("date").append(WriteLineDocTask.SEP).append("body");
    writer.write(doc.toString());
    writer.newLine();
    writer.close();
  }
  
  private void doIndexAndSearchTest(File file, boolean setBZCompress,
      String bz2CompressVal) throws Exception {

    Properties props = new Properties();
    
    // LineDocSource specific settings.
    props.setProperty("docs.file", file.getAbsolutePath());
    if (setBZCompress) {
      props.setProperty("bzip.compression", bz2CompressVal);
    }
    
    // Indexing configuration.
    props.setProperty("analyzer", MockAnalyzer.class.getName());
    props.setProperty("content.source", LineDocSource.class.getName());
    props.setProperty("directory", "RAMDirectory");
    
    // Create PerfRunData
    Config config = new Config(props);
    PerfRunData runData = new PerfRunData(config);

    TaskSequence tasks = new TaskSequence(runData, "testBzip2", null, false);
    tasks.addTask(new CreateIndexTask(runData));
    tasks.addTask(new AddDocTask(runData));
    tasks.addTask(new CloseIndexTask(runData));
    tasks.doLogic();
    
    IndexSearcher searcher = new IndexSearcher(runData.getDirectory(), true);
    TopDocs td = searcher.search(new TermQuery(new Term("body", "body")), 10);
    assertEquals(1, td.totalHits);
    assertNotNull(td.scoreDocs[0]);
    searcher.close();
  }
  
  /* Tests LineDocSource with a bzip2 input stream. */
  public void testBZip2() throws Exception {
    File file = new File(getWorkDir(), "one-line.bz2");
    createBZ2LineFile(file);
    doIndexAndSearchTest(file, true, "true");
  }
  
  public void testBZip2AutoDetect() throws Exception {
    File file = new File(getWorkDir(), "one-line.bz2");
    createBZ2LineFile(file);
    doIndexAndSearchTest(file, false, null);
  }
  
  public void testRegularFile() throws Exception {
    File file = new File(getWorkDir(), "one-line");
    createRegularLineFile(file);
    doIndexAndSearchTest(file, false, null);
  }

  public void testInvalidFormat() throws Exception {
    String[] testCases = new String[] {
      "", // empty line
      "title", // just title
      "title" + WriteLineDocTask.SEP, // title + SEP
      "title" + WriteLineDocTask.SEP + "body", // title + SEP + body
      // note that title + SEP + body + SEP is a valid line, which results in an
      // empty body
    };
    
    for (int i = 0; i < testCases.length; i++) {
      File file = new File(getWorkDir(), "one-line");
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"));
      writer.write(testCases[i]);
      writer.newLine();
      writer.close();
      try {
        doIndexAndSearchTest(file, false, null);
        fail("Some exception should have been thrown for: [" + testCases[i] + "]");
      } catch (Exception e) {
        // expected.
      }
    }
  }
  
}
