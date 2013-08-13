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

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.lucene.benchmark.byTask.feeds.AbstractQueryMaker;
import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocData;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.tasks.TaskSequence;
import org.apache.lucene.benchmark.byTask.utils.Algorithm;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

import conf.ConfLoader;

/** Test very simply that perf tasks are parses as expected. */
public class TestPerfTasksParse extends LuceneTestCase {

  static final String NEW_LINE = System.getProperty("line.separator");
  static final String INDENT = "  ";

  // properties in effect in all tests here
  static final String propPart = 
    INDENT + "directory=RAMDirectory" + NEW_LINE +
    INDENT + "print.props=false" + NEW_LINE
  ;

  /** Test the repetiotion parsing for parallel tasks */
  public void testParseParallelTaskSequenceRepetition() throws Exception {
    String taskStr = "AddDoc";
    String parsedTasks = "[ "+taskStr+" ] : 1000";
    Benchmark benchmark = new Benchmark(new StringReader(propPart+parsedTasks));
    Algorithm alg = benchmark.getAlgorithm();
    ArrayList<PerfTask> algTasks = alg.extractTasks();
    boolean foundAdd = false;
    for (final PerfTask task : algTasks) {
       if (task.toString().indexOf(taskStr)>=0) {
          foundAdd = true;
       }
       if (task instanceof TaskSequence) {
         assertEquals("repetions should be 1000 for "+parsedTasks, 1000, ((TaskSequence) task).getRepetitions());
         assertTrue("sequence for "+parsedTasks+" should be parallel!", ((TaskSequence) task).isParallel());
       }
       assertTrue("Task "+taskStr+" was not found in "+alg.toString(),foundAdd);
    }
  }

  /** Test the repetiotion parsing for sequential  tasks */
  public void testParseTaskSequenceRepetition() throws Exception {
    String taskStr = "AddDoc";
    String parsedTasks = "{ "+taskStr+" } : 1000";
    Benchmark benchmark = new Benchmark(new StringReader(propPart+parsedTasks));
    Algorithm alg = benchmark.getAlgorithm();
    ArrayList<PerfTask> algTasks = alg.extractTasks();
    boolean foundAdd = false;
    for (final PerfTask task : algTasks) {
       if (task.toString().indexOf(taskStr)>=0) {
          foundAdd = true;
       }
       if (task instanceof TaskSequence) {
         assertEquals("repetions should be 1000 for "+parsedTasks, 1000, ((TaskSequence) task).getRepetitions());
         assertFalse("sequence for "+parsedTasks+" should be sequential!", ((TaskSequence) task).isParallel());
       }
       assertTrue("Task "+taskStr+" was not found in "+alg.toString(),foundAdd);
    }
  }
  
  public static class MockContentSource extends ContentSource {
    @Override
    public DocData getNextDocData(DocData docData)
        throws NoMoreDataException, IOException {
      return docData;
    }
    @Override
    public void close() throws IOException { }
  }

  public static class MockQueryMaker extends AbstractQueryMaker {
    @Override
    protected Query[] prepareQueries() throws Exception {
      return new Query[0];
    }
  }
  
  /** Test the parsing of example scripts **/
  public void testParseExamples() throws Exception {
    // hackedy-hack-hack
    boolean foundFiles = false;
    final File examplesDir = new File(ConfLoader.class.getResource(".").toURI());
    for (File algFile : examplesDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) { return pathname.isFile() && pathname.getName().endsWith(".alg"); }
    })) {
      try {
        Config config = new Config(new InputStreamReader(new FileInputStream(algFile), "UTF-8"));
        String contentSource = config.get("content.source", null);
        if (contentSource != null) { Class.forName(contentSource); }
        config.set("work.dir", new File(TEMP_DIR,"work").getAbsolutePath());
        config.set("content.source", MockContentSource.class.getName());
        String dir = config.get("content.source", null);
        if (dir != null) { Class.forName(dir); }
        config.set("directory", RAMDirectory.class.getName());
        if (config.get("line.file.out", null) != null) {
          config.set("line.file.out", new File(TEMP_DIR,"o.txt").getAbsolutePath());
        }
        if (config.get("query.maker", null) != null) {
          Class.forName(config.get("query.maker", null));
          config.set("query.maker", MockQueryMaker.class.getName());
        }
        PerfRunData data = new PerfRunData(config);
        new Algorithm(data);
      } catch (Throwable t) {
        t.printStackTrace();
        fail("Could not parse sample file: " + algFile + " reason:"
            + t.getClass() + ":" + t.getMessage());
      }
      foundFiles = true;
    }
    if (!foundFiles) {
      fail("could not find any .alg files!");
    }
  }

}
