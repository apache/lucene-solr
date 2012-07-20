package org.apache.lucene.benchmark.byTask.tasks;

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

import java.io.File;
import java.util.Properties;

import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util._TestUtil;
import org.junit.BeforeClass;

/** Tests the functionality of {@link AddIndexesTask}. */
public class AddIndexesTaskTest extends BenchmarkTestCase {

  private static File testDir, inputDir;
  
  @BeforeClass
  public static void beforeClassAddIndexesTaskTest() throws Exception {
    testDir = _TestUtil.getTempDir("addIndexesTask");
    
    // create a dummy index under inputDir
    inputDir = new File(testDir, "input");
    Directory tmpDir = newFSDirectory(inputDir);
    try {
      IndexWriter writer = new IndexWriter(tmpDir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
      for (int i = 0; i < 10; i++) {
        writer.addDocument(new Document());
      }
      writer.close();
    } finally {
      tmpDir.close();
    }
  }
  
  private PerfRunData createPerfRunData() throws Exception {
    Properties props = new Properties();
    props.setProperty("writer.version", TEST_VERSION_CURRENT.toString());
    props.setProperty("print.props", "false"); // don't print anything
    props.setProperty("directory", "RAMDirectory");
    props.setProperty(AddIndexesTask.ADDINDEXES_INPUT_DIR, inputDir.getAbsolutePath());
    Config config = new Config(props);
    return new PerfRunData(config);
  }

  private void assertIndex(PerfRunData runData) throws Exception {
    Directory taskDir = runData.getDirectory();
    assertSame(RAMDirectory.class, taskDir.getClass());
    IndexReader r = DirectoryReader.open(taskDir);
    try {
      assertEquals(10, r.numDocs());
    } finally {
      r.close();
    }
  }
  
  public void testAddIndexesDefault() throws Exception {
    PerfRunData runData = createPerfRunData();
    // create the target index first
    new CreateIndexTask(runData).doLogic();
    
    AddIndexesTask task = new AddIndexesTask(runData);
    task.setup();
    
    // add the input index
    task.doLogic();
    
    // close the index
    new CloseIndexTask(runData).doLogic();
    
    assertIndex(runData);
    
    runData.close();
  }
  
  public void testAddIndexesDir() throws Exception {
    PerfRunData runData = createPerfRunData();
    // create the target index first
    new CreateIndexTask(runData).doLogic();
    
    AddIndexesTask task = new AddIndexesTask(runData);
    task.setup();
    
    // add the input index
    task.setParams("true");
    task.doLogic();
    
    // close the index
    new CloseIndexTask(runData).doLogic();
    
    assertIndex(runData);
    
    runData.close();
  }
  
  public void testAddIndexesReader() throws Exception {
    PerfRunData runData = createPerfRunData();
    // create the target index first
    new CreateIndexTask(runData).doLogic();
    
    AddIndexesTask task = new AddIndexesTask(runData);
    task.setup();
    
    // add the input index
    task.setParams("false");
    task.doLogic();
    
    // close the index
    new CloseIndexTask(runData).doLogic();
    
    assertIndex(runData);
    
    runData.close();
  }
  
}
