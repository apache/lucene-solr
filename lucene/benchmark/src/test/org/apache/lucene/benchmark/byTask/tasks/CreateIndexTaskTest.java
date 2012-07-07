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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.util.Version;


/** Tests the functionality of {@link CreateIndexTask}. */
public class CreateIndexTaskTest extends BenchmarkTestCase {

  private PerfRunData createPerfRunData(String infoStreamValue) throws Exception {
    Properties props = new Properties();
    props.setProperty("writer.version", Version.LUCENE_40.toString());
    props.setProperty("print.props", "false"); // don't print anything
    props.setProperty("directory", "RAMDirectory");
    if (infoStreamValue != null) {
      props.setProperty("writer.info.stream", infoStreamValue);
    }
    Config config = new Config(props);
    return new PerfRunData(config);
  }

  public void testInfoStream_SystemOutErr() throws Exception {
 
    PrintStream curOut = System.out;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos, false, Charset.defaultCharset().name()));
    try {
      PerfRunData runData = createPerfRunData("SystemOut");
      CreateIndexTask cit = new CreateIndexTask(runData);
      cit.doLogic();
      new CloseIndexTask(runData).doLogic();
      assertTrue(baos.size() > 0);
    } finally {
      System.setOut(curOut);
    }
    
    PrintStream curErr = System.err;
    baos.reset();
    System.setErr(new PrintStream(baos, false, Charset.defaultCharset().name()));
    try {
      PerfRunData runData = createPerfRunData("SystemErr");
      CreateIndexTask cit = new CreateIndexTask(runData);
      cit.doLogic();
      new CloseIndexTask(runData).doLogic();
      assertTrue(baos.size() > 0);
    } finally {
      System.setErr(curErr);
    }

  }

  public void testInfoStream_File() throws Exception {
    
    File outFile = new File(getWorkDir(), "infoStreamTest");
    PerfRunData runData = createPerfRunData(outFile.getAbsolutePath());
    new CreateIndexTask(runData).doLogic();
    new CloseIndexTask(runData).doLogic();
    assertTrue(outFile.length() > 0);
  }

  public void testNoMergePolicy() throws Exception {
    PerfRunData runData = createPerfRunData(null);
    runData.getConfig().set("merge.policy", NoMergePolicy.class.getName());
    new CreateIndexTask(runData).doLogic();
    new CloseIndexTask(runData).doLogic();
  }
  
  public void testNoMergeScheduler() throws Exception {
    PerfRunData runData = createPerfRunData(null);
    runData.getConfig().set("merge.scheduler", NoMergeScheduler.class.getName());
    new CreateIndexTask(runData).doLogic();
    new CloseIndexTask(runData).doLogic();
  }

  public void testNoDeletionPolicy() throws Exception {
    PerfRunData runData = createPerfRunData(null);
    runData.getConfig().set("deletion.policy", NoDeletionPolicy.class.getName());
    new CreateIndexTask(runData).doLogic();
    new CloseIndexTask(runData).doLogic();
  }
}
