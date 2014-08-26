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

import java.util.Properties;

import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.index.SegmentInfos;

/** Tests the functionality of {@link CreateIndexTask}. */
public class CommitIndexTaskTest extends BenchmarkTestCase {

  private PerfRunData createPerfRunData() throws Exception {
    Properties props = new Properties();
    props.setProperty("writer.version", TEST_VERSION_CURRENT.toString());
    props.setProperty("print.props", "false"); // don't print anything
    props.setProperty("directory", "RAMDirectory");
    Config config = new Config(props);
    return new PerfRunData(config);
  }

  public void testNoParams() throws Exception {
    PerfRunData runData = createPerfRunData();
    new CreateIndexTask(runData).doLogic();
    new CommitIndexTask(runData).doLogic();
    new CloseIndexTask(runData).doLogic();
  }
  
  public void testCommitData() throws Exception {
    PerfRunData runData = createPerfRunData();
    new CreateIndexTask(runData).doLogic();
    CommitIndexTask task = new CommitIndexTask(runData);
    task.setParams("params");
    task.doLogic();
    SegmentInfos infos = new SegmentInfos();
    infos.read(runData.getDirectory());
    assertEquals("params", infos.getUserData().get(OpenReaderTask.USER_DATA));
    new CloseIndexTask(runData).doLogic();
  }

}
