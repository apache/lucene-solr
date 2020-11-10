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
package org.apache.solr.cloud;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.TimeUnits;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@Nightly
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
@TimeoutSuite(millis = TimeUnits.HOUR)
@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13924")
public class MoveReplicaHDFSTest extends MoveReplicaTest {
  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
    }
  }

  @Override
  protected String getConfigSet() {
    return "cloud-hdfs";
  }

  @Test
  public void testNormalMove() throws Exception {
    inPlaceMove = false;
    test();
  }

  @Test
  //2018-06-18 (commented) @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  //commented 9-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Jul-2018
  //commented 23-AUG-2018  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 17-Aug-2018
  // commented 4-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 23-Aug-2018
  //commented 20-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 6-Sep-2018
  //Commented 14-Oct-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testNormalFailedMove() throws Exception {
    inPlaceMove = false;
    testFailedMove();
  }

  @Test
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-12080") // added 03-Oct-2018
  public void testFailedMove() throws Exception {
    super.testFailedMove();
  }
}

