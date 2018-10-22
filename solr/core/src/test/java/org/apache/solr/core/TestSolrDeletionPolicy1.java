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
package org.apache.solr.core;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.util.Constants;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class TestSolrDeletionPolicy1 extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-delpolicy1.xml","schema.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
  }
  
  private void addDocs() {

    assertU(adoc("id", String.valueOf(1),
            "name", "name" + String.valueOf(1)));
    assertU(commit());
    assertQ("return all docs",
            req("id:[0 TO 1]"),
            "*[count(//doc)=1]"
    );

    assertU(adoc("id", String.valueOf(2),
            "name", "name" + String.valueOf(2)));
    assertU(commit());
    assertQ("return all docs",
            req("id:[0 TO 2]"),
            "*[count(//doc)=2]"
    );

    assertU(adoc("id", String.valueOf(3),
            "name", "name" + String.valueOf(3)));
    assertU(optimize());
    assertQ("return all docs",
            req("id:[0 TO 3]"),
            "*[count(//doc)=3]"
    );

    assertU(adoc("id", String.valueOf(4),
            "name", "name" + String.valueOf(4)));
    assertU(optimize());
    assertQ("return all docs",
            req("id:[0 TO 4]"),
            "*[count(//doc)=4]"
    );

    assertU(adoc("id", String.valueOf(5),
            "name", "name" + String.valueOf(5)));
    assertU(optimize());
    assertQ("return all docs",
            req("id:[0 TO 5]"),
            "*[count(//doc)=5]"
    );

  }

  @Test
  public void testKeepOptimizedOnlyCommits() {

    IndexDeletionPolicyWrapper delPolicy = h.getCore().getDeletionPolicy();
    addDocs();
    Map<Long, IndexCommit> commits = delPolicy.getCommits();
    IndexCommit latest = delPolicy.getLatestCommit();
    for (Long gen : commits.keySet()) {
      if (commits.get(gen) == latest)
        continue;
      assertEquals(1, commits.get(gen).getSegmentCount());
    }
  }

  @Test
  public void testNumCommitsConfigured() {
    IndexDeletionPolicyWrapper delPolicy = h.getCore().getDeletionPolicy();
    addDocs();
    Map<Long, IndexCommit> commits = delPolicy.getCommits();
    assertTrue(commits.size() <= ((SolrDeletionPolicy) (delPolicy.getWrappedDeletionPolicy())).getMaxOptimizedCommitsToKeep());
  }

  @Test
  public void testCommitAge() throws InterruptedException {
    assumeFalse("This test is not working on Windows (or maybe machines with only 2 CPUs)",
      Constants.WINDOWS);
  
    IndexDeletionPolicyWrapper delPolicy = h.getCore().getDeletionPolicy();
    addDocs();
    Map<Long, IndexCommit> commits = delPolicy.getCommits();
    IndexCommit ic = delPolicy.getLatestCommit();
    String agestr = ((SolrDeletionPolicy) (delPolicy.getWrappedDeletionPolicy())).getMaxCommitAge().replaceAll("[a-zA-Z]", "").replaceAll("-", "");
    long age = Long.parseLong(agestr);
    Thread.sleep(age);

    assertU(adoc("id", String.valueOf(6),
            "name", "name" + String.valueOf(6)));
    assertU(optimize());
    assertQ("return all docs",
            req("id:[0 TO 6]"),
            "*[count(//doc)=6]"
    );

    commits = delPolicy.getCommits();
    assertTrue(!commits.containsKey(ic.getGeneration()));
  }

}
