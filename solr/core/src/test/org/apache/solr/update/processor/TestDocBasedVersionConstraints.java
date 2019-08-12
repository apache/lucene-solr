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
package org.apache.solr.update.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class TestDocBasedVersionConstraints extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-externalversionconstraint.xml", "schema15.xml");
  }

  @Before
  public void before() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }


  public void testSimpleUpdates() throws Exception {

    // skip low version against committed data
    assertU(adoc("id", "aaa", "name", "a1", "my_version_l", "1001"));
    assertU(commit());
    assertU(adoc("id", "aaa", "name", "a2", "my_version_l", "1002"));
    assertU(commit());
    assertU(adoc("id", "aaa", "name", "XX", "my_version_l",    "1"));
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("q","+id:aaa +name:a2"), "/response/numFound==1");
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");

    // skip low version against uncommitted data from updateLog
    assertU(adoc("id", "aaa", "name", "a3", "my_version_l", "1003"));
    assertU(adoc("id", "aaa", "name", "XX", "my_version_l",    "7"));
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a3'}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("q","+id:aaa +name:a3"), "/response/numFound==1");
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a3'}}");

    // interleave updates to multiple docs using same versions
    for (long ver = 1010; ver < 1020; ver++) {
      for (String id : new String[] {"aaa", "bbb", "ccc", "ddd"}) {
        assertU(adoc("id", id, "my_version_l", ""+ver));
      }
    }
    for (String id : new String[] {"aaa", "bbb", "ccc", "ddd"}) {
      assertU(adoc("id", id, "name", "XX", "my_version_l", "10"));
      assertJQ(req("qt","/get", "id",id, "fl","my_version_l")
               , "=={'doc':{'my_version_l':"+1019+"}}");
    }
    assertU(commit());
    assertJQ(req("q","name:XX"), "/response/numFound==0");
    for (String id : new String[] {"aaa", "bbb", "ccc", "ddd"}) {
      assertJQ(req("q","+id:"+id), "/response/numFound==1");
      assertJQ(req("q","+name:XX +id:"+id), "/response/numFound==0");
      assertJQ(req("q","+id:"+id + " +my_version_l:1019"), "/response/numFound==1");
      assertJQ(req("qt","/get", "id",id, "fl","my_version_l")
               , "=={'doc':{'my_version_l':"+1019+"}}");
    }
  }

  public void testSimpleDeletes() throws Exception {

    // skip low version delete against committed doc
    assertU(adoc("id", "aaa", "name", "a1", "my_version_l", "1001"));
    assertU(commit());
    assertU(adoc("id", "aaa", "name", "a2", "my_version_l", "1002"));
    assertU(commit());
    deleteAndGetVersion("aaa",
                        params("del_version", "7"));
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:a2"), "/response/numFound==1");
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");

    // skip low version delete against uncommitted doc from updateLog
    assertU(adoc("id", "aaa", "name", "a3", "my_version_l", "1003"));
    deleteAndGetVersion("aaa",
                        params("del_version", "8"));
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a3'}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:a3"), "/response/numFound==1");
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a3'}}");

    // skip low version add against uncommitted "delete" from updateLog
    deleteAndGetVersion("aaa", params("del_version", "1010"));
    assertU(adoc("id", "aaa", "name", "XX", "my_version_l", "22"));
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}");

    // skip low version add against committed "delete"
    // (delete was already done & committed above)
    assertU(adoc("id", "aaa", "name", "XX", "my_version_l", "23"));
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}");
  }

  /**
   * Sanity check that there are no hardcoded assumptions about the 
   * field type used that could byte us in the ass.
   */
  public void testFloatVersionField() throws Exception {

    // skip low version add & low version delete against committed doc
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "a1", "my_version_f", "10.01")),
            params("update.chain","external-version-float"));
    assertU(commit());
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_f", "4.2")),
            params("update.chain","external-version-float"));
    assertU(commit());
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a1'}}");
    deleteAndGetVersion("aaa", params("del_version", "7", 
                                      "update.chain","external-version-float"));
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a1'}}");
    assertU(commit());
    
    // skip low version delete against uncommitted doc from updateLog
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "a2", "my_version_f", "10.02")), 
            params("update.chain","external-version-float"));
    deleteAndGetVersion("aaa", params("del_version", "8", 
                                      "update.chain","external-version-float"));
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:a2"), "/response/numFound==1");
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");

    // skip low version add against uncommitted "delete" from updateLog
    deleteAndGetVersion("aaa", params("del_version", "10.10",
                                      "update.chain","external-version-float"));
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_f", "10.05")),
            params("update.chain","external-version-float"));
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_f")
             , "=={'doc':{'my_version_f':10.10}}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_f")
             , "=={'doc':{'my_version_f':10.10}}");

    // skip low version add against committed "delete"
    // (delete was already done & committed above)
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_f", "10.09")),
            params("update.chain","external-version-float"));
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_f")
             , "=={'doc':{'my_version_f':10.10}}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_f")
             , "=={'doc':{'my_version_f':10.10}}");
  }

  public void testFailOnOldVersion() throws Exception {

    // fail low version add & low version delete against committed doc
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "a1", "my_version_l", "1001")),
            params("update.chain","external-version-failhard"));
    assertU(commit());

    SolrException ex = expectThrows(SolrException.class, () -> {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_l", "42")),
          params("update.chain","external-version-failhard"));
    });
    assertEquals(409, ex.code());

    assertU(commit());
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a1'}}");

    ex = expectThrows(SolrException.class, () -> {
      deleteAndGetVersion("aaa", params("del_version", "7",
          "update.chain","external-version-failhard"));
    });
    assertEquals(409, ex.code());

    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a1'}}");
    assertU(commit());
    
    // fail low version delete against uncommitted doc from updateLog
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "a2", "my_version_l", "1002")), 
            params("update.chain","external-version-failhard"));
    ex = expectThrows(SolrException.class, () -> {
      deleteAndGetVersion("aaa", params("del_version", "8",
          "update.chain","external-version-failhard"));
    });
    assertEquals(409, ex.code());

    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:a2"), "/response/numFound==1");
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");

    // fail low version add against uncommitted "delete" from updateLog
    deleteAndGetVersion("aaa", params("del_version", "1010",
                                      "update.chain","external-version-failhard"));
    ex = expectThrows(SolrException.class, () -> {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_l", "1005")),
          params("update.chain","external-version-failhard"));
    });
    assertEquals(409, ex.code());

    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}");

    // fail low version add against committed "delete"
    // (delete was already done & committed above)
    ex = expectThrows(SolrException.class, () -> {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_l", "1009")),
          params("update.chain","external-version-failhard"));
    });
    assertEquals(409, ex.code());

    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}");
  }

  // Test multiple versions, that it has to be greater than my_version_l and my_version_f
  public void testMultipleVersions() throws Exception {
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "a1", "my_version_l", "1001", "my_version_f", "1.0")),
      params("update.chain","external-version-failhard-multiple"));
    assertU(commit());
    // All variations of additional versions should fail other than my_version_l greater or my_version_f greater.
    SolrException ex = expectThrows(SolrException.class, () -> {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "X1", "my_version_l", "1000", "my_version_f", "1.0")),
          params("update.chain","external-version-failhard-multiple"));
    });
    assertEquals(409, ex.code());

    ex = expectThrows(SolrException.class, () -> {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "X2", "my_version_l", "1001", "my_version_f", "0.9")),
          params("update.chain","external-version-failhard-multiple"));
    });
    assertEquals(409, ex.code());

    // Also fails on the exact same version
    ex = expectThrows(SolrException.class, () -> {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "X3", "my_version_l", "1001", "my_version_f", "1.0")),
          params("update.chain","external-version-failhard-multiple"));
    });
    assertEquals(409, ex.code());

    //Verify we are still unchanged
    assertU(commit());
    assertJQ(req("q","+id:aaa +name:a1"), "/response/numFound==1");

    // update version 1
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "Y1", "my_version_l", "2001", "my_version_f", "1.0")),
        params("update.chain","external-version-failhard-multiple"));
    assertU(commit());
    assertJQ(req("q","+id:aaa +name:Y1"), "/response/numFound==1");

    // update version 2
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "Y2", "my_version_l", "2001", "my_version_f", "2.0")),
        params("update.chain","external-version-failhard-multiple"));
    assertU(commit());
    assertJQ(req("q","+id:aaa +name:Y2"), "/response/numFound==1");
  }

  public void testMultipleVersionDeletes() throws Exception {
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "a1", "my_version_l", "1001", "my_version_f", "1.0")),
        params("update.chain","external-version-failhard-multiple"));
    assertU(commit());

    SolrException ex = expectThrows(SolrException.class, () -> {
      deleteAndGetVersion("aaa", params("del_version", "1000", "del_version_2", "1.0",
          "update.chain","external-version-failhard-multiple"));
    });
    assertEquals(409, ex.code());

    ex = expectThrows(SolrException.class, () -> {
      deleteAndGetVersion("aaa", params("del_version", "1001", "del_version_2", "0.9",
          "update.chain","external-version-failhard-multiple"));
    });
    assertEquals(409, ex.code());

    // And just verify if we pass version 1, we still error if version 2 isn't found.
    ignoreException("Delete by ID must specify doc version param");
    ex = expectThrows(SolrException.class, () -> {
      deleteAndGetVersion("aaa", params("del_version", "1001",
          "update.chain","external-version-failhard-multiple"));
    });
    assertEquals(400, ex.code());
    unIgnoreException("Delete by ID must specify doc version param");

    //Verify we are still unchanged
    assertU(commit());
    assertJQ(req("q","+id:aaa +name:a1"), "/response/numFound==1");

    //And let's verify the actual case.
    deleteAndGetVersion("aaa", params("del_version", "1001", "del_version_2", "2.0",
        "update.chain","external-version-failhard-multiple"));
    assertU(commit());
    assertJQ(req("q","+id:aaa +name:a1"), "/response/numFound==0"); //Delete allowed
  }


  /** 
   * Proof of concept test demonstrating how to manage and periodically cleanup
   * the "logically" deleted documents
   */
  public void testManagingDeletes() throws Exception {
    // add some docs
    for (long ver = 1010; ver < 1020; ver++) {
      for (String id : new String[] {"aaa", "bbb", "ccc", "ddd"}) {
        assertU(adoc("id", id, "name", "name_"+id, "my_version_l", ""+ver));
      }
    }
    assertU(adoc("id", "aaa", "name", "name_aaa", "my_version_l", "1030"));
    assertU(commit());
    // sample queries
    assertJQ(req("q","*:*",
                 "fq","live_b:true")
             ,"/response/numFound==4");
    assertJQ(req("q","id:aaa",
                 "fq","live_b:true",
                 "fl","id,my_version_l")
             ,"/response/numFound==1"
             ,"/response/docs==[{'id':'aaa','my_version_l':1030}]}");
    // logically delete
    deleteAndGetVersion("aaa",
                        params("del_version", "1031"));
    assertU(commit());
    // sample queries
    assertJQ(req("q","*:*",
                 "fq","live_b:true")
             ,"/response/numFound==3");
    assertJQ(req("q","id:aaa",
                 "fq","live_b:true")
             ,"/response/numFound==0");
    // placeholder doc is still in the index though
    assertJQ(req("q","id:aaa",
                 "fq","live_b:false",
                 "fq", "timestamp_tdt:[* TO *]",
                 "fl","id,live_b,my_version_l")
             ,"/response/numFound==1"
             ,"/response/docs==[{'id':'aaa','my_version_l':1031,'live_b':false}]}");
    // doc can't be re-added with a low version
    assertU(adoc("id", "aaa", "name", "XX", "my_version_l", "1025"));
    assertU(commit());
    assertJQ(req("q","id:aaa",
                 "fq","live_b:true")
             ,"/response/numFound==0");

    // "dead" placeholder docs can be periodically cleaned up 
    // ie: assertU(delQ("+live_b:false +timestamp_tdt:[* TO NOW/MINUTE-5MINUTE]"));
    // but to prevent the test from ebing time sensitive we'll just purge them all
    assertU(delQ("+live_b:false"));
    assertU(commit());
    // now doc can be re-added w/any version, no matter how low
    assertU(adoc("id", "aaa", "name", "aaa", "my_version_l", "7"));
    assertU(commit());
    assertJQ(req("q","id:aaa",
                 "fq","live_b:true",
                 "fl","id,live_b,my_version_l")
             ,"/response/numFound==1"
             ,"/response/docs==[{'id':'aaa','my_version_l':7,'live_b':true}]}");
    
  }

  /** 
   * Constantly hammer the same doc with multiple concurrent threads and diff versions,
   * confirm that the highest version wins.
   */
  public void testConcurrentAdds() throws Exception {
    final int NUM_DOCS = atLeast(50);
    final int MAX_CONCURENT = atLeast(10);
    ExecutorService runner = ExecutorUtil.newMDCAwareFixedThreadPool(MAX_CONCURENT, new DefaultSolrThreadFactory("TestDocBasedVersionConstraints"));
    // runner = Executors.newFixedThreadPool(1);    // to test single threaded
    try {
      for (int id = 0; id < NUM_DOCS; id++) {
        final int numAdds = TestUtil.nextInt(random(), 3, MAX_CONCURENT);
        final int winner = TestUtil.nextInt(random(), 0, numAdds - 1);
        final int winnerVersion = atLeast(100);
        final boolean winnerIsDeleted = (0 == TestUtil.nextInt(random(), 0, 4));
        List<Callable<Object>> tasks = new ArrayList<>(numAdds);
        for (int variant = 0; variant < numAdds; variant++) {
          final boolean iShouldWin = (variant==winner);
          final long version = (iShouldWin ? winnerVersion 
                                : TestUtil.nextInt(random(), 1, winnerVersion - 1));
          if ((iShouldWin && winnerIsDeleted)
              || (!iShouldWin && 0 == TestUtil.nextInt(random(), 0, 4))) {
            tasks.add(delayedDelete(""+id, ""+version));
          } else {
            tasks.add(delayedAdd("id",""+id,"name","name"+id+"_"+variant,
                                 "my_version_l", ""+ version));
          }
        }
        runner.invokeAll(tasks);
        final String expectedDoc = "{'id':'"+id+"','my_version_l':"+winnerVersion +
          ( ! winnerIsDeleted ? ",'name':'name"+id+"_"+winner+"'}" : "}");

        assertJQ(req("qt","/get", "id",""+id, "fl","id,name,my_version_l")
                 , "=={'doc':" + expectedDoc + "}");
        assertU(commit());
        assertJQ(req("q","id:"+id,
                     "fl","id,name,my_version_l")
                 ,"/response/numFound==1"
                 ,"/response/docs==["+expectedDoc+"]");
      }
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(runner);
    }
  }

  public void testMissingVersionOnOldDocs() throws Exception {
    String version = "2";

    // Write one doc with version, one doc without version using the "no version" chain
    updateJ(json("[{\"id\": \"a\", \"name\": \"a1\", \"my_version_l\": " + version + "}]"),
            params("update.chain", "no-external-version"));
    updateJ(json("[{\"id\": \"b\", \"name\": \"b1\"}]"), params("update.chain", "no-external-version"));
    assertU(commit());
    assertJQ(req("q","*:*"), "/response/numFound==2");
    assertJQ(req("q","id:a"), "/response/numFound==1");
    assertJQ(req("q","id:b"), "/response/numFound==1");

    // Try updating both with a new version and using the enforced version chain, expect id=b to fail bc old
    // doc is missing the version field
    String newVersion = "3";
    updateJ(json("[{\"id\": \"a\", \"name\": \"a1\", \"my_version_l\": " + newVersion + "}]"),
            params("update.chain", "external-version-constraint"));

    ignoreException("Doc exists in index, but has null versionField: my_version_l");
    SolrException ex = expectThrows(SolrException.class, () -> {
      updateJ(json("[{\"id\": \"b\", \"name\": \"b1\", \"my_version_l\": " + newVersion + "}]"),
          params("update.chain", "external-version-constraint"));
    });
    assertEquals("Doc exists in index, but has null versionField: my_version_l", ex.getMessage());
    unIgnoreException("Doc exists in index, but has null versionField: my_version_l");

    assertU(commit());
    assertJQ(req("q","*:*"), "/response/numFound==2");
    assertJQ(req("qt","/get", "id", "a", "fl", "id,my_version_l"), "=={'doc':{'id':'a', 'my_version_l':3}}"); // version changed to 3
    assertJQ(req("qt","/get", "id", "b", "fl", "id,my_version_l"), "=={'doc':{'id':'b'}}"); // no version, because update failed

    // Try to update again using the external version enforcement, but allowing old docs to not have the version
    // field. Expect id=a to fail because version is lower, expect id=b to succeed.
    version = "1";
    updateJ(json("[{\"id\": \"a\", \"name\": \"a1\", \"my_version_l\": " + version + "}]"),
            params("update.chain", "external-version-support-missing"));
    System.out.println("send b");
    updateJ(json("[{\"id\": \"b\", \"name\": \"b1\", \"my_version_l\": " + version + "}]"),
            params("update.chain", "external-version-support-missing"));
    assertU(commit());
    assertJQ(req("q","*:*"), "/response/numFound==2");
    assertJQ(req("qt","/get", "id", "a", "fl", "id,my_version_l"), "=={'doc':{'id':'a', 'my_version_l':3}}");
    assertJQ(req("qt","/get", "id", "b", "fl", "id,my_version_l"), "=={'doc':{'id':'b', 'my_version_l':1}}");
  }
  
  public void testTombstoneConfig() throws Exception {
    assertJQ(req("q","*:*"),"/response/numFound==0");
    updateWithChain("tombstone-config", 
        "id", "b!doc1", 
        "my_version_l", "1");
    assertU(commit());
    assertJQ(req("q","*:*"),"/response/numFound==1");
    assertJQ(req("q","foo_b:true"),"/response/numFound==0");
    assertJQ(req("q","foo_i:1"),"/response/numFound==0");
    assertJQ(req("q","foo_l:1"),"/response/numFound==0");
    assertJQ(req("q","foo_f:1.5"),"/response/numFound==0");
    assertJQ(req("q","foo_s:bar"),"/response/numFound==0");
    assertJQ(req("q","foo_ss:bar1"),"/response/numFound==0");
    assertJQ(req("q","foo_ss:bar2"),"/response/numFound==0");
    
    deleteAndGetVersion("b!doc1",
        params("del_version", "2", "update.chain",
            "tombstone-config"));
    assertU(commit());
    
    assertJQ(req("q","foo_b:true"),"/response/numFound==1");
    assertJQ(req("q","foo_i:1"),"/response/numFound==1");
    assertJQ(req("q","foo_l:1"),"/response/numFound==1");
    assertJQ(req("q","foo_f:1.5"),"/response/numFound==1");
    assertJQ(req("q","foo_s:bar"),"/response/numFound==1");
    assertJQ(req("q","foo_ss:bar1"),"/response/numFound==1");
    assertJQ(req("q","foo_ss:bar2"),"/response/numFound==1");
  }
  
  public void testCanCreateTombstonesBasic() {
    DocBasedVersionConstraintsProcessorFactory factory = new DocBasedVersionConstraintsProcessorFactory();
    NamedList<Object> config = new NamedList<>();
    config.add("versionField", "_version_");
    factory.init(config);
    IndexSchema schema = h.getCore().getLatestSchema();
    assertThat(factory.canCreateTombstoneDocument(schema), is(true));
  }
  
  public void testCanCreateTombstonesMissingRequiredField() {
    DocBasedVersionConstraintsProcessorFactory factory = new DocBasedVersionConstraintsProcessorFactory();
    NamedList<Object> config = new NamedList<>();
    config.add("versionField", "_version_");
    factory.init(config);
    IndexSchema schema = h.getCore().getLatestSchema();
    SchemaField sf = schema.getField("sku1");
    assertThat(sf, is(not(nullValue())));
    assertThat(schema.getRequiredFields(), not(hasItem(sf)));
    try {
      schema.getRequiredFields().add(sf);
      assertThat(factory.canCreateTombstoneDocument(schema), is(false));
    } finally {
      schema.getRequiredFields().remove(sf);
    }
  }
  
  public void testCanCreateTombstonesRequiredFieldWithDefault() {
    DocBasedVersionConstraintsProcessorFactory factory = new DocBasedVersionConstraintsProcessorFactory();
    NamedList<Object> config = new NamedList<>();
    config.add("versionField", "_version_");
    factory.init(config);
    IndexSchema schema = h.getCore().getLatestSchema();
    SchemaField sf = schema.getField("sku1");
    SchemaField sf2 = new SchemaField("sku1_with_default", sf.getType(), sf.getProperties(), "foo");
    try {
      schema.getRequiredFields().add(sf2);
      assertThat(factory.canCreateTombstoneDocument(schema), is(true));
    } finally {
      schema.getRequiredFields().remove(sf2);
    }
  }
  
  public void testCanCreateTombstonesRequiredFieldInTombstoneConfig() {
    DocBasedVersionConstraintsProcessorFactory factory = new DocBasedVersionConstraintsProcessorFactory();
    NamedList<Object> config = new NamedList<>();
    config.add("versionField", "_version_");
    NamedList<Object> tombstoneConfig = new NamedList<>();
    config.add("tombstoneConfig", tombstoneConfig);
    tombstoneConfig.add("sku1", "foo");
    factory.init(config);
    IndexSchema schema = h.getCore().getLatestSchema();
    SchemaField sf = schema.getField("sku1");
    assertThat(sf, is(not(nullValue())));
    assertThat(schema.getRequiredFields(), not(hasItem(sf)));
    try {
      schema.getRequiredFields().add(sf);
      assertThat(factory.canCreateTombstoneDocument(schema), is(true));
    } finally {
      schema.getRequiredFields().remove(sf);
    }
  }
  
  public void testCanCreateTombstonesVersionFieldRequired() {
    DocBasedVersionConstraintsProcessorFactory factory = new DocBasedVersionConstraintsProcessorFactory();
    NamedList<Object> config = new NamedList<>();
    config.add("versionField", "_version_");
    factory.init(config);
    IndexSchema schema = h.getCore().getLatestSchema();
    SchemaField versionField = schema.getField("_version_");
    assertThat(versionField, is(not(nullValue())));
    assertThat(schema.getRequiredFields(), not(hasItem(versionField)));
    try {
      schema.getRequiredFields().add(versionField);
      assertThat(factory.canCreateTombstoneDocument(schema), is(true));
    } finally {
      schema.getRequiredFields().remove(versionField);
    }
  }
  
  public void testCanCreateTombstonesUniqueKeyFieldRequired() {
    DocBasedVersionConstraintsProcessorFactory factory = new DocBasedVersionConstraintsProcessorFactory();
    NamedList<Object> config = new NamedList<>();
    config.add("versionField", "_version_");
    factory.init(config);
    IndexSchema schema = h.getCore().getLatestSchema();
    SchemaField uniqueKeyField = schema.getField("id");
    assertThat(uniqueKeyField, is(not(nullValue())));
    assertThat(uniqueKeyField, equalTo(schema.getUniqueKeyField()));
    assertThat(schema.getRequiredFields(), hasItem(schema.getUniqueKeyField()));
    assertThat(factory.canCreateTombstoneDocument(schema), is(true));
  }
  
  private void updateWithChain(String chain, String...fields) throws Exception {
    assert fields.length % 2 == 0;
    SolrInputDocument doc = new SolrInputDocument(fields);
    updateJ(jsonAdd(doc), params("update.chain", chain));
  }
  
  private Callable<Object> delayedAdd(final String... fields) {
    return Executors.callable(() -> {
      // log.info("ADDING DOC: " + adoc(fields));
      assertU(adoc(fields));
    });
  }
  private Callable<Object> delayedDelete(final String id, final String externalVersion) {
    return Executors.callable(() -> {
      try {
        // Why does this throw "Exception" ???
        // log.info("DELETING DOC: " + id + " v="+externalVersion);
        deleteAndGetVersion(id, params("del_version", externalVersion));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
  
}
