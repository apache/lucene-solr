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

package org.apache.solr.update;

import org.apache.lucene.util._TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    // skip low version against commited data
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

    // skip low version against uncommited data from updateLog
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

    // skip low version delete against commited doc
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

    // skip low version delete against uncommited doc from updateLog
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

    // skip low version add against uncommited "delete" from updateLog
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

    // skip low version add & low version delete against commited doc
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
    
    // skip low version delete against uncommited doc from updateLog
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

    // skip low version add against uncommited "delete" from updateLog
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

    // fail low version add & low version delete against commited doc
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "a1", "my_version_l", "1001")),
            params("update.chain","external-version-failhard"));
    assertU(commit());
    try {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_l", "42")),
              params("update.chain","external-version-failhard"));
      fail("no 409");
    } catch (SolrException ex) {
      assertEquals(409, ex.code());
    }
    assertU(commit());
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a1'}}");
    try {
      deleteAndGetVersion("aaa", params("del_version", "7", 
                                        "update.chain","external-version-failhard"));
      fail("no 409");
    } catch (SolrException ex) {
      assertEquals(409, ex.code());
    }
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a1'}}");
    assertU(commit());
    
    // fail low version delete against uncommited doc from updateLog
    updateJ(jsonAdd(sdoc("id", "aaa", "name", "a2", "my_version_l", "1002")), 
            params("update.chain","external-version-failhard"));
    try {
      deleteAndGetVersion("aaa", params("del_version", "8", 
                                        "update.chain","external-version-failhard"));
      fail("no 409");
    } catch (SolrException ex) {
      assertEquals(409, ex.code());
    }
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:a2"), "/response/numFound==1");
    assertJQ(req("qt","/get", "id","aaa", "fl","name")
             , "=={'doc':{'name':'a2'}}");

    // fail low version add against uncommited "delete" from updateLog
    deleteAndGetVersion("aaa", params("del_version", "1010",
                                      "update.chain","external-version-failhard"));
    try {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_l", "1005")),
              params("update.chain","external-version-failhard"));
      fail("no 409");
    } catch (SolrException ex) {
      assertEquals(409, ex.code());
    }
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}");

    // fail low version add against committed "delete"
    // (delete was already done & committed above)
    try {
      updateJ(jsonAdd(sdoc("id", "aaa", "name", "XX", "my_version_l", "1009")),
              params("update.chain","external-version-failhard"));
      fail("no 409");
    } catch (SolrException ex) {
      assertEquals(409, ex.code());
    }
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}}");
    assertU(commit());
    assertJQ(req("q","+id:aaa"), "/response/numFound==1");
    assertJQ(req("q","+id:aaa +name:XX"), "/response/numFound==0");
    assertJQ(req("qt","/get", "id","aaa", "fl","my_version_l")
             , "=={'doc':{'my_version_l':1010}}");
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
    ExecutorService runner = Executors.newFixedThreadPool(MAX_CONCURENT, new DefaultSolrThreadFactory("TestDocBasedVersionConstraints"));
    // runner = Executors.newFixedThreadPool(1);    // to test single threaded
    try {
      for (int id = 0; id < NUM_DOCS; id++) {
        final int numAdds = _TestUtil.nextInt(random(),3,MAX_CONCURENT);
        final int winner = _TestUtil.nextInt(random(),0,numAdds-1);
        final int winnerVersion = atLeast(100);
        final boolean winnerIsDeleted = (0 == _TestUtil.nextInt(random(),0,4));
        List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(numAdds);
        for (int variant = 0; variant < numAdds; variant++) {
          final boolean iShouldWin = (variant==winner);
          final long version = (iShouldWin ? winnerVersion 
                                : _TestUtil.nextInt(random(),1,winnerVersion-1));
          if ((iShouldWin && winnerIsDeleted)
              || (!iShouldWin && 0 == _TestUtil.nextInt(random(),0,4))) {
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
      runner.shutdownNow();
    }
  }
  
  private Callable<Object> delayedAdd(final String... fields) {
    return Executors.callable(new Runnable() {
        public void run() {
          // log.info("ADDING DOC: " + adoc(fields));
          assertU(adoc(fields));
        }
      });
  }
  private Callable<Object> delayedDelete(final String id, final String externalVersion) {
    return Executors.callable(new Runnable() {
        public void run() {
          try {
            // Why does this throw "Exception" ???
            // log.info("DELETING DOC: " + id + " v="+externalVersion);
            deleteAndGetVersion(id, params("del_version", externalVersion));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
  }
  
}
