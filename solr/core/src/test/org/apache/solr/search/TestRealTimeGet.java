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
package org.apache.solr.search;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestHarness;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.core.SolrCore.verbose;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class TestRealTimeGet extends TestRTGBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    randomizeUpdateLogImpl();
    initCore("solrconfig-tlog.xml","schema_latest.xml");
  }


  @Test
  public void testGetRealtime() throws Exception {
    clearIndex();
    assertU(commit());

    assertU(adoc("id","1",
        "a_f","-1.5", "a_fd","-1.5", "a_fdS","-1.5",                        "a_fs","1.0","a_fs","2.5", "a_fds","1.0","a_fds","2.5",  "a_fdsS","1.0","a_fdsS","2.5",
        "a_d","-1.2E99", "a_dd","-1.2E99", "a_ddS","-1.2E99",               "a_ds","1.0","a_ds","2.5", "a_dds","1.0","a_dds","2.5",  "a_ddsS","1.0","a_ddsS","2.5",
        "a_i","-1", "a_id","-1", "a_idS","-1",                              "a_is","1","a_is","2",     "a_ids","1","a_ids","2",      "a_idsS","1","a_idsS","2",
        "a_l","-9999999999", "a_ld","-9999999999", "a_ldS","-9999999999",   "a_ls","1","a_ls","9999999999",     "a_lds","1","a_lds","9999999999",      "a_ldsS","1","a_ldsS","9999999999",
        "a_s", "abc", "a_sd", "bcd", "a_sdS", "cde",                        "a_ss","def","a_ss", "efg",    "a_sds","fgh","a_sds","ghi",   "a_sdsS","hij","a_sdsS","ijk",
        "a_b", "false", "a_bd", "true", "a_bdS", "false",                    "a_bs","true","a_bs", "false",    "a_bds","true","a_bds","false",   "a_bdsS","true","a_bdsS","false"
    ));
    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );
    assertJQ(req("qt","/get", "id","1", "fl","id, a_f,a_fd,a_fdS   a_fs,a_fds,a_fdsS,  " +
            "a_d,a_dd,a_ddS,  a_ds,a_dds,a_ddsS,  a_i,a_id,a_idS   a_is,a_ids,a_idsS,   " +
            "a_l,a_ld,a_ldS,   a_ls,a_lds,a_ldsS,  a_s,a_sd,a_sdS   a_ss,a_sds,a_sdsS,   " +
            "a_b,a_bd,a_bdS,   a_bs,a_bds,a_bdsS"
        )
        ,"=={'doc':{'id':'1'" +
            ", a_f:-1.5, a_fd:-1.5, a_fdS:-1.5,  a_fs:[1.0,2.5],      a_fds:[1.0,2.5],a_fdsS:[1.0,2.5]" +
            ", a_d:-1.2E99, a_dd:-1.2E99, a_ddS:-1.2E99,              a_ds:[1.0,2.5],a_dds:[1.0,2.5],a_ddsS:[1.0,2.5]" +
            ", a_i:-1, a_id:-1, a_idS:-1,                             a_is:[1,2],a_ids:[1,2],a_idsS:[1,2]" +
            ", a_l:-9999999999, a_ld:-9999999999, a_ldS:-9999999999,  a_ls:[1,9999999999],a_lds:[1,9999999999],a_ldsS:[1,9999999999]" +
            ", a_s:'abc', a_sd:'bcd', a_sdS:'cde',  a_ss:['def','efg'],a_sds:['fgh','ghi'],a_sdsS:['hij','ijk']" +
            ", a_b:false, a_bd:true, a_bdS:false,  a_bs:[true,false],a_bds:[true,false],a_bdsS:[true,false]" +
            "       }}"
    );
    assertJQ(req("qt","/get","ids","1", "fl","id")
        ,"=={" +
        "  'response':{'numFound':1,'start':0,'numFoundExact':true,'docs':[" +
        "      {" +
        "        'id':'1'}]" +
        "  }}}"
    );

    assertU(commit());

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );

    // a cut-n-paste of the first big query, but this time it will be retrieved from the index rather than the transaction log
    assertJQ(req("qt","/get", "id","1", "fl","id, a_f,a_fd,a_fdS   a_fs,a_fds,a_fdsS,  a_d,a_dd,a_ddS,  a_ds,a_dds,a_ddsS,  a_i,a_id,a_idS   a_is,a_ids,a_idsS,   a_l,a_ld,a_ldS   a_ls,a_lds,a_ldsS")
        ,"=={'doc':{'id':'1'" +
            ", a_f:-1.5, a_fd:-1.5, a_fdS:-1.5,  a_fs:[1.0,2.5],      a_fds:[1.0,2.5],a_fdsS:[1.0,2.5]" +
            ", a_d:-1.2E99, a_dd:-1.2E99, a_ddS:-1.2E99,              a_ds:[1.0,2.5],a_dds:[1.0,2.5],a_ddsS:[1.0,2.5]" +
            ", a_i:-1, a_id:-1, a_idS:-1,                             a_is:[1,2],a_ids:[1,2],a_idsS:[1,2]" +
            ", a_l:-9999999999, a_ld:-9999999999, a_ldS:-9999999999,  a_ls:[1,9999999999],a_lds:[1,9999999999],a_ldsS:[1,9999999999]" +
            "       }}"
    );

    assertJQ(req("qt","/get","id","1", "fl","id")
        ,"=={'doc':{'id':'1'}}"
    );
    assertJQ(req("qt","/get","ids","1", "fl","id")
        ,"=={" +
        "  'response':{'numFound':1,'start':0,'numFoundExact':true,'docs':[" +
        "      {" +
        "        'id':'1'}]" +
        "  }}}"
    );

    assertU(delI("1"));

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':null}"
    );
    assertJQ(req("qt","/get","ids","1")
        ,"=={'response':{'numFound':0,'start':0,'numFoundExact':true,'docs':[]}}"
    );


    assertU(adoc("id","10"));
    assertU(adoc("id","11"));
    assertJQ(req("qt","/get","id","10", "fl","id")
        ,"=={'doc':{'id':'10'}}"
    );
    assertU(delQ("id:10 foo_s:abcdef"));
    assertJQ(req("qt","/get","id","10")
        ,"=={'doc':null}"
    );
    assertJQ(req("qt","/get","id","11", "fl","id")
        ,"=={'doc':{'id':'11'}}"
    );

    // multivalued field
    assertU(adoc("id","12", "val_ls","1", "val_ls","2"));
    assertJQ(req("q","id:12")
        ,"/response/numFound==0"
    );
    assertJQ(req("qt","/get", "id","12", "fl","id,val_ls")
        ,"=={'doc':{'id':'12', 'val_ls':[1,2]}}"
    );

    assertU(commit());

    assertJQ(req("qt","/get", "id","12", "fl","id,val_ls")
        ,"=={'doc':{'id':'12', 'val_ls':[1,2]}}"
    );
    assertJQ(req("q","id:12")
        ,"/response/numFound==1"
    );


    SolrQueryRequest req = req();
    RefCounted<SolrIndexSearcher> realtimeHolder = req.getCore().getRealtimeSearcher();

    //
    // filters
    //
    assertU(adoc("id", "12"));
    assertU(adoc("id", "13"));

    // this should not need to open another realtime searcher
    assertJQ(req("qt","/get","id","11", "fl","id", "fq","id:11")
        ,"=={doc:{id:'11'}}"
    );

    // assert that the same realtime searcher is still in effect (i.e. that we didn't
    // open a new searcher when we didn't have to).
    RefCounted<SolrIndexSearcher> realtimeHolder2 = req.getCore().getRealtimeSearcher();
    assertEquals(realtimeHolder.get(), realtimeHolder2.get());  // Autocommit could possibly cause this to fail?
    realtimeHolder2.decref();

    // filter most likely different segment
    assertJQ(req("qt","/get","id","12", "fl","id", "fq","id:11")
        ,"=={doc:null}"
    );

    // filter most likely same different segment
    assertJQ(req("qt","/get","id","12", "fl","id", "fq","id:13")
        ,"=={doc:null}"
    );

    assertJQ(req("qt","/get","id","12", "fl","id", "fq","id:12")
        ,"=={doc:{id:'12'}}"
    );

    assertU(adoc("id", "14"));
    assertU(adoc("id", "15"));

    // id list, with some in index and some not, first id from index. Also test mutiple fq params.
    assertJQ(req("qt","/get","ids","12,14,13,15", "fl","id", "fq","id:[10 TO 14]", "fq","id:[13 TO 19]")
        ,"/response/docs==[{id:'14'},{id:'13'}]"
    );

    assertU(adoc("id", "16"));
    assertU(adoc("id", "17"));

    // id list, with some in index and some not, first id from tlog
    assertJQ(req("qt","/get","ids","17,16,15,14", "fl","id", "fq","id:[15 TO 16]")
        ,"/response/docs==[{id:'16'},{id:'15'}]"
    );

    // more complex filter
    assertJQ(req("qt","/get","ids","17,16,15,14", "fl","id", "fq","{!frange l=15 u=16}id")
        ,"/response/docs==[{id:'16'},{id:'15'}]"
    );

    assertJQ(req("qt","/get","ids","15,14", "fl","id", "fq","-id:15")
            ,"/response/docs==[{id:'14'}]"
    );
    assertJQ(req("qt","/get","ids","17,16,15,14", "fl","id", "fq","-id:[15 TO 17]")
            ,"/response/docs==[{id:'14'}]"
    );

    realtimeHolder.decref();
    req.close();

  }


  @Test
  public void testVersions() throws Exception {
    clearIndex();
    assertU(commit());

    long version = addAndGetVersion(sdoc("id","1") , null);

    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    // test version is there from rtg
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // test version is there from the index
    assertU(commit());
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // simulate an update from the leader
    version += 10;
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

    // test version is there from rtg
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // simulate reordering: test that a version less than that does not take affect
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version - 1))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

    // test that version hasn't changed
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // simulate reordering: test that a delete w/ version less than that does not take affect
    // TODO: also allow passing version on delete instead of on URL?
    updateJ(jsonDelId("1"), params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",Long.toString(version - 1)));

    // test that version hasn't changed
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // make sure reordering detection also works after a commit
    assertU(commit());

    // simulate reordering: test that a version less than that does not take affect
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version - 1))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

    // test that version hasn't changed
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // simulate reordering: test that a delete w/ version less than that does not take affect
    updateJ(jsonDelId("1"), params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",Long.toString(version - 1)));

    // test that version hasn't changed
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // now simulate a normal delete from the leader
    version += 5;
    updateJ(jsonDelId("1"), params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",Long.toString(version)));

    // make sure a reordered add doesn't take affect.
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version - 1))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

    // test that it's still deleted
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':null}"
    );

    // test that we can remember the version of a delete after a commit
    assertU(commit());

    // make sure a reordered add doesn't take affect.
    long version2 = deleteByQueryAndGetVersion("id:2", null);

    // test that it's still deleted
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':null}"
    );

    version = addAndGetVersion(sdoc("id","2"), null);
    version2 = deleteByQueryAndGetVersion("id:2", null);
    assertTrue(Math.abs(version2) > version );

    // test that it's deleted
    assertJQ(req("qt","/get","id","2")
        ,"=={'doc':null}");


    version2 = Math.abs(version2) + 1000;
    updateJ(jsonAdd(sdoc("id","3", "_version_",Long.toString(version2+100))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
    updateJ(jsonAdd(sdoc("id","4", "_version_",Long.toString(version2+200))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

    // this should only affect id:3 so far
    deleteByQueryAndGetVersion("id:(3 4 5 6)", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",Long.toString(-(version2+150))) );

    assertJQ(req("qt","/get","id","3"),"=={'doc':null}");
    assertJQ(req("qt","/get","id","4", "fl","id"),"=={'doc':{'id':'4'}}");

    updateJ(jsonAdd(sdoc("id","5", "_version_",Long.toString(version2+201))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
    updateJ(jsonAdd(sdoc("id","6", "_version_",Long.toString(version2+101))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

   // the DBQ should also have caused id:6 to be removed
    assertJQ(req("qt","/get","id","5", "fl","id"),"=={'doc':{'id':'5'}}");
    assertJQ(req("qt","/get","id","6"),"=={'doc':null}");

    assertU(commit());

  }

  @Test
  public void testOptimisticLocking() throws Exception {
    clearIndex();
    assertU(commit());

    final long version = addAndGetVersion(sdoc("id","1") , null);
    long version2;

    // try version added directly on doc
    SolrException se = expectThrows(SolrException.class, "version should cause an error",
        () -> addAndGetVersion(sdoc("id","1", "_version_", Long.toString(version-1)), null));
    assertEquals("version should cause a conflict", 409, se.code());

    // try version added as a parameter on the request
    se = expectThrows(SolrException.class, "version should cause an error",
        () -> addAndGetVersion(sdoc("id","1"), params("_version_", Long.toString(version-1))));
    assertEquals("version should cause a conflict", 409, se.code());

    // try an add specifying a negative version
    se = expectThrows(SolrException.class, "negative version should cause a conflict",
        () -> addAndGetVersion(sdoc("id","1"), params("_version_", Long.toString(-version))));
    assertEquals("version should cause a conflict", 409, se.code());

    // try an add with a greater version
    se = expectThrows(SolrException.class, "greater version should cause a conflict",
        () -> addAndGetVersion(sdoc("id","1"), params("_version_", Long.toString(version+random().nextInt(1000)+1))));
    assertEquals("version should cause a conflict", 409, se.code());

    //
    // deletes
    //

    // try a delete with version on the request
    se = expectThrows(SolrException.class, "version should cause an error",
        () -> deleteAndGetVersion("1", params("_version_", Long.toString(version-1))));
    assertEquals("version should cause a conflict", 409, se.code());

    // try a delete with a negative version
    se = expectThrows(SolrException.class, "negative version should cause an error",
        () -> deleteAndGetVersion("1", params("_version_", Long.toString(-version))));
    assertEquals("version should cause a conflict", 409, se.code());

    // try a delete with a greater version
    se = expectThrows(SolrException.class, "greater version should cause an error",
        () -> deleteAndGetVersion("1", params("_version_", Long.toString(version+random().nextInt(1000)+1))));
    assertEquals("version should cause a conflict", 409, se.code());

    // try a delete of a document that doesn't exist, specifying a specific version
    se = expectThrows(SolrException.class, "document does not exist should cause an error",
        () -> deleteAndGetVersion("I_do_not_exist", params("_version_", Long.toString(version))));
    assertEquals("version should cause a conflict", 409, se.code());


    // try a delete of a document that doesn't exist, specifying that it should not
    version2 = deleteAndGetVersion("I_do_not_exist", params("_version_", Long.toString(-1)));
    assertTrue(version2 < 0);

    // overwrite the document
    version2 = addAndGetVersion(sdoc("id","1", "_version_", Long.toString(version)), null);
    assertTrue(version2 > version);

    // overwriting the previous version should now fail
    se = expectThrows(SolrException.class, "overwriting previous version should fail",
        () -> addAndGetVersion(sdoc("id","1"), params("_version_", Long.toString(version))));
    assertEquals(409, se.code());

    // deleting the previous version should now fail
    se = expectThrows(SolrException.class, "deleting the previous version should now fail",
        () -> deleteAndGetVersion("1", params("_version_", Long.toString(version))));
    assertEquals(409, se.code());

    final long prevVersion = version2;

    // deleting the current version should work
    version2 = deleteAndGetVersion("1", params("_version_", Long.toString(prevVersion)));

    // overwriting the previous existing doc should now fail (since it was deleted)
    se = expectThrows(SolrException.class, "overwriting the previous existing doc should now fail (since it was deleted)",
        () -> addAndGetVersion(sdoc("id","1"), params("_version_", Long.toString(prevVersion))));
    assertEquals(409, se.code());

    // deleting the previous existing doc should now fail (since it was deleted)
    se = expectThrows(SolrException.class, "deleting the previous existing doc should now fail (since it was deleted)",
        () -> deleteAndGetVersion("1", params("_version_", Long.toString(prevVersion))));
    assertEquals(409, se.code());

    // overwriting a negative version should work
    version2 = addAndGetVersion(sdoc("id","1"), params("_version_", Long.toString(-(prevVersion-1))));
    assertTrue(version2 > version);
    long lastVersion = version2;

    // sanity test that we see the right version via rtg
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + lastVersion + "}}"
    );
  }


  /***
    @Test
    public void testGetRealtime() throws Exception {
      SolrQueryRequest sr1 = req("q","foo");
      IndexReader r1 = sr1.getCore().getRealtimeReader();

      assertU(adoc("id","1"));

      IndexReader r2 = sr1.getCore().getRealtimeReader();
      assertNotSame(r1, r2);
      int refcount = r2.getRefCount();

      // make sure a new reader wasn't opened
      IndexReader r3 = sr1.getCore().getRealtimeReader();
      assertSame(r2, r3);
      assertEquals(refcount+1, r3.getRefCount());

      assertU(commit());

      // this is not critical, but currently a commit does not refresh the reader
      // if nothing has changed
      IndexReader r4 = sr1.getCore().getRealtimeReader();
      assertEquals(refcount+2, r4.getRefCount());


      r1.decRef();
      r2.decRef();
      r3.decRef();
      r4.decRef();
      sr1.close();
    }
    ***/


  @Test
  public void testStressGetRealtime() throws Exception {
    clearIndex();
    assertU(commit());

    // req().getCore().getUpdateHandler().getIndexWriterProvider().getIndexWriter(req().getCore()).setInfoStream(System.out);

    final int commitPercent = 5 + random().nextInt(20);
    final int softCommitPercent = 30+random().nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random().nextInt(25);
    final int deleteByQueryPercent = 1+random().nextInt(5);
    final int optimisticPercent = 1+random().nextInt(50);    // percent change that an update uses optimistic locking
    final int optimisticCorrectPercent = 25+random().nextInt(70);    // percent change that a version specified will be correct
    final int filteredGetPercent = random().nextInt( random().nextInt(20)+1 );   // percent of time that a get will be filtered... we normally don't want too high.
    final int ndocs = 5 + (random().nextBoolean() ? random().nextInt(25) : random().nextInt(200));
    int nWriteThreads = 5 + random().nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time...

        // query variables
    final int percentRealtimeQuery = 60;
    final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
    int nReadThreads = 5 + random().nextInt(25);


    verbose("commitPercent=", commitPercent);
    verbose("softCommitPercent=",softCommitPercent);
    verbose("deletePercent=",deletePercent);
    verbose("deleteByQueryPercent=", deleteByQueryPercent);
    verbose("ndocs=", ndocs);
    verbose("nWriteThreads=", nWriteThreads);
    verbose("nReadThreads=", nReadThreads);
    verbose("percentRealtimeQuery=", percentRealtimeQuery);
    verbose("maxConcurrentCommits=", maxConcurrentCommits);
    verbose("operations=", operations);


    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<>();

    for (int i=0; i<nWriteThreads; i++) {
      Thread thread = new Thread("WRITER"+i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
          while (operations.get() > 0) {
            int oper = rand.nextInt(100);

            if (oper < commitPercent) {
              if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                Map<Integer,DocInfo> newCommittedModel;
                long version;

                synchronized(TestRealTimeGet.this) {
                  newCommittedModel = new HashMap<>(model);  // take a snapshot
                  version = snapshotCount++;
                  verbose("took snapshot version=",version);
                }

                if (rand.nextInt(100) < softCommitPercent) {
                  verbose("softCommit start");
                  assertU(TestHarness.commit("softCommit","true"));
                  verbose("softCommit end");
                } else {
                  verbose("hardCommit start");
                  assertU(commit());
                  verbose("hardCommit end");
                }

                synchronized(TestRealTimeGet.this) {
                  // install this model snapshot only if it's newer than the current one
                  if (version >= committedModelClock) {
                    if (VERBOSE) {
                      verbose("installing new committedModel version="+committedModelClock);
                    }
                    committedModel = newCommittedModel;
                    committedModelClock = version;
                  }
                }
              }
              numCommitting.decrementAndGet();
              continue;
            }


            int id = rand.nextInt(ndocs);
            Object sync = syncArr[id];

            // set the lastId before we actually change it sometimes to try and
            // uncover more race conditions between writing and reading
            boolean before = rand.nextBoolean();
            if (before) {
              lastId = id;
            }

            // We can't concurrently update the same document and retain our invariants of increasing values
            // since we can't guarantee what order the updates will be executed.
            // Even with versions, we can't remove the sync because increasing versions does not mean increasing vals.
            synchronized (sync) {
              DocInfo info = model.get(id);

              long val = info.val;
              long nextVal = Math.abs(val)+1;

              if (oper < commitPercent + deletePercent) {
                boolean opt = rand.nextInt() < optimisticPercent;
                boolean correct = opt ? rand.nextInt() < optimisticCorrectPercent : false;
                long badVersion = correct ? 0 : badVersion(rand, info.version);

                if (VERBOSE) {
                  if (!opt) {
                    verbose("deleting id",id,"val=",nextVal);
                  } else {
                    verbose("deleting id",id,"val=",nextVal, "existing_version=",info.version,  (correct ? "" : (" bad_version=" + badVersion)));
                  }
                }

                // assertU("<delete><id>" + id + "</id></delete>");
                Long version = null;

                if (opt) {
                  if (correct) {
                    version = deleteAndGetVersion(Integer.toString(id), params("_version_", Long.toString(info.version)));
                  } else {
                    SolrException se = expectThrows(SolrException.class, "should not get random version",
                        () -> deleteAndGetVersion(Integer.toString(id), params("_version_", Long.toString(badVersion))));
                    assertEquals(409, se.code());
                  }
                } else {
                  version = deleteAndGetVersion(Integer.toString(id), null);
                }

                if (version != null) {
                  model.put(id, new DocInfo(version, -nextVal));
                }

                if (VERBOSE) {
                  verbose("deleting id", id, "val=",nextVal,"DONE");
                }
              } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                if (VERBOSE) {
                  verbose("deleteByQuery id ",id, "val=",nextVal);
                }

                assertU("<delete><query>id:" + id + "</query></delete>");
                model.put(id, new DocInfo(-1L, -nextVal));
                if (VERBOSE) {
                  verbose("deleteByQuery id",id, "val=",nextVal,"DONE");
                }
              } else {
                boolean opt = rand.nextInt() < optimisticPercent;
                boolean correct = opt ? rand.nextInt() < optimisticCorrectPercent : false;
                long badVersion = correct ? 0 : badVersion(rand, info.version);

                if (VERBOSE) {
                  if (!opt) {
                    verbose("adding id",id,"val=",nextVal);
                  } else {
                    verbose("adding id",id,"val=",nextVal, "existing_version=",info.version,  (correct ? "" : (" bad_version=" + badVersion)));
                  }
                }

                Long version = null;
                SolrInputDocument sd = sdoc("id", Integer.toString(id), FIELD, Long.toString(nextVal));

                if (opt) {
                  if (correct) {
                    version = addAndGetVersion(sd, params("_version_", Long.toString(info.version)));
                  } else {
                    SolrException se = expectThrows(SolrException.class, "should not get bad version",
                        () -> addAndGetVersion(sd, params("_version_", Long.toString(badVersion))));
                    assertEquals(409, se.code());
                  }
                } else {
                  version = addAndGetVersion(sd, null);
                }


                if (version != null) {
                  model.put(id, new DocInfo(version, nextVal));
                }

                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal,"DONE");
                }

              }
            }   // end sync

            if (!before) {
              lastId = id;
            }
          }
        } catch (Throwable e) {
          operations.set(-1L);
          throw new RuntimeException(e);
        }
        }
      };

      threads.add(thread);
    }


    for (int i=0; i<nReadThreads; i++) {
      Thread thread = new Thread("READER"+i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              boolean realTime = rand.nextInt(100) < percentRealtimeQuery;
              DocInfo info;

              if (realTime) {
                info = model.get(id);
              } else {
                synchronized(TestRealTimeGet.this) {
                  info = committedModel.get(id);
                }
              }

              if (VERBOSE) {
                verbose("querying id", id);
              }

              boolean filteredOut = false;
              SolrQueryRequest sreq;
              if (realTime) {
                ModifiableSolrParams p = params("wt","json", "qt","/get", "ids",Integer.toString(id));
                if (rand.nextInt(100) < filteredGetPercent) {
                  int idToFilter = rand.nextBoolean() ? id : rand.nextInt(ndocs);
                  filteredOut = idToFilter != id;
                  p.add("fq", "id:"+idToFilter);
                }
                sreq = req(p);
              } else {
                sreq = req("wt","json", "q","id:"+Integer.toString(id), "omitHeader","true");
              }

              String response = h.query(sreq);
              @SuppressWarnings({"rawtypes"})
              Map rsp = (Map) Utils.fromJSONString(response);
              @SuppressWarnings({"rawtypes"})
              List doclist = (List)(((Map)rsp.get("response")).get("docs"));
              if (doclist.size() == 0) {
                // there's no info we can get back with a delete, so not much we can check without further synchronization
                // This is also correct when filteredOut==true
              } else {
                assertEquals(1, doclist.size());
                long foundVal = (Long)(((Map)doclist.get(0)).get(FIELD));
                long foundVer = (Long)(((Map)doclist.get(0)).get("_version_"));
                if (filteredOut || foundVal < Math.abs(info.val)
                    || (foundVer == info.version && foundVal != info.val) ) {    // if the version matches, the val must
                  verbose("ERROR, id=", id, "found=",response,"model",info);
                  assertTrue(false);
                }
              }
            }
          }
        catch (Throwable e) {
          operations.set(-1L);
          throw new RuntimeException(e);
        }
      }
      };

      threads.add(thread);
    }


    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

  }


}
