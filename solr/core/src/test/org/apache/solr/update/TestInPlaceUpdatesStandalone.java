
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.processor.AtomicUpdateDocumentMerger;
import org.apache.solr.util.RefCounted;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.update.UpdateLogTest.buildAddUpdateCommand;
import static org.hamcrest.core.StringContains.containsString;


/**
 * Tests the in-place updates (docValues updates) for a standalone Solr instance.
 */
public class TestInPlaceUpdatesStandalone extends SolrTestCaseJ4 {
  private static SolrClient client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // we need consistent segments that aren't re-ordered on merge because we're
    // asserting inplace updates happen by checking the internal [docid]
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());

    initCore("solrconfig-tlog.xml", "schema-inplace-updates.xml");

    // sanity check that autocommits are disabled
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxDocs);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxDocs);

    // assert that NoMergePolicy was chosen
    RefCounted<IndexWriter> iw = h.getCore().getSolrCoreState().getIndexWriter(h.getCore());
    try {
      IndexWriter writer = iw.get();
      assertTrue("Actual merge policy is: " + writer.getConfig().getMergePolicy(),
          writer.getConfig().getMergePolicy() instanceof NoMergePolicy); 
    } finally {
      iw.decref();
    }

    // validate that the schema was not changed to an unexpected state
    IndexSchema schema = h.getCore().getLatestSchema();
    for (String fieldName : Arrays.asList("_version_",
                                          "inplace_l_dvo",
                                          "inplace_updatable_float",
                                          "inplace_updatable_int", 
                                          "inplace_updatable_float_with_default",
                                          "inplace_updatable_int_with_default")) {
      // these fields must only be using docValues to support inplace updates
      SchemaField field = schema.getField(fieldName);
      assertTrue(field.toString(),
                 field.hasDocValues() && ! field.indexed() && ! field.stored());
    }
    for (String fieldName : Arrays.asList("title_s", "regular_l", "stored_i")) {
      // these fields must support atomic updates, but not inplace updates (ie: stored)
      SchemaField field = schema.getField(fieldName);
      assertTrue(field.toString(), field.stored());
    }    

    // Don't close this client, it would shutdown the CoreContainer
    client = new EmbeddedSolrServer(h.getCoreContainer(), h.coreName);
  }

  @AfterClass
  public static void afterClass() {
    client = null;
  }

  @Before
  public void deleteAllAndCommit() throws Exception {
    clearIndex();
    assertU(commit("softCommit", "false"));
  }

  @Test
  public void testUpdateBadRequest() throws Exception {
    final long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first", "inplace_updatable_float", 41), null);
    assertU(commit());

    // invalid value with set operation
    SolrException e = expectThrows(SolrException.class,
        () -> addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", "NOT_NUMBER")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    MatcherAssert.assertThat(e.getMessage(), containsString("For input string: \"NOT_NUMBER\""));

    // invalid value with inc operation
    e = expectThrows(SolrException.class,
        () -> addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", "NOT_NUMBER")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    MatcherAssert.assertThat(e.getMessage(), containsString("For input string: \"NOT_NUMBER\""));

    // inc op with null value
    e = expectThrows(SolrException.class,
        () -> addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", null)));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    MatcherAssert.assertThat(e.getMessage(), containsString("Invalid input 'null' for field inplace_updatable_float"));

    e = expectThrows(SolrException.class,
        () -> addAndAssertVersion(version1, "id", "1", "inplace_updatable_float",
            map("inc", new ArrayList<>(Collections.singletonList(123)))));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    MatcherAssert.assertThat(e.getMessage(), containsString("Invalid input '[123]' for field inplace_updatable_float"));

    // regular atomic update should fail if user says they only want in-place atomic updates...
    e = expectThrows(SolrException.class,
                     () -> addAndGetVersion(sdoc("id", "1", "regular_l", map("inc", 1)),
                                            params(UpdateParams.REQUIRE_PARTIAL_DOC_UPDATES_INPLACE, "true")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    MatcherAssert.assertThat(e.getMessage(), containsString("Unable to update doc in-place: 1"));
  }

  @Test
  public void testUpdatingDocValues() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first", "inplace_updatable_float", 41), null);
    long version2 = addAndGetVersion(sdoc("id", "2", "title_s", "second", "inplace_updatable_float", 42), null);
    long version3 = addAndGetVersion(sdoc("id", "3", "title_s", "third", "inplace_updatable_float", 43), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='3']");

    // the reason we're fetching these docids is to validate that the subsequent updates 
    // are done in place and don't cause the docids to change
    int docid1 = getDocId("1");
    int docid2 = getDocId("2");
    int docid3 = getDocId("3");

    // Check docValues were "set"
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));
    version2 = addAndAssertVersion(version2, "id", "2", "inplace_updatable_float", map("set", 300));
    version3 = addAndAssertVersion(version3, "id", "3", "inplace_updatable_float", map("set", 100));
    assertU(commit("softCommit", "false"));

    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='3']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='200.0']",
        "//result/doc[2]/float[@name='inplace_updatable_float'][.='300.0']",
        "//result/doc[3]/float[@name='inplace_updatable_float'][.='100.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[2]/long[@name='_version_'][.='"+version2+"']",
        "//result/doc[3]/long[@name='_version_'][.='"+version3+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']",
        "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']",
        "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']"
        );

    // Check docValues are "inc"ed
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", 1));
    version2 = addAndAssertVersion(version2, "id", "2", "inplace_updatable_float", map("inc", -2));
    version3 = addAndAssertVersion(version3, "id", "3", "inplace_updatable_float", map("inc", 3));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='3']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='201.0']",
        "//result/doc[2]/float[@name='inplace_updatable_float'][.='298.0']",
        "//result/doc[3]/float[@name='inplace_updatable_float'][.='103.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[2]/long[@name='_version_'][.='"+version2+"']",
        "//result/doc[3]/long[@name='_version_'][.='"+version3+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']",
        "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']",
        "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']"
        );

    // Check back to back "inc"s are working (off the transaction log)
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", 1));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", 2)); // new value should be 204
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:1", "fl", "*,[docid]"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='204.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']");

    // Now let the document be atomically updated (non-inplace), ensure the old docvalue is part of new doc
    version1 = addAndAssertVersion(version1, "id", "1", "title_s", map("set", "new first"));
    assertU(commit("softCommit", "false"));
    int newDocid1 = getDocId("1");
    assertTrue(newDocid1 != docid1);
    docid1 = newDocid1;

    assertQ(req("q", "id:1"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='204.0']",
        "//result/doc[1]/str[@name='title_s'][.='new first']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']");

    // Check if atomic update with "inc" to a docValue works
    version2 = addAndAssertVersion(version2, "id", "2", "title_s", map("set", "new second"), "inplace_updatable_float", map("inc", 2));
    assertU(commit("softCommit", "false"));
    int newDocid2 = getDocId("2");
    assertTrue(newDocid2 != docid2);
    docid2 = newDocid2;

    assertQ(req("q", "id:2"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='300.0']",
        "//result/doc[1]/str[@name='title_s'][.='new second']",
        "//result/doc[1]/long[@name='_version_'][.='"+version2+"']");

    // Check if docvalue "inc" update works for a newly created document, which is not yet committed
    // Case1: docvalue was supplied during add of new document
    long version4 = addAndGetVersion(sdoc("id", "4", "title_s", "fourth", "inplace_updatable_float", "400"), params());
    version4 = addAndAssertVersion(version4, "id", "4", "inplace_updatable_float", map("inc", 1));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:4"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='401.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version4+"']");

    // Check if docvalue "inc" update works for a newly created document, which is not yet committed
    // Case2: docvalue was not supplied during add of new document, should assume default
    long version5 = addAndGetVersion(sdoc("id", "5", "title_s", "fifth"), params());
    version5 = addAndAssertVersion(version5, "id", "5", "inplace_updatable_float", map("inc", 1));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:5"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='1.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version5+"']");

    // Check if docvalue "set" update works for a newly created document, which is not yet committed
    long version6 = addAndGetVersion(sdoc("id", "6", "title_s", "sixth"), params());
    version6 = addAndAssertVersion(version6, "id", "6", "inplace_updatable_float", map("set", 600));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:6"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='600.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version6+"']");

    // Check optimistic concurrency works
    long v20 = addAndGetVersion(sdoc("id", "20", "title_s","first", "inplace_updatable_float", 100), params());    
    SolrException exception = expectThrows(SolrException.class, () -> {
      addAndGetVersion(sdoc("id","20", "_version_", -1, "inplace_updatable_float", map("inc", 1)), null);
    });
    assertEquals(exception.toString(), SolrException.ErrorCode.CONFLICT.code, exception.code());
    assertThat(exception.getMessage(), containsString("expected=-1"));
    assertThat(exception.getMessage(), containsString("actual="+v20));


    long oldV20 = v20;
    v20 = addAndAssertVersion(v20, "id","20", "_version_", v20, "inplace_updatable_float", map("inc", 1));
    exception = expectThrows(SolrException.class, () -> {
      addAndGetVersion(sdoc("id","20", "_version_", oldV20, "inplace_updatable_float", map("inc", 1)), null);
    });
    assertEquals(exception.toString(), SolrException.ErrorCode.CONFLICT.code, exception.code());
    assertThat(exception.getMessage(), containsString("expected="+oldV20));
    assertThat(exception.getMessage(), containsString("actual="+v20));

    v20 = addAndAssertVersion(v20, "id","20", "_version_", v20, "inplace_updatable_float", map("inc", 1));
    // RTG before a commit
    assertJQ(req("qt","/get", "id","20", "fl","id,inplace_updatable_float,_version_"),
        "=={'doc':{'id':'20', 'inplace_updatable_float':" + 102.0 + ",'_version_':" + v20 + "}}");
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:20"), 
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='102.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+v20+"']");

    // Check if updated DVs can be used for search
    assertQ(req("q", "inplace_updatable_float:102"), 
        "//result/doc[1]/str[@name='id'][.='20']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='102.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+v20+"']");

    // Check if updated DVs can be used for sorting
    assertQ(req("q", "*:*", "sort", "inplace_updatable_float asc"), 
        "//result/doc[4]/str[@name='id'][.='1']",
        "//result/doc[4]/float[@name='inplace_updatable_float'][.='204.0']",

        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[5]/float[@name='inplace_updatable_float'][.='300.0']",

        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[3]/float[@name='inplace_updatable_float'][.='103.0']",

        "//result/doc[6]/str[@name='id'][.='4']",
        "//result/doc[6]/float[@name='inplace_updatable_float'][.='401.0']",

        "//result/doc[1]/str[@name='id'][.='5']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='1.0']",

        "//result/doc[7]/str[@name='id'][.='6']",
        "//result/doc[7]/float[@name='inplace_updatable_float'][.='600.0']",

        "//result/doc[2]/str[@name='id'][.='20']",
        "//result/doc[2]/float[@name='inplace_updatable_float'][.='102.0']");
  }

  public void testUserRequestedFailIfNotInPlace() throws Exception {
    final SolrParams require_inplace = params(UpdateParams.REQUIRE_PARTIAL_DOC_UPDATES_INPLACE, "true");
    long v;
    
    // regular updates should be ok even if require_inplace params are used,
    // that way true "adds" wil work even if require_inplace params are in in "/update" defaults or invariants...
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first", "regular_l", 1, "inplace_updatable_float", 41), require_inplace);
    long version2 = addAndGetVersion(sdoc("id", "2", "title_s", "second", "regular_l", 2, "inplace_updatable_float", 42), require_inplace);
    long version3 = addAndGetVersion(sdoc("id", "3", "title_s", "third", "regular_l", 3, "inplace_updatable_float", 43), require_inplace);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='3']");

    // the reason we're fetching these docids is to validate that the subsequent updates 
    // are done in place and don't cause the docids to change
    final int docid1 = getDocId("1");
    final int docid2 = getDocId("2");
    final int docid3 = getDocId("3");

    // this atomic update should be done in place...
    v = addAndGetVersion(sdoc("id", "2", "inplace_updatable_float", map("inc", 2)), require_inplace);
    assertTrue(v > version2);
    version2 = v;

    // this atomic update should also be done in place, even though the user didn't insist on it...
    v = addAndGetVersion(sdoc("id", "3", "inplace_updatable_float", map("inc", 3)), params());
    assertTrue(v > version3);
    version3 = v;

    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]")
            , "//*[@numFound='3']"
            , "//result/doc[1]/long[@name='regular_l'][.='1']"
            , "//result/doc[2]/long[@name='regular_l'][.='2']"
            , "//result/doc[3]/long[@name='regular_l'][.='3']"
            , "//result/doc[1]/float[@name='inplace_updatable_float'][.='41.0']"
            , "//result/doc[2]/float[@name='inplace_updatable_float'][.='44.0']"
            , "//result/doc[3]/float[@name='inplace_updatable_float'][.='46.0']"
            , "//result/doc[1]/long[@name='_version_'][.='"+version1+"']"
            , "//result/doc[2]/long[@name='_version_'][.='"+version2+"']"
            , "//result/doc[3]/long[@name='_version_'][.='"+version3+"']"
            , "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']"
            , "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']"
            , "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']"
            );

    // this is an atomic update, but it can't be done in-place, so it should fail w/o affecting index...
    SolrException e = expectThrows(SolrException.class,
                                   () -> addAndGetVersion(sdoc("id", "1", "regular_l", map("inc", 1)),
                                                          require_inplace));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    MatcherAssert.assertThat(e.getMessage(), containsString("Unable to update doc in-place: 1"));

    // data in solr should be unchanged after failed attempt at non-inplace atomic update...
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]")
            , "//*[@numFound='3']"
            , "//result/doc[1]/long[@name='regular_l'][.='1']"
            , "//result/doc[2]/long[@name='regular_l'][.='2']"
            , "//result/doc[3]/long[@name='regular_l'][.='3']"
            , "//result/doc[1]/float[@name='inplace_updatable_float'][.='41.0']"
            , "//result/doc[2]/float[@name='inplace_updatable_float'][.='44.0']"
            , "//result/doc[3]/float[@name='inplace_updatable_float'][.='46.0']"
            , "//result/doc[1]/long[@name='_version_'][.='"+version1+"']"
            , "//result/doc[2]/long[@name='_version_'][.='"+version2+"']"
            , "//result/doc[3]/long[@name='_version_'][.='"+version3+"']"
            , "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']"
            , "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']"
            , "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']"
            );

    
    // the same atomic update w/o require_inplace params should proceed, and can modify the docid(s)
    // (but we don't assert that, since it the merge policy might kick in
    v = addAndGetVersion(sdoc("id", "1", "regular_l", map("inc", 100)), params());
    assertTrue(v > version1);
    version1 = v;

    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*")
            , "//*[@numFound='3']"
            , "//result/doc[1]/long[@name='regular_l'][.='101']"
            , "//result/doc[2]/long[@name='regular_l'][.='2']"
            , "//result/doc[3]/long[@name='regular_l'][.='3']"
            , "//result/doc[1]/float[@name='inplace_updatable_float'][.='41.0']"
            , "//result/doc[2]/float[@name='inplace_updatable_float'][.='44.0']"
            , "//result/doc[3]/float[@name='inplace_updatable_float'][.='46.0']"
            , "//result/doc[1]/long[@name='_version_'][.='"+version1+"']"
            , "//result/doc[2]/long[@name='_version_'][.='"+version2+"']"
            , "//result/doc[3]/long[@name='_version_'][.='"+version3+"']"
            );

    // a regular old re-indexing of a document should also succeed, even w/require_inplace, since it's not ant atomic update
    v = addAndGetVersion(sdoc("id", "1", "regular_l", "999"), require_inplace);
    assertTrue(v > version1);
    version1 = v;
    
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*")
            , "//*[@numFound='3']"
            , "//result/doc[1]/long[@name='regular_l'][.='999']"
            , "//result/doc[2]/long[@name='regular_l'][.='2']"
            , "//result/doc[3]/long[@name='regular_l'][.='3']"
            , "0=count(//result/doc[1]/float[@name='inplace_updatable_float'])" // not in new doc
            , "//result/doc[2]/float[@name='inplace_updatable_float'][.='44.0']"
            , "//result/doc[3]/float[@name='inplace_updatable_float'][.='46.0']"
            , "//result/doc[1]/long[@name='_version_'][.='"+version1+"']"
            , "//result/doc[2]/long[@name='_version_'][.='"+version2+"']"
            , "//result/doc[3]/long[@name='_version_'][.='"+version3+"']"
            );

  }
  
  @Test
  public void testUpdatingFieldNotPresentInDoc() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first"), null);
    long version2 = addAndGetVersion(sdoc("id", "2", "title_s", "second"), null);
    long version3 = addAndGetVersion(sdoc("id", "3", "title_s", "third"), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='3']");

    // subsequent updates shouldn't cause docid changes
    int docid1 = getDocId("1");
    int docid2 = getDocId("2");
    int docid3 = getDocId("3");

    // updating fields which are not present in the document
    // tests both set and inc with different fields
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));
    version2 = addAndAssertVersion(version2, "id", "2", "inplace_updatable_float", map("inc", 100));
    version3 = addAndAssertVersion(version3, "id", "3", "inplace_updatable_float", map("set", 300));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("set", 300));
    assertU(commit("softCommit", "false"));

    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='3']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='200.0']",
        "//result/doc[1]/int[@name='inplace_updatable_int'][.='300']",
        "//result/doc[2]/float[@name='inplace_updatable_float'][.='100.0']",
        "//result/doc[3]/float[@name='inplace_updatable_float'][.='300.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[2]/long[@name='_version_'][.='"+version2+"']",
        "//result/doc[3]/long[@name='_version_'][.='"+version3+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']",
        "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']",
        "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']"
    );

    // adding new field which is not present in any docs but matches dynamic field rule
    // and satisfies inplace condition should be treated as inplace update
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_i_dvo", map("set", 200));
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "id:1", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='1']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='200.0']",
        "//result/doc[1]/int[@name='inplace_updatable_int'][.='300']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']",
        "//result/doc[1]/int[@name='inplace_updatable_i_dvo'][.='200']"
        );

    // delete everything
    deleteAllAndCommit();

    // test for document with child documents
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("title_s", "parent");

    SolrInputDocument child1 = new SolrInputDocument();
    child1.setField("id", "1_1");
    child1.setField("title_s", "child1");
    SolrInputDocument child2 = new SolrInputDocument();
    child2.setField("id", "1_2");
    child2.setField("title_s", "child2");

    doc.addChildDocument(child1);
    doc.addChildDocument(child2);
    long version = addAndGetVersion(doc, null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='3']");

    int parentDocId = getDocId("1");
    int childDocid1 = getDocId("1_1");
    int childDocid2 = getDocId("1_2");
    version = addAndAssertVersion(version, "id", "1", "inplace_updatable_float", map("set", 200));
    version = addAndAssertVersion(version, "id", "1", "inplace_updatable_int", map("inc", 300));
    assertU(commit("softCommit", "false"));

    // first child docs would be returned followed by parent doc
    assertQ(req("q", "*:*", "fl", "*,[docid]"),
        "//*[@numFound='3']",
        "//result/doc[3]/float[@name='inplace_updatable_float'][.='200.0']",
        "//result/doc[3]/int[@name='inplace_updatable_int'][.='300']",
        "//result/doc[3]/int[@name='[docid]'][.='"+parentDocId+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+childDocid1+"']",
        "//result/doc[2]/int[@name='[docid]'][.='"+childDocid2+"']"
    );
  }


  @Test
  public void testUpdateTwoDifferentFields() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first", "inplace_updatable_float", 42), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='1']");

    int docid1 = getDocId("1");

    // Check docValues were "set"
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("set", 10));
    assertU(commit("softCommit", "false"));

    assertU(commit("softCommit", "false"));

    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='1']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='200.0']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']"
        );

    // two different update commands, updating each of the fields separately
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("inc", 1));
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", 1));
    // same update command, updating both the fields together
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_int", map("inc", 1),
        "inplace_updatable_float", map("inc", 1));

    if (random().nextBoolean()) {
      assertU(commit("softCommit", "false"));
      assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
          "//*[@numFound='1']",
          "//result/doc[1]/float[@name='inplace_updatable_float'][.='202.0']",
          "//result/doc[1]/int[@name='inplace_updatable_int'][.='12']",
          "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
          "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']"
          );
    } 

    // RTG
    assertJQ(req("qt","/get", "id","1", "fl","id,inplace_updatable_float,inplace_updatable_int"),
        "=={'doc':{'id':'1', 'inplace_updatable_float':" + 202.0 + ",'inplace_updatable_int':" + 12 + "}}");

  }

  @Test
  public void testUpdateWithValueNull() throws Exception {
    long doc = addAndGetVersion(sdoc("id", "1", "title_s", "first", "inplace_updatable_float", 42), null);
    assertU(commit("softCommit", "false"));

    assertQ(req("q", "*:*", "fq", "inplace_updatable_float:[* TO *]"), "//*[@numFound='1']");
    // RTG before update
    assertJQ(req("qt","/get", "id","1", "fl","id,inplace_updatable_float,title_s"),
        "=={'doc':{'id':'1', 'inplace_updatable_float':" + 42.0 + ",'title_s':" + "first" + "}}");

    // set the value to null
    doc = addAndAssertVersion(doc, "id", "1", "inplace_updatable_float", map("set", null));
    assertU(commit("softCommit", "false"));

    // numProducts should be 0
    assertQ(req("q", "*:*", "fq", "inplace_updatable_float:[* TO *]"), "//*[@numFound='0']");
    // after update
    assertJQ(req("qt","/get", "id","1", "fl","id,inplace_updatable_float,title_s"),
        "=={'doc':{'id':'1','title_s':first}}");
  }

  @Test
  public void testDVUpdatesWithDBQofUpdatedValue() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first", "inplace_updatable_float", "0"), null);
    assertU(commit());

    // in-place update
    addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 100), "_version_", version1);

    // DBQ where q=inplace_updatable_float:100
    assertU(delQ("inplace_updatable_float:100"));

    assertU(commit());

    assertQ(req("q", "*:*"), "//*[@numFound='0']");
  }

  @Test
  public void testDVUpdatesWithDelete() throws Exception {
    long version1 = 0;

    for (boolean postAddCommit : Arrays.asList(true, false)) {
      for (boolean delById : Arrays.asList(true, false)) {
        for (boolean postDelCommit : Arrays.asList(true, false)) {
          addAndGetVersion(sdoc("id", "1", "title_s", "first"), params());
          if (postAddCommit) assertU(commit());
          assertU(delById ? delI("1") : delQ("id:1"));
          if (postDelCommit) assertU(commit());
          version1 = addAndGetVersion(sdoc("id", "1", "inplace_updatable_float", map("set", 200)), params());
          // assert current doc#1 doesn't have old value of "title_s"
          assertU(commit());
          assertQ(req("q", "title_s:first", "sort", "id asc", "fl", "*,[docid]"),
              "//*[@numFound='0']");
        }
      }
    }

    // Update to recently deleted (or non-existent) document with a "set" on updatable 
    // field should succeed, since it is executed internally as a full update
    // because AUDM.doInPlaceUpdateMerge() returns false
    assertU(random().nextBoolean()? delI("1"): delQ("id:1"));
    if (random().nextBoolean()) assertU(commit());
    addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));
    assertU(commit());
    assertQ(req("q", "id:1", "sort", "id asc", "fl", "*"),
        "//*[@numFound='1']",
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='200.0']");

    // Another "set" on the same field should be an in-place update 
    int docid1 = getDocId("1");
    addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 300));
    assertU(commit());
    assertQ(req("q", "id:1", "fl", "*,[docid]"),
        "//result/doc[1]/float[@name='inplace_updatable_float'][.='300.0']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']");
  }

  public static long addAndAssertVersion(long expectedCurrentVersion, Object... fields) throws Exception {
    assert 0 < expectedCurrentVersion;
    long currentVersion = addAndGetVersion(sdoc(fields), null);
    assertTrue(currentVersion > expectedCurrentVersion);
    return currentVersion;
  }

  /**
   * Helper method to search for the specified (uniqueKey field) id using <code>fl=[docid]</code> 
   * and return the internal lucene docid.
   */
  private int getDocId(String id) throws Exception {
    SolrDocumentList results = client.query(params("q","id:" + id, "fl", "[docid]")).getResults();
    assertEquals(1, results.getNumFound());
    assertEquals(1, results.size());
    Object docid = results.get(0).getFieldValue("[docid]");
    assertTrue(docid instanceof Integer);
    return ((Integer)docid);
  }

  @Test
  public void testUpdateOfNonExistentDVsShouldNotFail() throws Exception {
    // schema sanity check: assert that the nonexistent_field_i_dvo doesn't exist already
    FieldInfo fi = h.getCore().withSearcher(searcher ->
        searcher.getSlowAtomicReader().getFieldInfos().fieldInfo("nonexistent_field_i_dvo"));
    assertNull(fi);

    // Partial update
    addAndGetVersion(sdoc("id", "0", "nonexistent_field_i_dvo", map("set", "42")), null);

    addAndGetVersion(sdoc("id", "1"), null);
    addAndGetVersion(sdoc("id", "1", "nonexistent_field_i_dvo", map("inc", "1")), null);
    addAndGetVersion(sdoc("id", "1", "nonexistent_field_i_dvo", map("inc", "1")), null);

    assertU(commit());

    assertQ(req("q", "*:*"), "//*[@numFound='2']");    
    assertQ(req("q", "nonexistent_field_i_dvo:42"), "//*[@numFound='1']");    
    assertQ(req("q", "nonexistent_field_i_dvo:2"), "//*[@numFound='1']");    
  }

  @Test
  public void testOnlyPartialUpdatesBetweenCommits() throws Exception {
    // Full updates
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first", "val1_i_dvo", "1", "val2_l_dvo", "1"), params());
    long version2 = addAndGetVersion(sdoc("id", "2", "title_s", "second", "val1_i_dvo", "2", "val2_l_dvo", "2"), params());
    long version3 = addAndGetVersion(sdoc("id", "3", "title_s", "third", "val1_i_dvo", "3", "val2_l_dvo", "3"), params());
    assertU(commit("softCommit", "false"));

    assertQ(req("q", "*:*", "fl", "*,[docid]"), "//*[@numFound='3']");

    int docid1 = getDocId("1");
    int docid2 = getDocId("2");
    int docid3 = getDocId("3");

    int numPartialUpdates = 1 + random().nextInt(5000);
    for (int i=0; i<numPartialUpdates; i++) {
      version1 = addAndAssertVersion(version1, "id", "1", "val1_i_dvo", map("set", i));
      version2 = addAndAssertVersion(version2, "id", "2", "val1_i_dvo", map("inc", 1));
      version3 = addAndAssertVersion(version3, "id", "3", "val1_i_dvo", map("set", i));

      version1 = addAndAssertVersion(version1, "id", "1", "val2_l_dvo", map("set", i));
      version2 = addAndAssertVersion(version2, "id", "2", "val2_l_dvo", map("inc", 1));
      version3 = addAndAssertVersion(version3, "id", "3", "val2_l_dvo", map("set", i));
    }
    assertU(commit("softCommit", "true"));

    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*,[docid]"),
        "//*[@numFound='3']",
        "//result/doc[1]/int[@name='val1_i_dvo'][.='"+(numPartialUpdates-1)+"']",
        "//result/doc[2]/int[@name='val1_i_dvo'][.='"+(numPartialUpdates+2)+"']",
        "//result/doc[3]/int[@name='val1_i_dvo'][.='"+(numPartialUpdates-1)+"']",
        "//result/doc[1]/long[@name='val2_l_dvo'][.='"+(numPartialUpdates-1)+"']",
        "//result/doc[2]/long[@name='val2_l_dvo'][.='"+(numPartialUpdates+2)+"']",
        "//result/doc[3]/long[@name='val2_l_dvo'][.='"+(numPartialUpdates-1)+"']",
        "//result/doc[1]/int[@name='[docid]'][.='"+docid1+"']",
        "//result/doc[2]/int[@name='[docid]'][.='"+docid2+"']",
        "//result/doc[3]/int[@name='[docid]'][.='"+docid3+"']",
        "//result/doc[1]/long[@name='_version_'][.='" + version1 + "']",
        "//result/doc[2]/long[@name='_version_'][.='" + version2 + "']",
        "//result/doc[3]/long[@name='_version_'][.='" + version3 + "']"
        );
  }

  /**
   * Useful to store the state of an expected document into an in-memory model
   * representing the index.
   */
  private static class DocInfo {
    public final long version;
    public final Long value;

    public DocInfo(long version, Long val) {
      this.version = version;
      this.value = val;
    }

    @Override
    public String toString() {
      return "["+version+", "+value+"]";
    }
  }

  /** @see #checkReplay */
  @Test
  public void testReplay_AfterInitialAddMixOfIncAndSet() throws Exception {
    checkReplay("val2_l_dvo",
        //
        sdoc("id", "0", "val2_l_dvo", 3000000000L),
        sdoc("id", "0", "val2_l_dvo", map("inc", 3)),
        HARDCOMMIT,
        sdoc("id", "0", "val2_l_dvo", map("inc", 5)),
        sdoc("id", "1", "val2_l_dvo", 2000000000L),
        sdoc("id", "1", "val2_l_dvo", map("set", 2000000002L)),
        sdoc("id", "1", "val2_l_dvo", map("set", 3000000000L)),
        sdoc("id", "0", "val2_l_dvo", map("inc", 7)),
        sdoc("id", "1", "val2_l_dvo", map("set", 7000000000L)),
        sdoc("id", "0", "val2_l_dvo", map("inc", 11)),
        sdoc("id", "2", "val2_l_dvo", 2000000000L),
        HARDCOMMIT,
        sdoc("id", "2", "val2_l_dvo", map("set", 3000000000L)),
        HARDCOMMIT);
  }

  /** @see #checkReplay */
  @Test
  public void testReplay_AfterInitialAddMixOfIncAndSetAndFullUpdates() throws Exception {
    checkReplay("val2_l_dvo",
        //
        sdoc("id", "0", "val2_l_dvo", 3000000000L),
        sdoc("id", "0", "val2_l_dvo", map("set", 3000000003L)),
        HARDCOMMIT,
        sdoc("id", "0", "val2_l_dvo", map("set", 3000000008L)),
        sdoc("id", "1", "val2_l_dvo", 2000000000L),
        sdoc("id", "1", "val2_l_dvo", map("inc", 2)),
        sdoc("id", "1", "val2_l_dvo", 3000000000L),
        sdoc("id", "0", "val2_l_dvo", map("set", 3000000015L)),
        sdoc("id", "1", "val2_l_dvo", 7000000000L),
        sdoc("id", "0", "val2_l_dvo", map("set", 3000000026L)),
        sdoc("id", "2", "val2_l_dvo", 2000000000L),
        HARDCOMMIT,
        sdoc("id", "2", "val2_l_dvo", 3000000000L),
        HARDCOMMIT);
  }

  /** @see #checkReplay */
  @Test
  public void testReplay_AllUpdatesAfterInitialAddAreInc() throws Exception {
    checkReplay("val2_l_dvo",
        //
        sdoc("id", "0", "val2_l_dvo", 3000000000L),
        sdoc("id", "0", "val2_l_dvo", map("inc", 3)),
        HARDCOMMIT,
        sdoc("id", "0", "val2_l_dvo", map("inc", 5)),
        sdoc("id", "1", "val2_l_dvo", 2000000000L),
        sdoc("id", "1", "val2_l_dvo", map("inc", 2)),
        sdoc("id", "1", "val2_l_dvo", 3000000000L),
        sdoc("id", "0", "val2_l_dvo", map("inc", 7)),
        sdoc("id", "1", "val2_l_dvo", 7000000000L),
        sdoc("id", "0", "val2_l_dvo", map("inc", 11)),
        sdoc("id", "2", "val2_l_dvo", 2000000000L),
        HARDCOMMIT,
        sdoc("id", "2", "val2_l_dvo", 3000000000L),
        HARDCOMMIT);
  }

  /** @see #checkReplay */
  @Test
  public void testReplay_AllUpdatesAfterInitialAddAreSets() throws Exception {
    checkReplay("val2_l_dvo",
        //
        sdoc("id", "0", "val2_l_dvo", 3000000000L),
        sdoc("id", "0", "val2_l_dvo", map("set", 3000000003L)),
        HARDCOMMIT,
        sdoc("id", "0", "val2_l_dvo", map("set", 3000000008L)),
        sdoc("id", "1", "val2_l_dvo", 2000000000L),
        sdoc("id", "1", "val2_l_dvo", map("set", 2000000002L)),
        sdoc("id", "1", "val2_l_dvo", map("set", 3000000000L)),
        sdoc("id", "0", "val2_l_dvo", map("set", 3000000015L)),
        sdoc("id", "1", "val2_l_dvo", map("set", 7000000000L)),
        sdoc("id", "0", "val2_l_dvo", map("set", 3000000026L)),
        sdoc("id", "2", "val2_l_dvo", 2000000000L),
        HARDCOMMIT,
        sdoc("id", "2", "val2_l_dvo", map("set", 3000000000L)),
        HARDCOMMIT
        );
  }
  
  /** @see #checkReplay */
  @Test
  public void testReplay_MixOfInplaceAndNonInPlaceAtomicUpdates() throws Exception {
    checkReplay("inplace_l_dvo",
                //
                sdoc("id", "3", "inplace_l_dvo", map("inc", -13)),
                sdoc("id", "3", "inplace_l_dvo", map("inc", 19),    "regular_l", map("inc", -17)),
                sdoc("id", "1",                                     "regular_l", map("inc", -19)),
                sdoc("id", "3", "inplace_l_dvo", map("inc", -11)),
                sdoc("id", "2", "inplace_l_dvo", map("set", 28)),
                HARDCOMMIT,
                sdoc("id", "2", "inplace_l_dvo", map("inc", 45)),
                sdoc("id", "3", "inplace_l_dvo", map("set", 72)),
                sdoc("id", "2",                                     "regular_l", map("inc", -55)),
                sdoc("id", "2", "inplace_l_dvo", -48,               "regular_l", 159),
                sdoc("id", "3", "inplace_l_dvo", 52,                "regular_l", 895),
                sdoc("id", "2", "inplace_l_dvo", map("inc", 19)),
                sdoc("id", "3", "inplace_l_dvo", map("inc", -264),  "regular_l", map("inc", -207)),
                sdoc("id", "3", "inplace_l_dvo", -762,              "regular_l", 272),
                SOFTCOMMIT);
  }
  
  @Test
  public void testReplay_SetOverriddenWithNoValueThenInc() throws Exception {
    final String inplaceField = "inplace_l_dvo"; 
    checkReplay(inplaceField,
                //
                sdoc("id", "1", inplaceField, map("set", 555L)),
                SOFTCOMMIT,
                sdoc("id", "1", "regular_l", 666L), // NOTE: no inplaceField, regular add w/overwrite 
                sdoc("id", "1", inplaceField, map("inc", -77)),
                HARDCOMMIT);
  }

  /** 
   * Simple enum for randomizing a type of update.
   * Each enum value has an associated probability, and the class has built in sanity checks 
   * that the total is 100%
   * 
   * @see RandomUpdate#pick
   * @see #checkRandomReplay
   */
  private static enum RandomUpdate {
    HARD_COMMIT(5), 
    SOFT_COMMIT(5),

    /** doc w/o the inplaceField, atomic update on some other (non-inplace) field */
    ATOMIC_NOT_INPLACE(5),
    
    /** atomic update of a doc w/ inc on both inplaceField *AND* non-inplace field */
    ATOMIC_INPLACE_AND_NOT_INPLACE(10), 

    
    /** atomic update of a doc w/ set inplaceField */
    ATOMIC_INPLACE_SET(25),
    /** atomic update of a doc w/ inc inplaceField */
    ATOMIC_INPLACE_INC(25), 
    
    /** doc w/o the inplaceField, normal add */
    ADD_NO_INPLACE_VALUE(5),
    /** a non atomic update of a doc w/ new inplaceField value */
    ADD_INPLACE_VALUE(20); 
    
    private RandomUpdate(int odds) {
      this.odds = odds;
    }
    public final int odds;

    static { // sanity check odds add up to 100%
      int total = 0;
      for (RandomUpdate candidate : RandomUpdate.values()) {
        total += candidate.odds;
      }
      assertEquals("total odds doesn't equal 100", 100, total);
    }

    /** pick a random type of RandomUpdate */
    public static final RandomUpdate pick(Random r) {
      final int target = TestUtil.nextInt(r, 1, 100);
      int cumulative_odds = 0;
      for (RandomUpdate candidate : RandomUpdate.values()) {
        cumulative_odds += candidate.odds;
        if (target <= cumulative_odds) {
          return candidate;
        }
      }
      fail("how did we not find a candidate? target=" + target + ", cumulative_odds=" + cumulative_odds);
      return null; // compiler mandated return
    }
  }
  
  /** @see #checkRandomReplay */
  @Test
  public void testReplay_Random_ManyDocsManyUpdates() throws Exception {
    
    // build up a random list of updates
    final int maxDocId = atLeast(50);
    final int numUpdates = maxDocId * 3;
    checkRandomReplay(maxDocId, numUpdates);
  }
  
  /** @see #checkRandomReplay */
  @Test
  public void testReplay_Random_FewDocsManyUpdates() throws Exception {
    
    // build up a random list of updates
    final int maxDocId = atLeast(3);
    final int numUpdates = maxDocId * 50;
    checkRandomReplay(maxDocId, numUpdates);
  }
  
  /** @see #checkRandomReplay */
  @Test
  public void testReplay_Random_FewDocsManyShortSequences() throws Exception {
    
    // build up a random list of updates
    final int numIters = atLeast(50);
    
    for (int i = 0; i < numIters; i++) {
      final int maxDocId = atLeast(3);
      final int numUpdates = maxDocId * 5;
      checkRandomReplay(maxDocId, numUpdates);
      deleteAllAndCommit();
    }
  }


  /** 
   * @see #checkReplay 
   * @see RandomUpdate
   */
  public void checkRandomReplay(final int maxDocId, final int numCmds) throws Exception {
    
    final String not_inplaceField = "regular_l";
    final String inplaceField = "inplace_l_dvo"; 

    final Object[] cmds = new Object[numCmds];
    for (int iter = 0; iter < numCmds; iter++) {
      final int id = TestUtil.nextInt(random(), 1, maxDocId);
      final RandomUpdate update = RandomUpdate.pick(random());

      switch (update) {
        
      case HARD_COMMIT:
        cmds[iter] = HARDCOMMIT;
        break;
        
      case SOFT_COMMIT:
        cmds[iter] = SOFTCOMMIT;
        break;

      case ATOMIC_NOT_INPLACE:
        // atomic update on non_inplaceField, w/o any value specified for inplaceField
        cmds[iter] = sdoc("id", id,
                          not_inplaceField, map("inc", random().nextInt()));
        break;
        
      case ATOMIC_INPLACE_AND_NOT_INPLACE:
        // atomic update of a doc w/ inc on both inplaceField and not_inplaceField
        cmds[iter] = sdoc("id", id,
                          inplaceField, map("inc", random().nextInt()),
                          not_inplaceField, map("inc", random().nextInt()));
        break;

      case ATOMIC_INPLACE_SET:
        // atomic update of a doc w/ set inplaceField
        cmds[iter] = sdoc("id", id,
                          inplaceField, map("set", random().nextLong()));
        break;

      case ATOMIC_INPLACE_INC:
        // atomic update of a doc w/ inc inplaceField
        cmds[iter] = sdoc("id", id,
                          inplaceField, map("inc", random().nextInt()));
        break;

      case ADD_NO_INPLACE_VALUE:
        // regular add of doc w/o the inplaceField, but does include non_inplaceField
        cmds[iter] = sdoc("id", id,
                          not_inplaceField, random().nextLong());
        break;

      case ADD_INPLACE_VALUE:
        // a non atomic update of a doc w/ new inplaceField value
        cmds[iter] = sdoc("id", id,
                          inplaceField, random().nextLong(),
                          not_inplaceField, random().nextLong());
        break;
        
      default:
        fail("WTF is this? ... " + update);
      }
      
      assertNotNull(cmds[iter]); // sanity check switch
    }

    checkReplay(inplaceField, cmds);
  }
  
  /** sentinal object for {@link #checkReplay} */
  public Object SOFTCOMMIT = new Object() { public String toString() { return "SOFTCOMMIT"; } };
  /** sentinal object for {@link #checkReplay} */
  public Object HARDCOMMIT = new Object() { public String toString() { return "HARDCOMMIT"; } };

  /**
   * Executes a sequence of commands against Solr, while tracking the expected value of a specified 
   * <code>valField</code> Long field (presumably that only uses docvalues) against an in memory model 
   * maintained in parallel (for the purpose of testing the correctness of in-place updates..
   *
   * <p>
   * A few restrictions are placed on the {@link SolrInputDocument}s that can be included when using 
   * this method, in order to keep the in-memory model management simple:
   * </p>
   * <ul>
   *  <li><code>id</code> must be uniqueKey field</li>
   *  <li><code>id</code> may have any FieldType, but all values must be parsable as Integers</li>
   *  <li><code>valField</code> must be a single valued field</li>
   *  <li>All values in the <code>valField</code> must either be {@link Number}s, or Maps containing 
   *      atomic updates ("inc" or "set") where the atomic value is a {@link Number}</li>
   * </ul>
   * 
   * @param valField the field to model
   * @param commands A sequence of Commands which can either be SolrInputDocuments 
   *                 (regular or containing atomic update Maps)
   *                 or one of the {@link TestInPlaceUpdatesStandalone#HARDCOMMIT} or {@link TestInPlaceUpdatesStandalone#SOFTCOMMIT} sentinal objects.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void checkReplay(final String valField, Object... commands) throws Exception {
    
    HashMap<Integer, DocInfo> model = new LinkedHashMap<>();
    HashMap<Integer, DocInfo> committedModel = new LinkedHashMap<>();

    // by default, we only check the committed model after a commit
    // of if the number of total commands is relatively small.
    //
    // (in theory, there's no reason to check the committed model unless we know there's been a commit
    // but for smaller tests the overhead of doing so is tiny, so we might as well)
    //
    // if some test seed fails, and you want to force the committed model to be checked
    // after every command, just temporaribly force this variable to true...
    boolean checkCommittedModel = (commands.length < 50);
    
    for (Object cmd : commands) {
      if (cmd == SOFTCOMMIT) {
        assertU(commit("softCommit", "true"));
        committedModel = new LinkedHashMap(model);
        checkCommittedModel = true;
      } else if (cmd == HARDCOMMIT) {
        assertU(commit("softCommit", "false"));
        committedModel = new LinkedHashMap(model);
        checkCommittedModel = true;
      } else {
        assertNotNull("null command in checkReplay", cmd);
        assertTrue("cmd is neither sentinal (HARD|SOFT)COMMIT object, nor Solr doc: " + cmd.getClass(),
                   cmd instanceof SolrInputDocument);
        
        final SolrInputDocument sdoc = (SolrInputDocument) cmd;
        final int id = Integer.parseInt(sdoc.getFieldValue("id").toString());
        
        final DocInfo previousInfo = model.get(id);
        final Long previousValue = (null == previousInfo) ? null : previousInfo.value;
        
        final long version = addAndGetVersion(sdoc, null);
        
        final Object val = sdoc.getFieldValue(valField);
        if (val instanceof Map) {
          // atomic update of the field we're modeling
          
          Map<String,?> atomicUpdate = (Map) val;
          assertEquals(sdoc.toString(), 1, atomicUpdate.size());
          if (atomicUpdate.containsKey("inc")) {
            // Solr treats inc on a non-existing doc (or doc w/o existing value) as if existing value is 0
            final long base = (null == previousValue) ? 0L : previousValue;
            model.put(id, new DocInfo(version,
                                      base + ((Number)atomicUpdate.get("inc")).longValue()));
          } else if (atomicUpdate.containsKey("set")) {
            model.put(id, new DocInfo(version, ((Number)atomicUpdate.get("set")).longValue()));
          } else {
            fail("wtf update is this? ... " + sdoc);
          }
        } else if (null == val) {
          // the field we are modeling is not mentioned in this update, It's either...
          //
          // a) a regular update of some other fields (our model should have a null value)
          // b) an atomic update of some other field (keep existing value in model)
          //
          // for now, assume it's atomic and we're going to keep our existing value...
          Long newValue = (null == previousInfo) ? null : previousInfo.value;
          for (SolrInputField field : sdoc) {
            if (! ( "id".equals(field.getName()) || (field.getValue() instanceof Map)) ) {
              // not an atomic update, newValue in model should be null
              newValue = null;
              break;
            }
          }
          model.put(id, new DocInfo(version, newValue));
          
        } else {
          // regular replacement of the value in the field we're modeling
          
          assertTrue("Model field value is not a Number: " + val.getClass(), val instanceof Number);
          model.put(id, new DocInfo(version, ((Number)val).longValue()));
        }
      }

      // after every op, check the model(s)
      
      // RTG to check the values for every id against the model
      for (Map.Entry<Integer, DocInfo> entry : model.entrySet()) {
        final Long expected = entry.getValue().value;
        assertEquals(expected, client.getById(String.valueOf(entry.getKey())).getFirstValue(valField));
      }

      // search to check the values for every id in the committed model
      if (checkCommittedModel) {
        final int numCommitedDocs = committedModel.size();
        String[] xpaths = new String[1 + numCommitedDocs];
        int i = 0;
        for (Map.Entry<Integer, DocInfo> entry : committedModel.entrySet()) {
          Integer id = entry.getKey();
          Long expected = entry.getValue().value;
          if (null != expected) {
            xpaths[i] = "//result/doc[./str='"+id+"'][./long='"+expected+"']";
          } else {
            xpaths[i] = "//result/doc[./str='"+id+"'][not(./long)]";
          }           
          i++;
        }
        xpaths[i] = "//*[@numFound='"+numCommitedDocs+"']";
        assertQ(req("q", "*:*",
                    "fl", "id," + valField,
                    "rows", ""+numCommitedDocs),
                xpaths);
      }
    }
  }

  @Test
  public void testMixedInPlaceAndNonInPlaceAtomicUpdates() throws Exception {
    SolrDocument rtgDoc = null;
    long version1 = addAndGetVersion(sdoc("id", "1", "inplace_updatable_float", "100", "stored_i", "100"), params());

    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", "1"), "stored_i", map("inc", "1"));
    rtgDoc = client.getById("1");
    assertEquals(101, rtgDoc.getFieldValue("stored_i"));
    assertEquals(101.0f, rtgDoc.getFieldValue("inplace_updatable_float"));
    
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("inc", "1"));
    rtgDoc = client.getById("1");
    assertEquals(101, rtgDoc.getFieldValue("stored_i"));
    assertEquals(102.0f, rtgDoc.getFieldValue("inplace_updatable_float"));

    version1 = addAndAssertVersion(version1, "id", "1", "stored_i", map("inc", "1"));
    rtgDoc = client.getById("1");
    assertEquals(102, rtgDoc.getFieldValue("stored_i"));
    assertEquals(102.0f, rtgDoc.getFieldValue("inplace_updatable_float"));

    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*", "sort", "id asc", "fl", "*"),
            "//*[@numFound='1']",
            "//result/doc[1]/float[@name='inplace_updatable_float'][.='102.0']",
            "//result/doc[1]/int[@name='stored_i'][.='102']",
            "//result/doc[1]/long[@name='_version_'][.='" + version1 + "']"
            );

    // recheck RTG after commit
    rtgDoc = client.getById("1");
    assertEquals(102, rtgDoc.getFieldValue("stored_i"));
    assertEquals(102.0f, rtgDoc.getFieldValue("inplace_updatable_float"));
  }

  /** 
   * @see #callComputeInPlaceUpdatableFields
   * @see AtomicUpdateDocumentMerger#computeInPlaceUpdatableFields 
   */
  @Test
  public void testComputeInPlaceUpdatableFields() throws Exception {
    Set<String> inPlaceUpdatedFields = new HashSet<String>();

    // these asserts should hold true regardless of type, or wether the field has a default
    List<String> fieldsToCheck = Arrays.asList("inplace_updatable_float",
                                               "inplace_updatable_int",
                                               "inplace_updatable_float_with_default",
                                               "inplace_updatable_int_with_default");
    Collections.shuffle(fieldsToCheck, random()); // ... and regardless of order checked
    for (String field : fieldsToCheck) {
      // In-place updatable field updated before it exists SHOULD NOW BE in-place updated (since LUCENE-8316):
      inPlaceUpdatedFields = callComputeInPlaceUpdatableFields(sdoc("id", "1", "_version_", 42L,
                                                                    field, map("set", 10)));
      assertTrue(field, inPlaceUpdatedFields.contains(field));
      
      // In-place updatable field updated after it exists SHOULD BE in-place updated:
      addAndGetVersion(sdoc("id", "1", field, "0"), params()); // setting up the dv
      inPlaceUpdatedFields = callComputeInPlaceUpdatableFields(sdoc("id", "1", "_version_", 42L,
                                                                    field, map("set", 10)));
      assertTrue(field, inPlaceUpdatedFields.contains(field));

      inPlaceUpdatedFields = callComputeInPlaceUpdatableFields(sdoc("id", "1", "_version_", 42L,
                                                                    field, map("inc", 10)));
      assertTrue(field, inPlaceUpdatedFields.contains(field));

      final String altFieldWithDefault = field.contains("float") ?
        "inplace_updatable_int_with_default" : "inplace_updatable_int_with_default";
      
      // Updating an in-place updatable field (with a default) for the first time.
      // DV for it should have been already created when first document was indexed (above),
      // since it has a default value
      inPlaceUpdatedFields = callComputeInPlaceUpdatableFields(sdoc("id", "1", "_version_", 42L,
                                                                    altFieldWithDefault, map("set", 10)));
      assertTrue(field + " -> " + altFieldWithDefault, inPlaceUpdatedFields.contains(altFieldWithDefault));
      
      deleteAllAndCommit();
    }
  
    // Non in-place updates
    addAndGetVersion(sdoc("id", "1", "stored_i", "0"), params()); // setting up the dv
    assertTrue("stored field updated",
               callComputeInPlaceUpdatableFields(sdoc("id", "1", "_version_", 42L,
                                                      "stored_i", map("inc", 1))).isEmpty());
    
    assertTrue("full document update",
               callComputeInPlaceUpdatableFields(sdoc("id", "1", "_version_", 42L,
                                                      "inplace_updatable_int_with_default", "100")).isEmpty());
  
    assertFalse("non existent dynamic dv field updated first time",
               callComputeInPlaceUpdatableFields(sdoc("id", "1", "_version_", 42L,
                                                      "new_updatable_int_i_dvo", map("set", 10))).isEmpty());
    
    // After adding a full document with the dynamic dv field, in-place update should work
    addAndGetVersion(sdoc("id", "2", "new_updatable_int_i_dvo", "0"), params()); // setting up the dv
    if (random().nextBoolean()) {
      assertU(commit("softCommit", "false"));
    }
    inPlaceUpdatedFields = callComputeInPlaceUpdatableFields(sdoc("id", "2", "_version_", 42L,
                                                                  "new_updatable_int_i_dvo", map("set", 10)));
    assertTrue(inPlaceUpdatedFields.contains("new_updatable_int_i_dvo"));

    // for copy fields, regardless of whether the source & target support inplace updates,
    // it won't be inplace if the DVs don't exist yet...
    assertTrue("inplace fields should be empty when doc has no copyfield src values yet",
               callComputeInPlaceUpdatableFields(sdoc("id", "1", "_version_", 42L,
                                                      "copyfield1_src__both_updatable", map("set", 1),
                                                      "copyfield2_src__only_src_updatable", map("set", 2))).isEmpty());

    // now add a doc that *does* have the src field for each copyfield...
    addAndGetVersion(sdoc("id", "3",
                          "copyfield1_src__both_updatable", -13,
                          "copyfield2_src__only_src_updatable", -15), params()); 
    if (random().nextBoolean()) {
      assertU(commit("softCommit", "false"));
    }
    
    // If a supported dv field has a copyField target which is supported, it should be an in-place update
    inPlaceUpdatedFields = callComputeInPlaceUpdatableFields(sdoc("id", "3", "_version_", 42L,
                                                                  "copyfield1_src__both_updatable", map("set", 10)));
    assertTrue(inPlaceUpdatedFields.contains("copyfield1_src__both_updatable"));

    // If a supported dv field has a copyField target which is not supported, it should not be an in-place update
    inPlaceUpdatedFields = callComputeInPlaceUpdatableFields(sdoc("id", "3", "_version_", 42L,
                                                                  "copyfield2_src__only_src_updatable", map("set", 10)));
    assertTrue(inPlaceUpdatedFields.isEmpty());
  }

  @Test
  /**
   *  Test the @see {@link AtomicUpdateDocumentMerger#doInPlaceUpdateMerge(AddUpdateCommand,Set<String>)} 
   *  method is working fine
   */
  public void testDoInPlaceUpdateMerge() throws Exception {
    long version1 = addAndGetVersion(sdoc("id", "1", "title_s", "first"), null);
    long version2 = addAndGetVersion(sdoc("id", "2", "title_s", "second"), null);
    long version3 = addAndGetVersion(sdoc("id", "3", "title_s", "third"), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q", "*:*"), "//*[@numFound='3']");

    // Adding a few in-place updates
    version1 = addAndAssertVersion(version1, "id", "1", "inplace_updatable_float", map("set", 200));

    // Test the AUDM.doInPlaceUpdateMerge() method is working fine
    try (SolrQueryRequest req = req()) {
      AddUpdateCommand cmd = buildAddUpdateCommand(req, sdoc("id", "1", "_version_", 42L,
                                                             "inplace_updatable_float", map("inc", 10)));
      AtomicUpdateDocumentMerger docMerger = new AtomicUpdateDocumentMerger(req);
      assertTrue(docMerger.doInPlaceUpdateMerge(cmd, AtomicUpdateDocumentMerger.computeInPlaceUpdatableFields(cmd)));
      assertEquals(42L, cmd.getSolrInputDocument().getFieldValue("_version_"));
      assertEquals(42L, cmd.getSolrInputDocument().getFieldValue("_version_"));
      assertEquals(210f, cmd.getSolrInputDocument().getFieldValue("inplace_updatable_float"));
      // in-place merged doc shouldn't have non-inplace fields from the index/tlog
      assertFalse(cmd.getSolrInputDocument().containsKey("title_s"));
      assertEquals(version1, cmd.prevVersion);
    }
    
    // do a commit, and the same results should be repeated
    assertU(commit("softCommit", "false"));

    // Test the AUDM.doInPlaceUpdateMerge() method is working fine
    try (SolrQueryRequest req = req()) {
      AddUpdateCommand cmd = buildAddUpdateCommand(req, sdoc("id", "1", "_version_", 42L,
                                                             "inplace_updatable_float", map("inc", 10)));
      AtomicUpdateDocumentMerger docMerger = new AtomicUpdateDocumentMerger(req);
      assertTrue(docMerger.doInPlaceUpdateMerge(cmd, AtomicUpdateDocumentMerger.computeInPlaceUpdatableFields(cmd)));
      assertEquals(42L, cmd.getSolrInputDocument().getFieldValue("_version_"));
      assertEquals(42L, cmd.getSolrInputDocument().getFieldValue("_version_"));
      assertEquals(210f, cmd.getSolrInputDocument().getFieldValue("inplace_updatable_float"));
      // in-place merged doc shouldn't have non-inplace fields from the index/tlog
      assertFalse(cmd.getSolrInputDocument().containsKey("title_s")); 
      assertEquals(version1, cmd.prevVersion);
    }
  }

  public void testFailOnVersionConflicts() throws Exception {

    assertU(add(doc("id", "1", "title_s", "first")));
    assertU(commit());
    assertQ(req("q", "title_s:first"), "//*[@numFound='1']");
    assertU(add(doc("id", "1", "title_s", "first1")));
    assertU(commit());
    assertQ(req("q", "title_s:first1"), "//*[@numFound='1']");
    assertFailedU(add(doc("id", "1", "title_s", "first2", "_version_", "-1")));
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("_version_", "-1");
    SolrInputDocument doc = new SolrInputDocument("id", "1", "title_s", "first2");
    SolrInputDocument doc2 = new SolrInputDocument("id", "2", "title_s", "second");
    SolrException ex = expectThrows(SolrException.class, "This should have failed", () -> updateJ(jsonAdd(doc, doc2), params));

    assertTrue(ex.getMessage().contains("version conflict for"));
    params.add(CommonParams.FAIL_ON_VERSION_CONFLICTS, "false");
    updateJ(jsonAdd(doc, doc2), params);//this should not throw any error
    assertU(commit());
    assertQ(req("q", "title_s:second"), "//*[@numFound='1']");
    assertQ(req("q", "title_s:first1"), "//*[@numFound='1']");// but the old value exists
    assertQ(req("q", "title_s:first2"), "//*[@numFound='0']");// and the new value does not reflect

    // tests inplace updates when doc does not exist
    assertFailedU(add(doc("id", "3", "title_s", "third", "_version_", "1")));
    params.set(CommonParams.FAIL_ON_VERSION_CONFLICTS, "true");
    params.set("_version_", "1");
    SolrInputDocument doc1_v3 = sdoc("id", "1", "title_s", map("set", "first3"));
    SolrInputDocument doc3 = sdoc("id", "3", "title_s", map("set", "third"));
    ex =
        expectThrows(
            SolrException.class,
            "This should have failed",
            () -> updateJ(jsonAdd(doc1_v3, doc3), params));
    assertTrue(ex.getMessage().contains("Document not found for update"));

    params.set(CommonParams.FAIL_ON_VERSION_CONFLICTS, "false");
    SolrInputDocument doc1_v4 = sdoc("id", "1", "title_s", map("set", "first4"));
    updateJ(jsonAdd(doc1_v4, doc3), params); // this should not throw any error

    assertU(commit());
    assertQ(req("q", "title_s:first4"), "//*[@numFound='1']"); // the new value does reflect
    assertQ(req("q", "title_s:first1"), "//*[@numFound='0']"); // but the old value does not exist
    assertQ(req("q", "title_s:third"), "//*[@numFound='0']"); // doc3 does not exist
  }
  /** 
   * Helper method that sets up a req/cmd to run {@link AtomicUpdateDocumentMerger#computeInPlaceUpdatableFields} 
   * on the specified solr input document.
   */
  private static Set<String> callComputeInPlaceUpdatableFields(final SolrInputDocument sdoc) throws Exception {
    try (SolrQueryRequest req = req()) {
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.solrDoc = sdoc;
      assertTrue(cmd.solrDoc.containsKey(CommonParams.VERSION_FIELD));
      cmd.setVersion(Long.parseLong(cmd.solrDoc.getFieldValue(CommonParams.VERSION_FIELD).toString()));
      return AtomicUpdateDocumentMerger.computeInPlaceUpdatableFields(cmd);
    }
  }
}
