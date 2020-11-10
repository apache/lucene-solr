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

import java.util.List;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;
import static org.hamcrest.core.StringContains.containsString;

public class UpdateLogTest extends SolrTestCaseJ4 {

  /** BytesRef that can be re-used to lookup doc with id "1" */
  private static final BytesRef DOC_1_INDEXED_ID = new BytesRef("1");


  static UpdateLog ulog = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml", "schema-inplace-updates.xml");

    try (SolrQueryRequest req = req()) {
      UpdateHandler uhandler = req.getCore().getUpdateHandler();
      ((DirectUpdateHandler2) uhandler).getCommitTracker().setTimeUpperBound(100);
      ((DirectUpdateHandler2) uhandler).getCommitTracker().setOpenSearcher(false);
      ulog = uhandler.getUpdateLog();
    }
  }

  @AfterClass
  public static void afterClass() {
    ulog = null;
  }

  @Test
  /**
   * @see org.apache.solr.update.UpdateLog#applyPartialUpdates(BytesRef,long,long,SolrDocumentBase)
   */
  public void testApplyPartialUpdatesOnMultipleInPlaceUpdatesInSequence() {    
    // Add a full update, two in-place updates and verify applying partial updates is working
    ulogAdd(ulog, null, sdoc("id", "1", "title_s", "title1", "val1_i_dvo", "1", "_version_", "100"));
    ulogAdd(ulog, 100L, sdoc("id", "1", "price", "1000", "val1_i_dvo", "2", "_version_", "101"));
    ulogAdd(ulog, 101L, sdoc("id", "1", "val1_i_dvo", "3", "_version_", "102"));

    Object partialUpdate = ulog.lookup(DOC_1_INDEXED_ID);
    SolrDocument partialDoc = RealTimeGetComponent.toSolrDoc((SolrInputDocument)((List)partialUpdate).get(4), 
        h.getCore().getLatestSchema());
    long prevVersion = (Long)((List)partialUpdate).get(3);
    long prevPointer = (Long)((List)partialUpdate).get(2);

    assertEquals(3L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertFalse(partialDoc.containsKey("title_s"));

    long returnVal = ulog.applyPartialUpdates(DOC_1_INDEXED_ID, prevPointer, prevVersion, null, partialDoc);

    assertEquals(0, returnVal);
    assertEquals(1000, Integer.parseInt(partialDoc.getFieldValue("price").toString()));
    assertEquals(3L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertEquals("title1", partialDoc.getFieldValue("title_s"));

    // Add a full update, commit, then two in-place updates, and verify that applying partial updates is working (since
    // the prevTlog and prevTlog2 are retained after a commit
    ulogCommit(ulog);
    if (random().nextBoolean()) { // sometimes also try a second commit
      ulogCommit(ulog);
    }
    ulogAdd(ulog, 102L, sdoc("id", "1", "price", "2000", "val1_i_dvo", "4", "_version_", "200"));
    ulogAdd(ulog, 200L, sdoc("id", "1", "val1_i_dvo", "5", "_version_", "201"));

    partialUpdate = ulog.lookup(DOC_1_INDEXED_ID);
    partialDoc = RealTimeGetComponent.toSolrDoc((SolrInputDocument)((List)partialUpdate).get(4), h.getCore().getLatestSchema());
    prevVersion = (Long)((List)partialUpdate).get(3);
    prevPointer = (Long)((List)partialUpdate).get(2);

    assertEquals(5L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertFalse(partialDoc.containsKey("title_s"));

    returnVal = ulog.applyPartialUpdates(DOC_1_INDEXED_ID, prevPointer, prevVersion, null, partialDoc);

    assertEquals(0, returnVal);
    assertEquals(2000, Integer.parseInt(partialDoc.getFieldValue("price").toString()));
    assertEquals(5L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertEquals("title1", partialDoc.getFieldValue("title_s"));
  }
  
  @Test
  public void testApplyPartialUpdatesAfterMultipleCommits() {    
    ulogAdd(ulog, null, sdoc("id", "1", "title_s", "title1", "val1_i_dvo", "1", "_version_", "100"));
    ulogAdd(ulog, 100L, sdoc("id", "1", "price", "1000", "val1_i_dvo", "2", "_version_", "101"));
    ulogAdd(ulog, 101L, sdoc("id", "1", "val1_i_dvo", "3", "_version_", "102"));

    // Do 3 commits, then in-place update, and verify that applying partial updates can't find full doc
    for (int i=0; i<3; i++)
      ulogCommit(ulog);
    ulogAdd(ulog, 101L, sdoc("id", "1", "val1_i_dvo", "6", "_version_", "300"));

    Object partialUpdate = ulog.lookup(DOC_1_INDEXED_ID);
    SolrDocument partialDoc = RealTimeGetComponent.toSolrDoc((SolrInputDocument)((List)partialUpdate).get(4), h.getCore().getLatestSchema());
    long prevVersion = (Long)((List)partialUpdate).get(3);
    long prevPointer = (Long)((List)partialUpdate).get(2);

    assertEquals(6L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertFalse(partialDoc.containsKey("title_s"));

    long returnVal = ulog.applyPartialUpdates(DOC_1_INDEXED_ID, prevPointer, prevVersion, null, partialDoc);

    assertEquals(-1, returnVal);
  }

  @Test
  public void testApplyPartialUpdatesDependingOnNonAddShouldThrowException() {
    ulogAdd(ulog, null, sdoc("id", "1", "title_s", "title1", "val1_i_dvo", "1", "_version_", "100"));
    ulogDelete(ulog, "1", 500L, false); // dbi
    ulogAdd(ulog, 500L, sdoc("id", "1", "val1_i_dvo", "2", "_version_", "501"));
    ulogAdd(ulog, 501L, sdoc("id", "1", "val1_i_dvo", "3", "_version_", "502"));

    Object partialUpdate = ulog.lookup(DOC_1_INDEXED_ID);
    SolrDocument partialDoc = RealTimeGetComponent.toSolrDoc((SolrInputDocument)((List)partialUpdate).get(4), h.getCore().getLatestSchema());
    long prevVersion = (Long)((List)partialUpdate).get(3);
    long prevPointer = (Long)((List)partialUpdate).get(2);

    assertEquals(3L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertEquals(502L, ((NumericDocValuesField)partialDoc.getFieldValue("_version_")).numericValue());
    assertFalse(partialDoc.containsKey("title_s"));

    // If an in-place update depends on a non-add (i.e. DBI), assert that an exception is thrown.
    SolrException ex = expectThrows(SolrException.class, () -> {
        long returnVal = ulog.applyPartialUpdates(DOC_1_INDEXED_ID, prevPointer, prevVersion, null, partialDoc);
        fail("502 depends on 501, 501 depends on 500, but 500 is a"
             + " DELETE. This should've generated an exception. returnVal is: "+returnVal);
      });
    assertEquals(ex.toString(), SolrException.ErrorCode.INVALID_STATE.code, ex.code());
    assertThat(ex.getMessage(), containsString("should've been either ADD or UPDATE_INPLACE"));
    assertThat(ex.getMessage(), containsString("looking for id=1"));
  }

  @Test
  public void testApplyPartialUpdatesWithDelete() throws Exception {
    ulogAdd(ulog, null, sdoc("id", "1", "title_s", "title1", "val1_i_dvo", "1", "_version_", "100"));
    ulogAdd(ulog, 100L, sdoc("id", "1", "val1_i_dvo", "2", "_version_", "101")); // in-place update
    ulogAdd(ulog, 101L, sdoc("id", "1", "val1_i_dvo", "3", "_version_", "102")); // in-place update
    
    // sanity check that the update log has one document, and RTG returns the document
    assertEquals(1, ulog.map.size());
    assertJQ(req("qt","/get", "id","1")
             , "=={'doc':{ 'id':'1', 'val1_i_dvo':3, '_version_':102, 'title_s':'title1', "
             // fields with default values
             + "'inplace_updatable_int_with_default':666, 'inplace_updatable_float_with_default':42.0}}");
    
    boolean dbq = random().nextBoolean();
    ulogDelete(ulog, "1", 200L, dbq); // delete id:1 document
    if (dbq) {
      assertNull(ulog.lookup(DOC_1_INDEXED_ID)); // any DBQ clears out the ulog, so this document shouldn't exist
      assertEquals(0, ulog.map.size());
      assertTrue(String.valueOf(ulog.prevMap), ulog.prevMap == null || ulog.prevMap.size() == 0);
      assertTrue(String.valueOf(ulog.prevMap2), ulog.prevMap2 == null || ulog.prevMap2.size() == 0);
      // verify that the document is deleted, by doing an RTG call
      assertJQ(req("qt","/get", "id","1"), "=={'doc':null}");
    } else { // dbi
      @SuppressWarnings({"rawtypes"})
      List entry = ((List)ulog.lookup(DOC_1_INDEXED_ID));
      assertEquals(UpdateLog.DELETE, (int)entry.get(UpdateLog.FLAGS_IDX) & UpdateLog.OPERATION_MASK);
    }
  }

  /**
   * Simulate a commit on a given updateLog
   */
  private static void ulogCommit(UpdateLog ulog) {
    try (SolrQueryRequest req = req()) {
      CommitUpdateCommand commitCmd = new CommitUpdateCommand(req, false);
      ulog.preCommit(commitCmd);
      ulog.postCommit(commitCmd);
    }
  }

  /**
   * Simulate a delete on a given updateLog
   *
   * @param ulog The UpdateLog to apply a delete against
   * @param id of document to be deleted
   * @param version Version to use on the DeleteUpdateCommand
   * @param dbq if true, an <code>id:$id</code> DBQ will used, instead of delete by id
   */
  private static void ulogDelete(UpdateLog ulog, String id, long version, boolean dbq) {
    try (SolrQueryRequest req = req()) {
      DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
      cmd.setVersion(version);
      if (dbq) {
        cmd.query = ("id:"+id);
        ulog.deleteByQuery(cmd);
      } else {
        cmd.id = id;
        ulog.delete(cmd);
      }
    }
  }

  /**
   * Simulate an add on a given updateLog.
   * <p>
   *   This method, when prevVersion is passed in (i.e. for in-place update), represents an 
   *   AddUpdateCommand that has undergone the merge process and inc/set operations have now been
   *   converted into actual values that just need to be written. 
   * </p>
   * <p>
   * NOTE: For test simplicity, the Solr input document must include the <code>_version_</code> field.
   * </p>
   *
   * @param ulog The UpdateLog to apply a delete against
   * @param prevVersion If non-null, then this AddUpdateCommand represents an in-place update.
   * @param sdoc The document to use for the add.
   * @see #buildAddUpdateCommand
   */
  private static void ulogAdd(UpdateLog ulog, Long prevVersion, SolrInputDocument sdoc) {
    try (SolrQueryRequest req = req()) {
      AddUpdateCommand cmd = buildAddUpdateCommand(req, sdoc);
      if (prevVersion != null) {
        cmd.prevVersion = prevVersion;
      }
      ulog.add(cmd);
    }
  }

  /**
   * Helper method to construct an <code>AddUpdateCommand</code> for a <code>SolrInputDocument</code> 
   * in the context of the specified <code>SolrQueryRequest</code>. 
   *
   * NOTE: For test simplicity, the Solr input document must include the <code>_version_</code> field.
   */ 
  public static AddUpdateCommand buildAddUpdateCommand(final SolrQueryRequest req, final SolrInputDocument sdoc) {
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.solrDoc = sdoc;
    assertTrue("", cmd.solrDoc.containsKey(VERSION_FIELD));
    cmd.setVersion(Long.parseLong(cmd.solrDoc.getFieldValue(VERSION_FIELD).toString()));
    return cmd;
  }
}
