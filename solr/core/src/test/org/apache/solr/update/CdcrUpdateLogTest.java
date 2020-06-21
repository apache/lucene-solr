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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.fromJSONString;

@Nightly
public class CdcrUpdateLogTest extends SolrTestCaseJ4 {

  private static int timeout = 60;  // acquire timeout in seconds.  change this to a huge number when debugging to prevent threads from advancing.

  // TODO: fix this test to not require FSDirectory
  static String savedFactory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    initCore("solrconfig-cdcrupdatelog.xml", "schema15.xml");
  }

  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  private void clearCore() throws IOException {
    clearIndex();
    assertU(commit());

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

    h.close();

    String[] files = ulog.getLogList(logDir);
    for (String file : files) {

      File toDelete = new File(logDir, file);
      Files.delete(toDelete.toPath()); // Should we really error out here?
    }

    assertEquals(0, ulog.getLogList(logDir).length);

    createCore();
  }

  private void deleteByQuery(String q) throws Exception {
    deleteByQueryAndGetVersion(q, null);
  }

  private void addDocs(int nDocs, int start, LinkedList<Long> versions) throws Exception {
    for (int i = 0; i < nDocs; i++) {
      versions.addFirst(addAndGetVersion(sdoc("id", Integer.toString(start + i)), null));
    }
  }

  private static Long getVer(SolrQueryRequest req) throws Exception {
    @SuppressWarnings({"rawtypes"})
    Map rsp = (Map) fromJSONString(JQ(req));
    @SuppressWarnings({"rawtypes"})
    Map doc = null;
    if (rsp.containsKey("doc")) {
      doc = (Map) rsp.get("doc");
    } else if (rsp.containsKey("docs")) {
      @SuppressWarnings({"rawtypes"})
      List lst = (List) rsp.get("docs");
      if (lst.size() > 0) {
        doc = (Map) lst.get(0);
      }
    } else if (rsp.containsKey("response")) {
      @SuppressWarnings({"rawtypes"})
      Map responseMap = (Map) rsp.get("response");
      @SuppressWarnings({"rawtypes"})
      List lst = (List) responseMap.get("docs");
      if (lst.size() > 0) {
        doc = (Map) lst.get(0);
      }
    }

    if (doc == null) return null;

    return (Long) doc.get("_version_");
  }

  @Test
  public void testLogReaderNext() throws Exception {
    this.clearCore();

    int start = 0;

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    CdcrUpdateLog.CdcrLogReader reader = ((CdcrUpdateLog) ulog).newLogReader(); // test reader on empty updates log

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(11, start, versions);
    start += 11;
    assertU(commit());

    for (int i = 0; i < 10; i++) { // 10 adds
      assertNotNull(reader.next());
    }
    Object o = reader.next();
    assertNotNull(o);

    @SuppressWarnings({"rawtypes"})
    List entry = (List) o;
    int opAndFlags = (Integer) entry.get(0);
    assertEquals(UpdateLog.COMMIT, opAndFlags & UpdateLog.OPERATION_MASK);

    for (int i = 0; i < 11; i++) { // 11 adds
      assertNotNull(reader.next());
    }
    o = reader.next();
    assertNotNull(o);

    entry = (List) o;
    opAndFlags = (Integer) entry.get(0);
    assertEquals(UpdateLog.COMMIT, opAndFlags & UpdateLog.OPERATION_MASK);

    assertNull(reader.next());

    // add a new tlog after having exhausted the reader

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    // the reader should pick up the new tlog

    for (int i = 0; i < 11; i++) { // 10 adds + 1 commit
      assertNotNull(reader.next());
    }
    assertNull(reader.next());
  }

  /**
   * Check the seek method of the log reader.
   */
  @Test
  public void testLogReaderSeek() throws Exception {
    this.clearCore();

    int start = 0;

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    CdcrUpdateLog.CdcrLogReader reader1 = ((CdcrUpdateLog) ulog).newLogReader();
    CdcrUpdateLog.CdcrLogReader reader2 = ((CdcrUpdateLog) ulog).newLogReader();
    CdcrUpdateLog.CdcrLogReader reader3 = ((CdcrUpdateLog) ulog).newLogReader();

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(11, start, versions);
    start += 11;
    assertU(commit());

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    // Test case where target version is equal to startVersion of tlog file
    long targetVersion = getVer(req("q", "id:10"));

    assertTrue(reader1.seek(targetVersion));
    Object o = reader1.next();
    assertNotNull(o);
    @SuppressWarnings({"rawtypes"})
    List entry = (List) o;
    long version = (Long) entry.get(1);

    assertEquals(targetVersion, version);

    assertNotNull(reader1.next());

    // test case where target version is superior to startVersion of tlog file
    targetVersion = getVer(req("q", "id:26"));

    assertTrue(reader2.seek(targetVersion));
    o = reader2.next();
    assertNotNull(o);
    entry = (List) o;
    version = (Long) entry.get(1);

    assertEquals(targetVersion, version);

    assertNotNull(reader2.next());

    // test case where target version is inferior to startVersion of oldest tlog file
    targetVersion = getVer(req("q", "id:0")) - 1;

    assertFalse(reader3.seek(targetVersion));
  }

  /**
   * Check that the log reader is able to read the new tlog
   * and pick up new entries as they appear.
   */
  @Test
  public void testLogReaderNextOnNewTLog() throws Exception {
    this.clearCore();

    int start = 0;

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    CdcrUpdateLog.CdcrLogReader reader = ((CdcrUpdateLog) ulog).newLogReader();

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(11, start, versions);
    start += 11;

    for (int i = 0; i < 22; i++) { // 21 adds + 1 commit
      assertNotNull(reader.next());
    }

    // we should have reach the end of the new tlog
    assertNull(reader.next());

    addDocs(5, start, versions);
    start += 5;

    // the reader should now pick up the new updates

    for (int i = 0; i < 5; i++) { // 5 adds
      assertNotNull(reader.next());
    }

    assertNull(reader.next());
  }

  @Test
  public void testRemoveOldLogs() throws Exception {
    this.clearCore();

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

    int start = 0;
    int maxReq = 50;

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));
    assertU(commit());
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));

    addDocs(10, start, versions);
    start += 10;
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));
    assertU(commit());
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));

    assertEquals(2, ulog.getLogList(logDir).length);

    // Get a cdcr log reader to initialise a log pointer
    CdcrUpdateLog.CdcrLogReader reader = ((CdcrUpdateLog) ulog).newLogReader();

    addDocs(105, start, versions);
    start += 105;
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));
    assertU(commit());
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));

    // the previous two tlogs should not be removed
    assertEquals(3, ulog.getLogList(logDir).length);

    // move the pointer past the first tlog
    for (int i = 0; i <= 11; i++) { // 10 adds + 1 commit
      assertNotNull(reader.next());
    }

    addDocs(10, start, versions);
    start += 10;
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));
    assertU(commit());
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));

    // the first tlog should be removed
    assertEquals(3, ulog.getLogList(logDir).length);

    h.close();
    createCore();

    ulog = h.getCore().getUpdateHandler().getUpdateLog();

    addDocs(105, start, versions);
    start += 105;
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));
    assertU(commit());
    assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));

    // previous tlogs should be gone now
    assertEquals(1, ulog.getLogList(logDir).length);
  }

  /**
   * Check that the removal of old logs is taking into consideration
   * multiple log pointers. Check also that the removal takes into consideration the
   * numRecordsToKeep limit, even if the log pointers are ahead.
   */
  @Test
  public void testRemoveOldLogsMultiplePointers() throws Exception {
    this.clearCore();

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());
    CdcrUpdateLog.CdcrLogReader reader1 = ((CdcrUpdateLog) ulog).newLogReader();
    CdcrUpdateLog.CdcrLogReader reader2 = ((CdcrUpdateLog) ulog).newLogReader();

    int start = 0;

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(105, start, versions);
    start += 105;
    assertU(commit());

    // the previous two tlogs should not be removed
    assertEquals(3, ulog.getLogList(logDir).length);

    // move the first pointer past the first tlog
    for (int i = 0; i <= 11; i++) { // 10 adds + 1 commit
      assertNotNull(reader1.next());
    }

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    // the first tlog should not be removed
    assertEquals(4, ulog.getLogList(logDir).length);

    // move the second pointer past the first tlog
    for (int i = 0; i <= 11; i++) { // 10 adds + 1 commit
      assertNotNull(reader2.next());
    }

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    // the first tlog should be removed
    assertEquals(4, ulog.getLogList(logDir).length);

    // exhaust the readers
    while (reader1.next() != null) {
    }
    while (reader2.next() != null) {
    }

    // the readers should point to the new tlog
    // now add enough documents to trigger the numRecordsToKeep limit

    addDocs(80, start, versions);
    start += 80;
    assertU(commit());

    // the update log should kept the last 3 tlogs, which sum up to 100 records
    assertEquals(3, ulog.getLogList(logDir).length);
  }

  /**
   * Check that the output stream of an uncapped tlog is correctly reopen
   * and that the commit is written during recovery.
   */
  @Test
  public void testClosingOutputStreamAfterLogReplay() throws Exception {
    this.clearCore();
    try {
      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();

      Deque<Long> versions = new ArrayDeque<>();
      versions.addFirst(addAndGetVersion(sdoc("id", "A11"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A12"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A13"), null));

      assertJQ(req("q", "*:*"), "/response/numFound==0");

      assertJQ(req("qt", "/get", "getVersions", "" + versions.size()), "/versions==" + versions);

      h.close();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // verify that previous close didn't do a commit
      // recovery should be blocked by our hook
      assertJQ(req("q", "*:*"), "/response/numFound==0");

      // unblock recovery
      logReplay.release(1000);

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));

      assertJQ(req("q", "*:*"), "/response/numFound==3");

      // The transaction log should have written a commit and close its output stream
      UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
      assertEquals(0, ulog.logs.peekLast().refcount.get());
      assertNull(ulog.logs.peekLast().channel);

      ulog.logs.peekLast().incref(); // reopen the output stream to check if its ends with a commit
      assertTrue(ulog.logs.peekLast().endsWithCommit());
      ulog.logs.peekLast().decref();
    } finally {
      TestInjection.skipIndexWriterCommitOnClose = false; // reset
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }

  /**
   * Check the buffering of the old tlogs
   */
  @Test
  public void testBuffering() throws Exception {
    this.clearCore();

    CdcrUpdateLog ulog = (CdcrUpdateLog) h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

    int start = 0;

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(105, start, versions);
    start += 105;
    assertU(commit());

    // the first two tlogs should have been removed
    assertEquals(1, ulog.getLogList(logDir).length);

    // enable buffer
    ulog.enableBuffer();

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(105, start, versions);
    start += 105;
    assertU(commit());

    // no tlog should have been removed
    assertEquals(4, ulog.getLogList(logDir).length);

    // disable buffer
    ulog.disableBuffer();

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    // old tlogs should have been removed
    assertEquals(2, ulog.getLogList(logDir).length);
  }


  @Test
  public void testSubReader() throws Exception {
    this.clearCore();

    CdcrUpdateLog ulog = (CdcrUpdateLog) h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());
    CdcrUpdateLog.CdcrLogReader reader = ulog.newLogReader();

    int start = 0;

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    assertEquals(2, ulog.getLogList(logDir).length);

    // start to read the first tlog
    for (int i = 0; i < 10; i++) {
      assertNotNull(reader.next());
    }

    // instantiate a sub reader, and finish to read the first tlog (commit operation), plus start to read the
    // second tlog (first five adds)
    CdcrUpdateLog.CdcrLogReader subReader = reader.getSubReader();
    for (int i = 0; i < 6; i++) {
      assertNotNull(subReader.next());
    }

    // Five adds + one commit
    assertEquals(6, subReader.getNumberOfRemainingRecords());

    // Generate a new tlog
    addDocs(105, start, versions);
    start += 105;
    assertU(commit());

    // Even if the subreader is past the first tlog, the first tlog should not have been removed
    // since the parent reader is still pointing to it
    assertEquals(3, ulog.getLogList(logDir).length);

    // fast forward the parent reader with the subreader
    reader.forwardSeek(subReader);
    subReader.close();

    // After fast forward, the parent reader should be position on the doc15
    @SuppressWarnings({"rawtypes"})
    List o = (List) reader.next();
    assertNotNull(o);
    assertTrue("Expected SolrInputDocument but got" + o.toString() ,o.get(3) instanceof SolrInputDocument);
    assertEquals("15", ((SolrInputDocument) o.get(3)).getFieldValue("id"));

    // Finish to read the second tlog, and start to read the third one
    for (int i = 0; i < 6; i++) {
      assertNotNull(reader.next());
    }

    assertEquals(105, reader.getNumberOfRemainingRecords());

    // Generate a new tlog to activate tlog cleaning
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    // If the parent reader was correctly fast forwarded, it should be on the third tlog, and the first two should
    // have been removed.
    assertEquals(2, ulog.getLogList(logDir).length);
  }

  /**
   * Check that the reader is correctly reset to its last position
   */
  @Test
  public void testResetToLastPosition() throws Exception {
    this.clearCore();

    CdcrUpdateLog ulog = (CdcrUpdateLog) h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());
    CdcrUpdateLog.CdcrLogReader reader = ulog.newLogReader();

    int start = 0;

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    assertEquals(2, ulog.getLogList(logDir).length);

    for (int i = 0; i < 22; i++) {
      Object o = reader.next();
      assertNotNull(o);
      // reset to last position
      reader.resetToLastPosition();
      // we should read the same update operation, i.e., same version number
      assertEquals(((List) o).get(1), ((List) reader.next()).get(1));
    }
    assertNull(reader.next());
  }

  /**
   * Check that the reader is correctly reset to its last position
   */
  @Test
  public void testGetNumberOfRemainingRecords() throws Exception {
    try {
      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplayFinish = new Semaphore(0);
      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();

      this.clearCore();

      int start = 0;

      LinkedList<Long> versions = new LinkedList<>();
      addDocs(10, start, versions);
      start += 10;
      assertU(commit());

      addDocs(10, start, versions);
      start += 10;

      h.close();
      logReplayFinish.drainPermits();
      createCore();

      // At this stage, we have re-opened a capped tlog, and an uncapped tlog.
      // check that the number of remaining records is correctly computed in these two cases

      CdcrUpdateLog ulog = (CdcrUpdateLog) h.getCore().getUpdateHandler().getUpdateLog();
      CdcrUpdateLog.CdcrLogReader reader = ulog.newLogReader();

      // wait for the replay to finish
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));

      // 20 records + 2 commits
      assertEquals(22, reader.getNumberOfRemainingRecords());

      for (int i = 0; i < 22; i++) {
        Object o = reader.next();
        assertNotNull(o);
        assertEquals(22 - (i + 1), reader.getNumberOfRemainingRecords());
      }
      assertNull(reader.next());
      assertEquals(0, reader.getNumberOfRemainingRecords());

      // It should pick up the new tlog files
      addDocs(10, start, versions);
      assertEquals(10, reader.getNumberOfRemainingRecords());
    } finally {
      TestInjection.skipIndexWriterCommitOnClose = false; // reset
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }

  /**
   * Check that the initialisation of the log reader is picking up the tlog file that is currently being
   * written.
   */
  @Test
  public void testLogReaderInitOnNewTlog() throws Exception {
    this.clearCore();

    int start = 0;

    // Start to index some documents to instantiate the new tlog
    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;

    // Create the reader after the instantiation of the new tlog
    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    CdcrUpdateLog.CdcrLogReader reader = ((CdcrUpdateLog) ulog).newLogReader();

    // Continue to index documents and commits
    addDocs(11, start, versions);
    start += 11;
    assertU(commit());

    // check that the log reader was initialised with the new tlog
    for (int i = 0; i < 22; i++) { // 21 adds + 1 commit
      assertNotNull(reader.next());
    }

    // we should have reach the end of the new tlog
    assertNull(reader.next());
  }

  /**
   * Check that the absolute version number is used for the update log index and for the last entry read
   */
  @Test
  public void testAbsoluteLastVersion() throws Exception {
    this.clearCore();

    CdcrUpdateLog ulog = (CdcrUpdateLog) h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());
    CdcrUpdateLog.CdcrLogReader reader = ulog.newLogReader();

    int start = 0;

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions);
    start += 10;
    deleteByQuery("*:*");
    assertU(commit());

    deleteByQuery("*:*");
    addDocs(10, start, versions);
    start += 10;
    assertU(commit());

    assertEquals(2, ulog.getLogList(logDir).length);

    for (long version : ulog.getStartingVersions()) {
      assertTrue(version > 0);
    }

    for (int i = 0; i < 10; i++) {
      reader.next();
    }

    // first delete
    Object o = reader.next();
    assertTrue((Long) ((List) o).get(1) < 0);
    assertTrue(reader.getLastVersion() > 0);

    reader.next(); // commit

    // second delete
    o = reader.next();
    assertTrue((Long) ((List) o).get(1) < 0);
    assertTrue(reader.getLastVersion() > 0);
  }

}

