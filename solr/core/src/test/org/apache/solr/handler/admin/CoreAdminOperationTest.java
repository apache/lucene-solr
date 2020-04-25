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
package org.apache.solr.handler.admin;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoreAdminOperationTest extends SolrTestCaseJ4 {
  
  private CoreAdminHandler.CallInfo callInfo;
  private SolrQueryRequest mockRequest;

  @BeforeClass
  public static void setUpClass() {
    assumeWorkingMockito();
  }

  @Before
  public void setUp() throws Exception{
    super.setUp();
    
    mockRequest = mock(SolrQueryRequest.class);
    callInfo = new CoreAdminHandler.CallInfo(null, mockRequest, null, null);
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @Test
  public void testStatusUnexpectedFailuresResultIn500SolrException() throws Exception {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () -> CoreAdminOperation.STATUS_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testUnloadUnexpectedFailuresResultIn500SolrException() throws Exception {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () -> CoreAdminOperation.UNLOAD_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testUnloadMissingCoreNameResultsIn400SolrException() throws Exception {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () -> CoreAdminOperation.UNLOAD_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testReloadUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () -> CoreAdminOperation.RELOAD_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testReloadMissingCoreNameResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () -> CoreAdminOperation.RELOAD_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testCreateUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.CREATE_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testCreateMissingCoreNameResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.CREATE_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testSwapUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.SWAP_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testSwapMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("other", "some-core-name");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.SWAP_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testSwapMissingOtherParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "some-core-name");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.SWAP_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testRenameUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.RENAME_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testRenameMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("other", "some-core-name");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.RENAME_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testRenameMissingOtherParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "some-core-name");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.RENAME_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testMergeUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.MERGEINDEXES_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testMergeMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("indexDir", "some/index/dir");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.MERGEINDEXES_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testSplitUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.SPLIT_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testSplitMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.SPLIT_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testPrepRecoveryUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.PREPRECOVERY_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testRequestRecoveryUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTRECOVERY_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testRequestRecoveryMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTRECOVERY_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testRequestSyncUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTSYNCSHARD_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testRequestSyncMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTSYNCSHARD_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testRequestBufferUpdatesUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTBUFFERUPDATES_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testRequestBufferUpdatesMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTBUFFERUPDATES_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testRequestApplyUpdatesUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTAPPLYUPDATES_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testRequestApplyUpdatesMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTAPPLYUPDATES_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }

  @Test
  public void testOverseerOpUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.OVERSEEROP_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.SERVER_ERROR.code);
  }
  
  @Test
  public void testRequestStatusUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTSTATUS_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testRequestStatusMissingRequestIdParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REQUESTSTATUS_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }

  @Test
  public void testRejoinLeaderElectionUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.REJOINLEADERELECTION_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.SERVER_ERROR.code);
  }
 

  @Test
  public void testInvokeUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.INVOKE_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testInvokeMissingClassParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.INVOKE_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testBackupUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.BACKUPCORE_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testBackupMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("name", "any-name-param");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.BACKUPCORE_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testBackupMissingNameParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "any-core-param");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.BACKUPCORE_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testRestoreUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.RESTORECORE_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testRestoreMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("name", "any-name-param");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.RESTORECORE_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testRestoreMissingNameParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "any-core-param");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.RESTORECORE_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testCreateSnapshotUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.CREATESNAPSHOT_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testCreateSnapshotMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("commitName", "anyCommitName");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.CREATESNAPSHOT_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testCreateSnapshotMissingCommitNameParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "any-core-param");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.CREATESNAPSHOT_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testDeleteSnapshotUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.DELETESNAPSHOT_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testDeleteSnapshotMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("commitName", "anyCommitName");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.DELETESNAPSHOT_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testDeleteSnapshotMissingCommitNameParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "any-core-param");
    whenCoreAdminOpHasParams(params);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.DELETESNAPSHOT_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  @Test
  public void testListSnapshotUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.LISTSNAPSHOTS_OP.execute(callInfo));
    assertSolrExceptionWithCodeAndCause(ex, ErrorCode.SERVER_ERROR.code, cause);
  }
  
  @Test
  public void testListSnapshotMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    Exception ex = expectThrows(Exception.class, () ->  CoreAdminOperation.LISTSNAPSHOTS_OP.execute(callInfo));
    assertSolrExceptionWithCode(ex, ErrorCode.BAD_REQUEST.code);
  }
  
  private void whenUnexpectedErrorOccursDuringCoreAdminOp(Throwable cause) {
    when(mockRequest.getParams()).thenThrow(cause);
  }
  
  private void whenCoreAdminOpHasParams(Map<String, String> solrParams) {
    when(mockRequest.getParams()).thenReturn(new MapSolrParams(solrParams));
  }
  
  private void assertSolrExceptionWithCodeAndCause(Throwable thrownException, int expectedStatus, Throwable expectedCause) {
    assertEquals(SolrException.class, thrownException.getClass());
    
    final SolrException solrException = (SolrException) thrownException;
    assertEquals(expectedStatus, solrException.code());
    
    if (expectedCause != null) assertEquals(expectedCause, solrException.getCause());
  }
  
  private void assertSolrExceptionWithCode(Throwable thrownException, int expectedStatus) {
    assertSolrExceptionWithCodeAndCause(thrownException, expectedStatus, null);
  }
}
