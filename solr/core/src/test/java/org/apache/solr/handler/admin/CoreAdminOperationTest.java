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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

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

    try {
      CoreAdminOperation.STATUS_OP.execute(callInfo);
      fail("Expected core-status execution to fail with exception");
    } catch (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testUnloadUnexpectedFailuresResultIn500SolrException() throws Exception {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.UNLOAD_OP.execute(callInfo);
      fail("Expected core-unload execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testUnloadMissingCoreNameResultsIn400SolrException() throws Exception {
    whenCoreAdminOpHasParams(Maps.newHashMap());
    
    try {
      CoreAdminOperation.UNLOAD_OP.execute(callInfo);
      fail("Expected core-unload execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testReloadUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.RELOAD_OP.execute(callInfo);
      fail("Expected core-reload execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testReloadMissingCoreNameResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());
    
    try {
      CoreAdminOperation.RELOAD_OP.execute(callInfo);
      fail("Expected core-reload execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testCreateUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.CREATE_OP.execute(callInfo);
      fail("Expected core-create execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testCreateMissingCoreNameResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());
    
    try {
      CoreAdminOperation.CREATE_OP.execute(callInfo);
      fail("Expected core-create execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testSwapUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.SWAP_OP.execute(callInfo);
      fail("Expected core-swap execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testSwapMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("other", "some-core-name");
    whenCoreAdminOpHasParams(params);
    
    try {
      CoreAdminOperation.SWAP_OP.execute(callInfo);
      fail("Expected core-swap execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testSwapMissingOtherParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "some-core-name");
    whenCoreAdminOpHasParams(params);
    
    try {
      CoreAdminOperation.SWAP_OP.execute(callInfo);
      fail("Expected core-swap execution to fail when no 'other' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testRenameUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.RENAME_OP.execute(callInfo);
      fail("Expected core-rename execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testRenameMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("other", "some-core-name");
    whenCoreAdminOpHasParams(params);
    
    try {
      CoreAdminOperation.RENAME_OP.execute(callInfo);
      fail("Expected core-rename execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testRenameMissingOtherParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "some-core-name");
    whenCoreAdminOpHasParams(params);
    
    try {
      CoreAdminOperation.RENAME_OP.execute(callInfo);
      fail("Expected core-rename execution to fail when no 'other' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testMergeUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.MERGEINDEXES_OP.execute(callInfo);
      fail("Expected core-merge execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testMergeMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("indexDir", "some/index/dir");
    whenCoreAdminOpHasParams(params);
    
    try {
      CoreAdminOperation.MERGEINDEXES_OP.execute(callInfo);
      fail("Expected core-merge execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testSplitUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.SPLIT_OP.execute(callInfo);
      fail("Expected split execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testSplitMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());
    
    try {
      CoreAdminOperation.SPLIT_OP.execute(callInfo);
      fail("Expected split execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testPrepRecoveryUnexpectedFailuresResultIn500SolrException() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.PREPRECOVERY_OP.execute(callInfo);
      fail("Expected preprecovery execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testRequestRecoveryUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.REQUESTRECOVERY_OP.execute(callInfo);
      fail("Expected request-recovery execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }    
  }
  
  @Test
  public void testRequestRecoveryMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());
    
    try {
      CoreAdminOperation.REQUESTRECOVERY_OP.execute(callInfo);
      fail("Expected request-recovery execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      thrown.printStackTrace();
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }  
  }
  
  @Test
  public void testRequestSyncUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.REQUESTSYNCSHARD_OP.execute(callInfo);
      fail("Expected request-sync execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testRequestSyncMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());
    
    try {
      CoreAdminOperation.REQUESTSYNCSHARD_OP.execute(callInfo);
      fail("Expected request-sync execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }      
  }
  
  @Test
  public void testRequestBufferUpdatesUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.REQUESTBUFFERUPDATES_OP.execute(callInfo);
      fail("Expected request-buffer-updates execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testRequestBufferUpdatesMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    try {
      CoreAdminOperation.REQUESTBUFFERUPDATES_OP.execute(callInfo);
      fail("Expected request-buffer-updates execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }
  }
  
  @Test
  public void testRequestApplyUpdatesUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.REQUESTAPPLYUPDATES_OP.execute(callInfo);
      fail("Expected request-apply-updates execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }    
  }
  
  @Test
  public void testRequestApplyUpdatesMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    try {
      CoreAdminOperation.REQUESTAPPLYUPDATES_OP.execute(callInfo);
      fail("Expected request-apply-updates execution to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }    
  }

  @Test
  public void testOverseerOpUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.OVERSEEROP_OP.execute(callInfo);
      fail("Expected overseerop execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.SERVER_ERROR.code);
    }
  }
  
  @Test
  public void testRequestStatusUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.REQUESTSTATUS_OP.execute(callInfo);
      fail("Expected request-status execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }
  }
  
  @Test
  public void testRequestStatusMissingRequestIdParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    try {
      CoreAdminOperation.REQUESTSTATUS_OP.execute(callInfo);
      fail("Expected request-status execution to fail when no 'requestid' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }    
  }

  @Test
  public void testRejoinLeaderElectionUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.REJOINLEADERELECTION_OP.execute(callInfo);
      fail("Expected rejoin-leader-election execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.SERVER_ERROR.code);
    }    
  }
 

  @Test
  public void testInvokeUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.INVOKE_OP.execute(callInfo);
      fail("Expected invoke execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }    
  }
  
  @Test
  public void testInvokeMissingClassParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    try {
      CoreAdminOperation.INVOKE_OP.execute(callInfo);
      fail("Expected invoke to fail when no 'class' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }    
  }
  
  @Test
  public void testBackupUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.BACKUPCORE_OP.execute(callInfo);
      fail("Expected backup-core execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }  
  }
  
  @Test
  public void testBackupMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("name", "any-name-param");
    whenCoreAdminOpHasParams(params);

    try {
      CoreAdminOperation.BACKUPCORE_OP.execute(callInfo);
      fail("Expected backup-core to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }   
  }
  
  @Test
  public void testBackupMissingNameParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "any-core-param");
    whenCoreAdminOpHasParams(params);

    try {
      CoreAdminOperation.BACKUPCORE_OP.execute(callInfo);
      fail("Expected backup-core to fail when no 'name' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }   
  }
  
  @Test
  public void testRestoreUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.RESTORECORE_OP.execute(callInfo);
      fail("Expected restore-core execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }  
  }
  
  @Test
  public void testRestoreMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("name", "any-name-param");
    whenCoreAdminOpHasParams(params);

    try {
      CoreAdminOperation.RESTORECORE_OP.execute(callInfo);
      fail("Expected restore-core to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }   
  }
  
  @Test
  public void testRestoreMissingNameParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "any-core-param");
    whenCoreAdminOpHasParams(params);

    try {
      CoreAdminOperation.RESTORECORE_OP.execute(callInfo);
      fail("Expected restore-core to fail when no 'name' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }   
  }
  
  @Test
  public void testCreateSnapshotUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.CREATESNAPSHOT_OP.execute(callInfo);
      fail("Expected create-snapshot execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }  
  }
  
  @Test
  public void testCreateSnapshotMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("commitName", "anyCommitName");
    whenCoreAdminOpHasParams(params);

    try {
      CoreAdminOperation.CREATESNAPSHOT_OP.execute(callInfo);
      fail("Expected create-snapshot to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }   
  }
  
  @Test
  public void testCreateSnapshotMissingCommitNameParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "any-core-param");
    whenCoreAdminOpHasParams(params);

    try {
      CoreAdminOperation.CREATESNAPSHOT_OP.execute(callInfo);
      fail("Expected create-snapshot to fail when no 'commitName' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }   
  }
  
  @Test
  public void testDeleteSnapshotUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.DELETESNAPSHOT_OP.execute(callInfo);
      fail("Expected delete-snapshot execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }  
  }
  
  @Test
  public void testDeleteSnapshotMissingCoreParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("commitName", "anyCommitName");
    whenCoreAdminOpHasParams(params);

    try {
      CoreAdminOperation.DELETESNAPSHOT_OP.execute(callInfo);
      fail("Expected delete-snapshot to fail when no 'core' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }   
  }
  
  @Test
  public void testDeleteSnapshotMissingCommitNameParamResultsIn400SolrException() {
    final Map<String, String> params = Maps.newHashMap();
    params.put("core", "any-core-param");
    whenCoreAdminOpHasParams(params);

    try {
      CoreAdminOperation.DELETESNAPSHOT_OP.execute(callInfo);
      fail("Expected delete-snapshot to fail when no 'commitName' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }   
  }
  
  @Test
  public void testListSnapshotUnexpectedFailuresResultIn500Exception() {
    final Throwable cause = new NullPointerException();
    whenUnexpectedErrorOccursDuringCoreAdminOp(cause);
    
    try {
      CoreAdminOperation.LISTSNAPSHOTS_OP.execute(callInfo);
      fail("Expected list-snapshot execution to fail with exception.");
    } catch  (Exception thrown) {
      assertSolrExceptionWithCodeAndCause(thrown, ErrorCode.SERVER_ERROR.code, cause);
    }  
  }
  
  @Test
  public void testListSnapshotMissingCoreParamResultsIn400SolrException() {
    whenCoreAdminOpHasParams(Maps.newHashMap());

    try {
      CoreAdminOperation.LISTSNAPSHOTS_OP.execute(callInfo);
      fail("Expected list-snapshot to fail when no 'commitName' param present");
    } catch (Exception thrown) {
      assertSolrExceptionWithCode(thrown, ErrorCode.BAD_REQUEST.code);
    }  
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
