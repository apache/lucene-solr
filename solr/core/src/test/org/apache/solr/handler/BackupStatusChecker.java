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
package org.apache.solr.handler;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;

import static org.apache.solr.SolrTestCaseJ4.params;

import static org.apache.lucene.util.LuceneTestCase.assertNotNull;
import static org.apache.lucene.util.LuceneTestCase.assertNull;
import static org.apache.lucene.util.LuceneTestCase.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for validating when the replication handler has finished a backup.
 *
 */
public final class BackupStatusChecker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  final SolrClient client;
  final String path;

  /** 
   * @param client the client to use in all requests, will not be closed
   * @param path the path to use for accessing the /replication handle when using the client
   */
  public BackupStatusChecker(final SolrClient client, String path) {
    this.client = client;
    this.path = path;
  }
  
  /** 
   * Defaults to a path of <code>/replication</code> 
   * (ie: assumes client is configured with a core specific solr URL).
   *
   * @see #BackupStatusChecker(SolrClient,String)
   */
  public BackupStatusChecker(final SolrClient client) {
    this(client, "/replication");
  }

  /**
   * Convinience wrapper
   * @see #waitForBackupSuccess(String,TimeOut)
   */
  public String waitForBackupSuccess(final String backupName, final int timeLimitInSeconds) throws Exception {
    return waitForBackupSuccess(backupName,
                                new TimeOut(timeLimitInSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME));
  }
  
  /**
   * Polls the replication handler's status until the it reports that the specified backupName is
   * completed as a <code>"success"</code> (in which case the method returns the directoryName of the backup)
   * or either <code>"exception"</code> is reported or the <code>timeOut</code> expires 
   * (in either case an assertion is thrown)
   * 
   * <p>
   * <b>NOTE:</b> this method is <em>NOT</em> suitable/safe to use in a test where multiple backups are 
   * being taken/deleted concurrently, because the replication handler API provides no reliable way to check
   * the results of a specific backup before the results of another backup may overwrite them internally.
   * </p>
   * 
   * @param backupName to look for
   * @param timeOut limiting how long we wait
   * @return the (new) directoryName of the specified backup
   * @see #checkBackupSuccess(String)
   */
  public String waitForBackupSuccess(final String backupName, final TimeOut timeOut) throws Exception {
    assertNotNull("backupName must not be null", backupName);
    while (!timeOut.hasTimedOut()) {
      final String newDirName = checkBackupSuccess(backupName);
      if (null != newDirName) {
        return newDirName;
      }
      timeOut.sleep(50);
    }
    
    // total TimeOut elapsed, so one last check or fail whole test.
    final String newDirName = checkBackupSuccess(backupName);
    assertNotNull(backupName + " did not succeed before the TimeOut elapsed",
                  newDirName);
    return newDirName;
  }
  
  /**
   * Convinience wrapper
   * @see #waitForDifferentBackupDir(String,TimeOut)
   */
  public String waitForDifferentBackupDir(final String directoryName,
                                          final int timeLimitInSeconds) throws Exception {
    
    return waitForDifferentBackupDir(directoryName,
                                     new TimeOut(timeLimitInSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME));
  }
  
  /**
   * Polls the replication handler's status until the it reports that <em>any</em> backup has
   * completed as a <code>"success"</code> with a different <code>"directoryName"</code> then the 
   * one specified (in which case the method returns the new directoryName) or either an
   * <code>"exception"</code> is reported or the <code>timeOut</code> expires 
   * (in either case an assertion is thrown)
   *
   * <p>
   * <b>NOTE:</b> this method is <em>NOT</em> suitable/safe to use in a test where multiple backups are 
   * being taken/deleted concurrently, because the replication handler API provides no reliable way to determine 
   * if the the most recently reported status to the a particular backup request.
   * </p>
   * 
   * @param directoryName to compare to, may be null
   * @param timeOut limiting how long we wait
   * @return the (new) directoryName of the latests successful backup
   * @see #checkBackupSuccess()
   */
  public String waitForDifferentBackupDir(final String directoryName, final TimeOut timeOut) throws Exception {
    while (!timeOut.hasTimedOut()) {
      final String newDirName = checkBackupSuccess();
      if (null != newDirName && ! newDirName.equals(directoryName)) {
        return newDirName;
      }
      timeOut.sleep(50);
    }
    
    // total TimeOut elapsed, so one last check or fail whole test...
    final String newDirName = checkBackupSuccess();
    assertTrue("No successful backup with different directoryName then "
               + directoryName + " before TimeOut elapsed",
               (null != newDirName && ! newDirName.equals(directoryName)));
    return newDirName;
  }
  
  /**
   * Does a single check of the replication handler's status to determine if the mostrecently 
   * completed backup was a success.
   * Throws a test assertion failure if any <code>"exception"</code> message is ever encountered
   * (The Replication Handler API does not make it possible to know <em>which</em> backup 
   * this exception was related to)
   *
   * <p>
   * <b>NOTE:</b> this method is <em>NOT</em> suitable/safe to use in a test where multiple backups are 
   * being taken/deleted concurrently, because the replication handler API provides no reliable way to determine 
   * if the the most recently reported status to the a particular backup request.
   * </p>
   *
   * @returns the "directoryName" of the backup if the response indicates that a is completed successfully, otherwise null
   */
  public String checkBackupSuccess() throws Exception {
    return _checkBackupSuccess(null);
  }
  
  /**
   * Does a single check of the replication handler's status to determine if the specified name matches 
   * the most recently completed backup, and if that backup was a success.
   * Throws a test assertion failure if any <code>"exception"</code> message is ever encountered
   * (The Replication Handler API does not make it possible to know <em>which</em> backup 
   * this exception was related to)
   *
   * @returns the "directoryName" of the backup if the response indicates that the specified backupName is completed successfully, otherwise null
   * @see #waitForBackupSuccess(String,TimeOut)
   */
  public String checkBackupSuccess(final String backupName) throws Exception {
    assertNotNull("backupName must not be null", backupName);
    return _checkBackupSuccess(backupName);
  }
  
  /**
   * Helper method that works with either named or unnamemed backups
   */
  private String _checkBackupSuccess(final String backupName) throws Exception {
    final String label = (null == backupName ? "latest backup" : backupName);
    final SimpleSolrResponse rsp = new GenericSolrRequest(GenericSolrRequest.METHOD.GET, path,
                                                          params("command", "details")).process(client);
    final NamedList data = rsp.getResponse();
    log.info("Checking Status of {}: {}", label, data);
    final NamedList<String> backupData = (NamedList<String>) data.findRecursive("details","backup");
    if (null == backupData) {
      // no backup has finished yet
      return null;
    }
    
    final Object exception = backupData.get("exception");
    assertNull("Backup failure: " + label, exception);

    if ("success".equals(backupData.get("status"))
        && (null == backupName || backupName.equals(backupData.get("snapshotName"))) ) {
      assert null != backupData.get("directoryName");
      return backupData.get("directoryName");
    }
    return null;
  }



  /**
   * Convinience wrapper
   * @see #waitForBackupDeletionSuccess(String,TimeOut)
   */
  public void waitForBackupDeletionSuccess(final String backupName, final int timeLimitInSeconds) throws Exception {
    waitForBackupDeletionSuccess(backupName,
                                 new TimeOut(timeLimitInSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME));
  }
  
  /**
   * Polls the replication handler's status until the it reports that the specified backupName is
   * deleted or either <code>"Unable to delete"</code> status is reported or the <code>timeOut</code> expires 
   * (in either case an assertion is thrown)
   * 
   * <p>
   * <b>NOTE:</b> this method is <em>NOT</em> suitable/safe to use in a test where multiple backups are 
   * being taken/deleted concurrently, because the replication handler API provides no reliable way to check
   * the results of a specific backup before the results of another backup may overwrite them internally.
   * </p>
   * 
   * @param backupName to look for in status
   * @param timeOut limiting how long we wait
   * @see #checkBackupSuccess(String)
   */
  public void waitForBackupDeletionSuccess(final String backupName, final TimeOut timeOut) throws Exception {
    assertNotNull("backumpName must not be null", backupName);
    while (!timeOut.hasTimedOut()) {
      if (checkBackupDeletionSuccess(backupName)) {
        return;
      }
      timeOut.sleep(50);
    }
    
    // total TimeOut elapsed, so one last check or fail whole test.
    assertTrue(backupName + " was not reported as deleted before the TimeOut elapsed",
               checkBackupDeletionSuccess(backupName));
  }
  
  /**
   * Does a single check of the replication handler's status to determine if the specified name matches 
   * the most recently deleted backup, and if deleting that backup was a success.
   * Throws a test assertion failure if the status is about this backupName but the starts message 
   * with <code>"Unable to delete"</code>
   *
   * @returns true if the replication status info indicates the backup was deleted, false otherwise
   * @see #waitForBackupDeletionSuccess(String,TimeOut)
   */
  public boolean checkBackupDeletionSuccess(final String backupName) throws Exception {
    assertNotNull("backumpName must not be null", backupName);
    final SimpleSolrResponse rsp = new GenericSolrRequest(GenericSolrRequest.METHOD.GET, path,
                                                          params("command", "details")).process(client);
    final NamedList data = rsp.getResponse();
    log.info("Checking Deletion Status of {}: {}", backupName, data);
    final NamedList<String> backupData = (NamedList<String>) data.findRecursive("details","backup");
    if (null == backupData
        || null == backupData.get("status")
        || ! backupName.equals(backupData.get("snapshotName")) ) {
      // either no backup activity at all,
      // or most recent activity isn't something we can infer anything from,
      // or is not about the backup we care about...
      return false;
    }
    
    final Object status = backupData.get("status");
    if (status.toString().startsWith("Unable to delete")) {
      // we already know backupData is about our backup
      assertNull("Backup Deleting failure: " + backupName, status);
    }

    if ("success".equals(status) && null != backupData.get("snapshotDeletedAt")) {
      return true; // backup done
    }
    
    // if we're still here then this status is about our backup, but doesn't seem to be a deletion
    return false;
  }
}
