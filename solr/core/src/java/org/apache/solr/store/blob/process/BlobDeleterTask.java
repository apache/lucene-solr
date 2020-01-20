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

package org.apache.solr.store.blob.process;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.process.BlobDeleterTask.BlobDeleterTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic deletion task for files located on shared storage
 */
public abstract class BlobDeleterTask implements Callable<BlobDeleterTaskResult> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final CoreStorageClient client;
  private final String collectionName;
  private final AtomicInteger attempt;
  
  private final long queuedTimeMs;
  private final int maxAttempts;
  private final boolean allowRetry;
  private Throwable err;

  public BlobDeleterTask(CoreStorageClient client, String collectionName, boolean allowRetry,
      int maxAttempts) {
    this.client = client;
    this.collectionName = collectionName;
    this.attempt = new AtomicInteger(0);
    this.queuedTimeMs = System.nanoTime();
    this.allowRetry = allowRetry;
    this.maxAttempts = maxAttempts;
  }
  
  /**
   * Performs a deletion action and request against the shared storage for the given collection
   * and returns the list of file paths deleted
   */
  public abstract Collection<String> doDelete() throws Exception;
  
  /**
   * Return a String representing the action performed by the BlobDeleterTask for logging purposes
   */
  public abstract String getActionName();
  
  @Override
  public BlobDeleterTaskResult call() {
    List<String> filesDeleted = new LinkedList<>();
    final long startTimeMs = System.nanoTime();
    boolean isSuccess = true;
    boolean shouldRetry = false;
    try {
      filesDeleted.addAll(doDelete());
      attempt.incrementAndGet();
      return new BlobDeleterTaskResult(this, filesDeleted, isSuccess, shouldRetry, err);
    } catch (Exception ex) {
      if (err == null) {
        err = ex;
      } else {
        err.addSuppressed(ex);
      }
      int attempts = attempt.incrementAndGet();
      isSuccess = false;
      log.warn("BlobDeleterTask failed on attempt=" + attempts  + " collection=" + collectionName
          + " task=" + toString(), ex);
      if (allowRetry) {
        if (attempts < maxAttempts) {
          shouldRetry = true;
        } else {
          log.warn("Reached " + maxAttempts + " attempt limit for deletion task " + toString() + 
              ". This task won't be retried.");
        }
      }
    } finally {
      long now = System.nanoTime();
      long runTime = now - startTimeMs;
      long startLatency = now - this.queuedTimeMs;
      log(getActionName(), collectionName, runTime, startLatency, isSuccess, getAdditionalLogMessage());
    }
    return new BlobDeleterTaskResult(this, filesDeleted, isSuccess, shouldRetry, err);
  }
  
  /**
   * Override-able by deletion tasks to provide additional action specific logging
   */
  public String getAdditionalLogMessage() {
    return "";
  }
  
  @Override
  public String toString() {
    return "collectionName=" + collectionName + " allowRetry=" + allowRetry + 
        " queuedTimeMs=" + queuedTimeMs + " attemptsTried=" + attempt.get();
  }
  
  public int getAttempts() {
    return attempt.get();
  }

  public void log(String action, String collectionName, long runTime, long startLatency, boolean isSuccess, 
      String additionalMessage) {
    String message = String.format(Locale.ROOT, 
        "action=%s storageProvider=%s bucketRegion=%s bucketName=%s, runTime=%s "
        + "startLatency=%s attempt=%s isSuccess=%s %s",
        action, client.getStorageProvider().name(), client.getBucketRegion(), client.getBucketName(),
        runTime, startLatency, attempt.get(), isSuccess, additionalMessage);
    log.info(message);
  }
  
  /**
   * Represents the result of a deletion task
   */
  public static class BlobDeleterTaskResult {
    private final BlobDeleterTask task;
    private final Collection<String> filesDeleted;
    private final boolean isSuccess;
    private final boolean shouldRetry;
    private final Throwable err;
    
    public BlobDeleterTaskResult(BlobDeleterTask task, Collection<String> filesDeleted, 
        boolean isSuccess, boolean shouldRetry, Throwable errs) {
      this.task = task;
      this.filesDeleted = filesDeleted;
      this.isSuccess = isSuccess;
      this.shouldRetry = shouldRetry;
      this.err = errs;
    }
    
    public boolean isSuccess() {
      return isSuccess;
    }
    
    public boolean shouldRetry() {
      return shouldRetry;
    }
    
    public BlobDeleterTask getTask() {
      return task;
    }
    
    /**
     * @return the files that are being deleted. Note if the task wasn't successful there is no gaurantee
     * all of these files were in fact deleted from shared storage
     */
    public Collection<String> getFilesDeleted() {
      return filesDeleted;
    }
    
    public Throwable getError() {
      return err;
    }
  }
  
  /**
   * A BlobDeleterTask that deletes a given set of blob files from shared store
   */
  public static class BlobFileDeletionTask extends BlobDeleterTask {
    private final CoreStorageClient client;
    private final Set<String> blobNames;
    
    /**
     * Constructor for BlobDeleterTask that deletes a given set of blob files from shared store
     */
    public BlobFileDeletionTask(CoreStorageClient client, String collectionName, Set<String> blobNames, boolean allowRetry,
        int maxRetryAttempt) {
      super(client, collectionName, allowRetry, maxRetryAttempt);
      this.blobNames = blobNames;
      this.client = client;
    }

    @Override
    public Collection<String> doDelete() throws Exception {
      client.deleteBlobs(blobNames);
      return blobNames;
    }

    @Override
    public String getActionName() {
      return "DELETE";
    }
    
    @Override 
    public String getAdditionalLogMessage() {
      return "filesAffected=" + blobNames.size();
    }
    
    @Override
    public String toString() {
      return "BlobFileDeletionTask action=" + getActionName() + " totalFilesSpecified=" + blobNames.size() + 
          " " + super.toString();
    }
  }
  
  /**
   * A BlobDeleterTask that deletes all files from shared storage with the given string prefix 
   */
  public static class BlobPrefixedFileDeletionTask extends BlobDeleterTask {
    private final CoreStorageClient client;
    private final String prefix;
    
    private int affectedFiles = 0;
    
    /**
     * Constructor for BlobDeleterTask that deletes all files from shared storage with the given string prefix 
     */
    public BlobPrefixedFileDeletionTask(CoreStorageClient client, String collectionName, String prefix, boolean allowRetry,
        int maxRetryAttempt) {
      super(client, collectionName, allowRetry, maxRetryAttempt);
      this.prefix = prefix;
      this.client = client;
    }

    @Override
    public List<String> doDelete() throws Exception {
      List<String> allFiles = client.listCoreBlobFiles(prefix);
      affectedFiles = allFiles.size();
      client.deleteBlobs(allFiles);
      return allFiles;
    }

    @Override
    public String getActionName() {
      return "DELETE_FILES_PREFIXED";
    }
    
    @Override 
    public String getAdditionalLogMessage() {
      return "filesAffected=" + affectedFiles;
    }
    
    @Override
    public String toString() {
      return "BlobCollectionDeletionTask action=" + getActionName() + " " + super.toString();
    }
  }
}
