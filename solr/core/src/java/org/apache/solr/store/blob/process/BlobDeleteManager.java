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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.metadata.ServerSideMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class manages the deletion machinery required by shared storage enabled collections. Its responsibilities
 * include the allocation and management of bounded deletion task queues and their consumers. 
 * 
 * Deletion of blob files from shared store happen on two paths:
 *  1. In the indexing path, the local {@link SolrCore}'s index files represented by an instance of a
 *  {@link ServerSideMetadata} object is resolved against the blob store's core.metadata file, or the
 *  the source of truth for what index files a {@link SolrCore} should have. As the difference between 
 *  these two metadata instances are resolved, we add files to be deleted to the BlobDeleteManager which
 *  enqueues a {@link BlobDeleterTask} for asynchronous processing.
 *  2. In the collection admin API, we may delete a collection or collection shard. In the former, all index
 *  files belonging to the specified collection on shared storage should be deleted while in the latter 
 *  all index files belonging to a particular collection/shard pair should be deleted.   
 * 
 * Shard leaders are the only replicas receiving indexing traffic and pushing to shared store in a shared collection
 * so all Solr nodes in a cluster may be sending deletion requests to the shared storage provider at a given moment.
 * Collection commands are only processed by the Overseer and therefore only the Overseer should be deleting entire
 * collections or shard files from shared storage.
 * 
 * The BlobDeleteManager maintains two queues to prevent any potential starvation, one for the incremental indexing 
 * deletion path that is always initiated when a Solr node with shared collections starts up and one that is only
 * used when the current node is Overseer and handles Overseer specific actions.
 */
public class BlobDeleteManager {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  /**
   * Identifier for a BlobDeleteProcessor that runs on all Solr nodes containing shared collections
   */
  public static final String BLOB_FILE_DELETER = "BlobFileDeleter";
  
  /**
   * Identifier for a BlobDeleteProcessor that runs on the Overseer if the Solr cluster contains
   * any shared collection
   */
  public static final String OVERSEER_BLOB_FILE_DELETER = "OverseerBlobFileDeleter";
  
  /**
   * Limit to the number of blob files to delete accepted on the delete queue (and lost in case of server crash). When
   * the queue reaches that size, no more deletes are accepted (will be retried later for a core, next time it is pushed).
   */
  private static final int DEFAULT_ALMOST_MAX_DELETER_QUEUE_SIZE = 200;
  
  private static final int DEFAULT_THREAD_POOL_SIZE = 5;
  
  // TODO: 30 seconds is currently chosen arbitrarily and these values should be based as configurable values
  private static final int DEFAULT_DELETE_DELAY_MS = 30000;

  private static int MAX_DELETE_ATTEMPTS = 50;
  private static long SLEEP_MS_FAILED_ATTEMPT = TimeUnit.SECONDS.toMillis(10);
  
  private final CoreStorageClient storageClient;
  
  private final BlobDeleteProcessor deleteProcessor;
  private final BlobDeleteProcessor overseerDeleteProcessor;

  /**
   * After a core push has marked a file as deleted, wait at least this long before actually deleting its blob from the
   * blob store, just in case a concurrent (unexpected) update to the core metadata on the Blob store assumes the file
   * is still present... (given reading and writing core metadata on the blob are not transaction/atomic, if two updates
   * occur at the same time, the last writer wins and might "resuscitate" files marked for delete by the first one).<p>
   *
   * The delay here should be longer than twice the longest observed push operation, as logged from the end of {@link CorePushPull}.
   * Why twice? because files are marked for delete before pushing happens, so by the time we consider if physical delete should happen,
   * there's already one push interval that has gone by. If we consider that just before the write of core.metadata back
   * to blob store by this first push operation another push starts and reads core.metadata, then that other push can again
   * take quite a long time before writing back core.metadata in which the files initially marked as deleted should not longer
   * be deleted. If we want these files to not be deleted before the second push completes, then the hard delete delay should
   * be twice the longest possible push. Note that if the second push requires these files as part of the core (no longer
   * deleted) then they would have disappeared from the deleted file list in core.metadata and no further delete enqueue
   * will be made for them. In other words, there's nothing to cancel, the strategy works by not enqueueing a physical
   * delete until we know for sure the file can be resuscitated...
   */
  private final long deleteDelayMs;
  
  private AtomicBoolean isShutdown; 

  /**
   * Creates a new BlobDeleteManager with the provided {@link CoreStorageClient} and instantiates
   * it with a default deleteDelayMs, queue size, and thread pool size. A default {@link BlobDeleteProcessor}
   * is also created.
   */
  public BlobDeleteManager(CoreStorageClient client) {
    this(client, DEFAULT_ALMOST_MAX_DELETER_QUEUE_SIZE, DEFAULT_THREAD_POOL_SIZE, DEFAULT_DELETE_DELAY_MS);
  }
  
  /**
   * Creates a new BlobDeleteManager with the specified BlobDeleteProcessors.
   */
  @VisibleForTesting
  public BlobDeleteManager(CoreStorageClient client, long deleteDelayMs, BlobDeleteProcessor deleteProcessor,
      BlobDeleteProcessor overseerDeleteProcessor) {
    this.deleteDelayMs = deleteDelayMs;
    this.storageClient = client;
    this.isShutdown = new AtomicBoolean(false);
    this.deleteProcessor = deleteProcessor;
    this.overseerDeleteProcessor = overseerDeleteProcessor;
  }
  
  /**
   * Creates a new BlobDeleteManager and default {@link BlobDeleteProcessor}.
   */
  public BlobDeleteManager(CoreStorageClient client, int almostMaxQueueSize, int numDeleterThreads, long deleteDelayMs) {
    this.deleteDelayMs = deleteDelayMs;
    this.storageClient = client;
    this.isShutdown = new AtomicBoolean(false);
    
    deleteProcessor = new BlobDeleteProcessor(BLOB_FILE_DELETER, storageClient, almostMaxQueueSize, numDeleterThreads,
        MAX_DELETE_ATTEMPTS, SLEEP_MS_FAILED_ATTEMPT);
    
    // Non-Overseer nodes will initiate a delete processor but the underlying pool will sit idle
    // until the node is elected and tasks are added. The overhead should be small.
    overseerDeleteProcessor = new BlobDeleteProcessor(OVERSEER_BLOB_FILE_DELETER, storageClient, almostMaxQueueSize, numDeleterThreads,
        MAX_DELETE_ATTEMPTS, SLEEP_MS_FAILED_ATTEMPT);
  }
  
  /**
   * Returns a delete processor for normal indexing flow shared store deletion
   */
  public BlobDeleteProcessor getDeleteProcessor() {
    return deleteProcessor;
  }
  
  /**
   * Returns a delete processor specifically allocated for Overseer use
   */
  public BlobDeleteProcessor getOverseerDeleteProcessor() {
    return overseerDeleteProcessor;
  }

  /**
   * Shuts down all {@link BlobDeleteProcessor} currently managed by the BlobDeleteManager
   */
  public void shutdown() {
    isShutdown.set(true);
    log.info("BlobDeleteManager is shutting down");
    
    deleteProcessor.shutdown();
    log.info("BlobDeleteProcessor " + deleteProcessor.getName() + " has shutdown");
    overseerDeleteProcessor.shutdown();
    log.info("BlobDeleteProcessor " + overseerDeleteProcessor.getName() + " has shutdown");
  }

  /**
   * Get the delay that a file being deleted via the indexing flow must wait before it is 
   * eligible for deletion from shared storage. See {@link BlobDeleteManager#deleteDelayMs} for more
   * details  
   */
  public long getDeleteDelayMs() {
    return deleteDelayMs;
  }
}
