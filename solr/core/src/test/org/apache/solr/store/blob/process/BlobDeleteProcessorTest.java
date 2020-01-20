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

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.store.blob.client.BlobException;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.client.LocalStorageClient;
import org.apache.solr.store.blob.process.BlobDeleterTask.BlobDeleterTaskResult;
import org.apache.solr.store.blob.process.BlobDeleterTask.BlobFileDeletionTask;
import org.apache.solr.store.blob.process.BlobDeleterTask.BlobPrefixedFileDeletionTask;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link BlobDeleteProcessor}
 */
public class BlobDeleteProcessorTest extends SolrTestCaseJ4 {
  
  private static String DEFAULT_PROCESSOR_NAME = "DeleterForTest";
  private static Path sharedStoreRootPath;
  private static CoreStorageClient blobClient;
  
  private static List<BlobDeleterTask> enqueuedTasks;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");
    System.setProperty(LocalStorageClient.BLOB_STORE_LOCAL_FS_ROOT_DIR_PROPERTY, sharedStoreRootPath.resolve("LocalBlobStore/").toString());
    blobClient = new LocalStorageClient() {
       
      // no ops for BlobFileDeletionTask and BlobPrefixedFileDeletionTask to execute successfully
      @Override
      public void deleteBlobs(Collection<String> paths) throws BlobException {
        return;
      }

      // no ops for BlobFileDeletionTask and BlobPrefixedFileDeletionTask to execute successfully
      @Override
      public List<String> listCoreBlobFiles(String prefix) throws BlobException {
        return new LinkedList<>();
      }
    };
  }
  
  @Before
  public void setup() {
    enqueuedTasks = new LinkedList<BlobDeleterTask>();
  }
  
  /**
   * Verify we enqueue a {@link BlobFileDeletionTask} with the correct parameters.
   * Note we're not testing the functionality of the deletion task here only that the processor successfully
   * handles the task. End to end blob deletion tests can be found {@link SharedStoreDeletionProcessTest} 
   */
  @Test
  public void testDeleteFilesEnqueueTask() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 5;
    int retryDelay = 500; 
    String name = "testName";
    
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(enqueuedTasks, blobClient,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    Set<String> names = new HashSet<>();
    names.add("test1");
    names.add("test2");
    // uses the specified defaultMaxAttempts at the processor (not task) level 
    CompletableFuture<BlobDeleterTaskResult> cf = processor.deleteFiles(name, names, true);
    // wait for this task and all its potential retries to finish
    BlobDeleterTaskResult res = cf.get(5000, TimeUnit.MILLISECONDS);
    assertEquals(1, enqueuedTasks.size());
    
    assertEquals(1, enqueuedTasks.size());
    assertNotNull(res);
    assertEquals(1, res.getTask().getAttempts());
    assertEquals(true, res.isSuccess());
    assertEquals(false, res.shouldRetry());
    
    processor.shutdown();
  }
  
  /**
   * Verify we enqueue a {@link BlobPrefixedFileDeletionTask} with the correct parameters.
   * Note we're not testing the functionality of the deletion task here only that the processor successfully
   * handles the task. End to end blob deletion tests can be found {@link SharedStoreDeletionProcessTest} 
   */
  @Test
  public void testDeleteShardEnqueueTask() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 5;
    int retryDelay = 500; 
    String name = "testName";
    
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(enqueuedTasks, blobClient,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    // uses the specified defaultMaxAttempts at the processor (not task) level 
    CompletableFuture<BlobDeleterTaskResult> cf = processor.deleteCollection(name, true);
    // wait for this task and all its potential retries to finish
    BlobDeleterTaskResult res = cf.get(5000, TimeUnit.MILLISECONDS);
    assertEquals(1, enqueuedTasks.size());
    
    assertEquals(1, enqueuedTasks.size());
    assertNotNull(res);
    assertEquals(1, res.getTask().getAttempts());
    assertEquals(true, res.isSuccess());
    assertEquals(false, res.shouldRetry());
    
    processor.shutdown();
  }
  
  /**
   * Verify we enqueue a {@link BlobPrefixedFileDeletionTask} with the correct parameters.
   * Note we're not testing the functionality of the deletion task here only that the processor successfully
   * handles the task. End to end blob deletion tests can be found {@link SharedStoreDeletionProcessTest} 
   */
  @Test
  public void testDeleteCollectionEnqueueTask() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 5;
    int retryDelay = 500; 
    String name = "testName";
    
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(enqueuedTasks, blobClient,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    // uses the specified defaultMaxAttempts at the processor (not task) level 
    CompletableFuture<BlobDeleterTaskResult> cf = processor.deleteShard(name, name, true);
    // wait for this task and all its potential retries to finish
    BlobDeleterTaskResult res = cf.get(5000, TimeUnit.MILLISECONDS);
    assertEquals(1, enqueuedTasks.size());
    
    assertEquals(1, enqueuedTasks.size());
    assertNotNull(res);
    assertEquals(1, res.getTask().getAttempts());
    assertEquals(true, res.isSuccess());
    assertEquals(false, res.shouldRetry());
    
    processor.shutdown();
  }
  
  /**
   * Verify that we don't retry tasks that are not configured to be retried
   * and end up failing
   */
  @Test
  public void testNonRetryableTask() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 1; // ignored when we build the test task
    int retryDelay = 500;
    int totalAttempts = 5; // total number of attempts the task should be tried 

    String name = "testName";
    boolean isRetry = false;
    
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(enqueuedTasks, blobClient,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    
    // enqueue a task that fails and is not retryable
    CompletableFuture<BlobDeleterTaskResult> cf = 
        processor.enqueue(buildFailingTaskForTest(blobClient, name, totalAttempts, false), isRetry);
    // wait for this task and all its potential retries to finish
    BlobDeleterTaskResult res = cf.get(5000, TimeUnit.MILLISECONDS);
    
    // the first fails
    assertEquals(1, enqueuedTasks.size());
    assertNotNull(res);
    assertEquals(1, res.getTask().getAttempts());
    assertEquals(false, res.isSuccess());
    assertEquals(false, res.shouldRetry());

    // initial error + 0 retry errors suppressed
    assertNotNull(res.getError());
    assertEquals(0, res.getError().getSuppressed().length);
    
    processor.shutdown();
  }
  
  /**
   * Verify that the retry logic kicks in for tasks configured to retry
   * and subsequent retry succeeds
   */
  @Test
  public void testRetryableTaskSucceeds() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 1; // ignored when we build the test task
    int retryDelay = 500;
    int totalAttempts = 5; // total number of attempts the task should be tried 
    int totalFails = 3; // total number of times the task should fail
    
    String name = "testName";
    boolean isRetry = false;
    
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(enqueuedTasks, blobClient,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    // enqueue a task that fails totalFails number of times before succeeding
    CompletableFuture<BlobDeleterTaskResult> cf = 
        processor.enqueue(buildScheduledFailingTaskForTest(blobClient, name, totalAttempts, true, totalFails), isRetry);
    
    // wait for this task and all its potential retries to finish
    BlobDeleterTaskResult res = cf.get(5000, TimeUnit.MILLISECONDS);
    
    // the first 3 fail and last one succeeds
    assertEquals(4, enqueuedTasks.size());
    
    assertNotNull(res);
    assertEquals(4, res.getTask().getAttempts());
    assertEquals(true, res.isSuccess());
    
    // initial error + 2 retry errors suppressed
    assertNotNull(res.getError());
    assertEquals(2, res.getError().getSuppressed().length);
    
    processor.shutdown();
  }
  
  /**
   * Verify that after all task attempts are exhausted we bail out
   */
  @Test
  public void testRetryableTaskFails() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 1; // ignored when we build the test task
    int retryDelay = 500;
    int totalAttempts = 5; // total number of attempts the task should be tried
    
    String name = "testName";
    boolean isRetry = false;
    
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(enqueuedTasks, blobClient,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    // enqueue a task that fails every time it runs but is configured to retry
    CompletableFuture<BlobDeleterTaskResult> cf = 
        processor.enqueue(buildFailingTaskForTest(blobClient, name, totalAttempts, true), isRetry);
    
    // wait for this task and all its potential retries to finish 
    BlobDeleterTaskResult res = cf.get(5000, TimeUnit.MILLISECONDS);
    // 1 initial enqueue + 4 retries
    assertEquals(5, enqueuedTasks.size());
    
    assertNotNull(res);
    assertEquals(5, res.getTask().getAttempts());
    assertEquals(false, res.isSuccess());
    // circuit breaker should be false after all attempts are exceeded
    assertEquals(false, res.shouldRetry());
    
    // initial error + 4 retry errors suppressed
    assertNotNull(res.getError());
    assertEquals(4, res.getError().getSuppressed().length);
    
    processor.shutdown();
  }
  
  /**
   * Verify that we cannot add more deletion tasks to the processor if the work queue
   * is at its target max but that we can re-add tasks that are retries to the queue
   */
  @Test
  public void testWorkQueueFull() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 1;
    int retryDelay = 1000;
    
    String name = "testName";
    boolean allowRetry = false;
    
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(enqueuedTasks, blobClient,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    // numThreads is 1 and we'll enqueue a blocking task that ensures our pool
    // will be occupied while we add new tasks subsequently to test enqueue rejection
    CountDownLatch tasklatch = new CountDownLatch(1);
    processor.enqueue(buildBlockingTaskForTest(tasklatch), allowRetry);

    // Fill the internal work queue beyond the maxQueueSize, the internal queue size is not 
    // approximate so we'll just add beyond the max
    for (int i = 0; i < maxQueueSize*2; i++) {
      try {
        processor.deleteCollection(name, allowRetry);
      } catch (Exception ex) {
        // ignore
      }
    }
    
    // verify adding a new task is rejected
    try {
      processor.deleteCollection(name, allowRetry);
      fail("Task should have been rejected");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("Unable to enqueue deletion"));
    }
    CompletableFuture<BlobDeleterTaskResult> cf = null;
    try {
      // verify adding a task that is marked as a retry is not rejected 
       cf = processor.enqueue(buildFailingTaskForTest(blobClient, name, 5, true), /* isRetry */ true);
    } catch (Exception ex) {
      fail("Task should not have been rejected");
    }
    
    // clean up and unblock the task
    tasklatch.countDown();
    processor.shutdown();
  }
  
  /**
   * Verify that with a continuous stream of delete tasks being enqueued, all eventually complete
   * successfully in the face of failing tasks and retries without locking up our pool anywhere
   */
  @Test
  public void testSimpleConcurrentDeletionEnqueues() throws Exception {
    int maxQueueSize = 200;
    int numThreads = 5;
    int defaultMaxAttempts = 5;
    int retryDelay = 100;
    int numberOfTasks = 200;
    
    BlobDeleteProcessor processor = buildBlobDeleteProcessorForTest(enqueuedTasks, blobClient,
        maxQueueSize, numThreads, defaultMaxAttempts, retryDelay);
    List<BlobDeleterTask> tasks = generateRandomTasks(defaultMaxAttempts, numberOfTasks);
    List<CompletableFuture<BlobDeleterTaskResult>> taskResultsFutures = new LinkedList<>();
    List<BlobDeleterTaskResult> results = new LinkedList<>();
    for (BlobDeleterTask t : tasks) {
      taskResultsFutures.add(processor.enqueue(t, false));
    }
    
    taskResultsFutures.forEach(cf -> {
      try {
        results.add(cf.get(20000, TimeUnit.MILLISECONDS));
      } catch (Exception ex) {
        fail("We timed out on some task!");
      }
    });
    
    // we shouldn't enqueue more than (numberOfTasks * defaultMaxAttempts) tasks to the pool 
    assertTrue(enqueuedTasks.size() < (numberOfTasks * defaultMaxAttempts));
    assertEquals(numberOfTasks, results.size());
    int totalAttempts = 0;
    for (BlobDeleterTaskResult res : results) {
      assertNotNull(res);
      assertNotNull(res.getTask());
      assertEquals("scheduledFailingTask", res.getTask().getActionName());
      totalAttempts += res.getTask().getAttempts();
    }
    // total task attempts should be consistent with our test scaffolding
    assertTrue(totalAttempts < (numberOfTasks * defaultMaxAttempts));
      
    processor.shutdown();
  }
  
  private List<BlobDeleterTask> generateRandomTasks(int defaultMaxAttempts, int taskCount) {
    List<BlobDeleterTask> tasks = new LinkedList<>();
    for (int i = 0; i < taskCount; i++) {
      BlobDeleterTask task = null;
      int totalAttempts = random().nextInt(defaultMaxAttempts);
      int totalFails = random().nextInt(defaultMaxAttempts + 1);
      task = buildScheduledFailingTaskForTest(blobClient, "test"+i, totalAttempts, true, totalFails);
      tasks.add(task);
    }
    return tasks;
  }
  
  /**
   * Returns a test-only task for just holding onto a resource for test purposes
   */
  private BlobDeleterTask buildBlockingTaskForTest(CountDownLatch latch) {
    return new BlobDeleterTask(null, null, false, 0) {
      @Override
      public Collection<String> doDelete() throws Exception {
        // block until something forces this latch to count down
        latch.await();
        return null;
      }
      
      @Override
      public String getActionName() { return "blockingTask"; }
    };
  }
  
  /**
   * Returns a test-only task that always fails on action execution by throwing an
   * exception
   */
  private BlobDeleterTask buildFailingTaskForTest(CoreStorageClient client, 
      String collectionName, int maxRetries, boolean allowRetries) {
    return new BlobDeleterTask(client, collectionName, allowRetries, maxRetries) {
      @Override
      public Collection<String> doDelete() throws Exception {
        throw new Exception("");
      }
      
      @Override
      public String getActionName() { return "failingTask"; }
    };
  }
  
  /**
   * Returns a test-only task that fails a specified number of times before succeeding
   */
  private BlobDeleterTask buildScheduledFailingTaskForTest(CoreStorageClient client, 
      String collectionName, int maxRetries, boolean allowRetries, int failTotal) {
    return new BlobDeleterTask(client, collectionName, allowRetries, maxRetries) {
      private AtomicInteger failCount = new AtomicInteger(0);
      
      @Override
      public Collection<String> doDelete() throws Exception {
        while (failCount.get() < failTotal) {
          failCount.incrementAndGet();
          throw new Exception("");
        }
        return null;
      }
      
      @Override
      public String getActionName() { return "scheduledFailingTask"; }
    };
  }
  
  // enables capturing all enqueues to the executor pool, including retries
  private BlobDeleteProcessor buildBlobDeleteProcessorForTest(List<BlobDeleterTask> enqueuedTasks, 
      CoreStorageClient client, int almostMaxQueueSize, int numDeleterThreads, int defaultMaxDeleteAttempts, 
      long fixedRetryDelay) {
    return new BlobDeleteProcessorForTest(DEFAULT_PROCESSOR_NAME, client, almostMaxQueueSize, numDeleterThreads,
        defaultMaxDeleteAttempts, fixedRetryDelay, enqueuedTasks);
  }
  
  class BlobDeleteProcessorForTest extends BlobDeleteProcessor {
    List<BlobDeleterTask> enqueuedTasks;
    
    public BlobDeleteProcessorForTest(String name, CoreStorageClient client, int almostMaxQueueSize,
        int numDeleterThreads, int defaultMaxDeleteAttempts, long fixedRetryDelay, List<BlobDeleterTask> enqueuedTasks) {
      super(name, client, almostMaxQueueSize, numDeleterThreads, defaultMaxDeleteAttempts, fixedRetryDelay);
      this.enqueuedTasks = enqueuedTasks;
    }
    
    @Override
    protected CompletableFuture<BlobDeleterTaskResult> enqueue(BlobDeleterTask task, boolean isRetry) {
      enqueuedTasks.add(task);
      return super.enqueue(task, isRetry);
    }
  }
}
