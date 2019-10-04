package org.apache.solr.store.blob.process;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.store.blob.client.CoreStorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task in charge of deleting Blobs (files) from blob store.
 */
class BlobDeleterTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Note we sleep() after each failed attempt, so multiply this value by {@link #SLEEP_MS_FAILED_ATTEMPT} to find
   * out how long we'll retry (at least) if Blob access fails for some reason ("at least" because we
   * re-enqueue at the tail of the queue ({@link BlobDeleteManager} creates a list), so there might be additional
   * processing delay if the queue is not empty and is processed before the enqueued retry is processed).
   */
  private static int MAX_DELETE_ATTEMPTS = 50;
  private static long SLEEP_MS_FAILED_ATTEMPT = TimeUnit.SECONDS.toMillis(10);

  private final CoreStorageClient client;
  private final String sharedBlobName;
  private final Set<String> blobNames;
  private final AtomicInteger attempt;
  private final ThreadPoolExecutor executor;
  private final long queuedTimeMs;

  BlobDeleterTask(CoreStorageClient client, String sharedBlobName, Set<String> blobNames, ThreadPoolExecutor executor) {
    this.client = client; 
    this.sharedBlobName = sharedBlobName;
    this.blobNames = blobNames;
    this.attempt = new AtomicInteger(0);
    this.executor = executor;
    this.queuedTimeMs = System.currentTimeMillis();
  }

  @Override
  public void run() {
    final long startTimeMs = System.currentTimeMillis();
    boolean isSuccess = true;
      
    try {
      client.deleteBlobs(blobNames);
      // Blob might not have been deleted if at some point we've enqueued files to delete while doing a core push,
      // but the push ended up failing and the core.metadata file was not updated. We ended up with the blobs enqueued for
      // delete and eventually removed by a BlobDeleterTask and the files to delete still present in core.metadata
      // so enqueued again.
      // Note it is ok to delete these files even if the core.metadata update fails. The delete is not linked
      // to the push activity, it is related to blobs marked for delete that can be safely removed after some delay has passed.
      } catch (Exception e) {
        isSuccess = false;
        int attempts = attempt.incrementAndGet();

        log.warn("Blob file delete task failed."
                +" attempt=" + attempts +  " sharedBlobName=" + this.sharedBlobName + " numOfBlobs=" + this.blobNames.size(), e);

        if (attempts < MAX_DELETE_ATTEMPTS) {
          // We failed, but we'll try again. Enqueue the task for a new delete attempt. attempt already increased.
          // Note this execute call accepts the
          try {
            // Some delay before retry... (could move this delay to before trying to delete a file that previously
            // failed to be deleted, that way if the queue is busy and it took time to retry, we don't add an additional
            // delay on top of that. On the other hand, an exception here could be an issue with the Blob store
            // itself and nothing specific to the file at hand, so slowing all delete attempts for all files might
            // make sense. Splunk will eventually tell us... or not.
            Thread.sleep(SLEEP_MS_FAILED_ATTEMPT);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
          // This can throw an exception if the pool is shutting down.
          executor.execute(this);
        }
      } finally {
        long now = System.currentTimeMillis();
        long runTime = now - startTimeMs;
        long startLatency = now - this.queuedTimeMs;
        String message = String.format("sharedBlobName=%s action=%s storageProvider=%s bucketRegion=%s bucketName=%s "
                      + "runTime=%s startLatency=%s bytesTransferred=%s attempt=%s localGenerationNumber=%s "
                      + "blobGenerationNumber=%s filesAffected=%s isSuccess=%s",
                      sharedBlobName, "DELETE", client.getStorageProvider().name(), client.getBucketRegion(),
                client.getBucketName(), runTime, startLatency, 0L, attempt.get(), -1L,
                -1L, this.blobNames.size(), isSuccess);
        log.info(message);
      }
  }
}
