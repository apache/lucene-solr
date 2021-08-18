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
package org.apache.solr.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.SyncProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation is adapted from
 * https://github.com/confluentinc/kafka-connect-storage-cloud/blob/5.0.x/kafka-connect-s3/src/main/java/io/confluent/connect/s3/storage/S3OutputStream.java,
 * which uses ASLv2.
 *
 * <p>More recent versions of the kafka-connect-storage-cloud implementation use the CCL license,
 * but this class was based off of the ASLv2 version.
 */
public class S3OutputStream extends OutputStream {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // 16 MB. Part sizes must be between 5MB to 5GB.
  // https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
  static final int PART_SIZE = 16777216;
  static final int MIN_PART_SIZE = 5242880;

  private final AmazonS3 s3Client;
  private final String bucketName;
  private final String key;
  private final SyncProgressListener progressListener;
  private volatile boolean closed;
  private final ByteBuffer buffer;
  private MultipartUpload multiPartUpload;

  public S3OutputStream(AmazonS3 s3Client, String key, String bucketName) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.key = key;
    this.closed = false;
    this.buffer = ByteBuffer.allocate(PART_SIZE);
    this.progressListener = new ConnectProgressListener();
    this.multiPartUpload = null;

    if (log.isDebugEnabled()) {
      log.debug("Created S3OutputStream for bucketName '{}' key '{}'", bucketName, key);
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    buffer.put((byte) b);

    // If the buffer is now full, push it to remote S3.
    if (!buffer.hasRemaining()) {
      uploadPart(false);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    int currentOffset = off;
    int lenRemaining = len;
    while (buffer.remaining() < lenRemaining) {
      int firstPart = buffer.remaining();
      buffer.put(b, currentOffset, firstPart);
      uploadPart(false);

      currentOffset += firstPart;
      lenRemaining -= firstPart;
    }
    if (lenRemaining > 0) {
      buffer.put(b, currentOffset, lenRemaining);
    }
  }

  private static boolean outOfRange(int off, int len) {
    return off < 0 || off > len;
  }

  private void uploadPart(boolean isLastPart) throws IOException {

    int size = buffer.position();

    if (size == 0) {
      // nothing to upload
      return;
    }

    if (multiPartUpload == null) {
      if (log.isDebugEnabled()) {
        log.debug("New multi-part upload for bucketName '{}' key '{}'", bucketName, key);
      }
      multiPartUpload = newMultipartUpload();
    }
    try {
      multiPartUpload.uploadPart(new ByteArrayInputStream(buffer.array()), size, isLastPart);
    } catch (Exception e) {
      if (multiPartUpload != null) {
        multiPartUpload.abort();
        if (log.isDebugEnabled()) {
          log.debug("Multipart upload aborted for bucketName '{}' key '{}'.", bucketName, key);
        }
      }
      throw new S3Exception("Part upload failed: ", e);
    }

    // reset the buffer for eventual next write operation
    buffer.clear();
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    // Flush is possible only if we have more data than the required part size
    // If buffer size is lower than than, just skip
    if (buffer.position() >= MIN_PART_SIZE) {
      uploadPart(false);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    // flush first
    uploadPart(true);

    if (multiPartUpload != null) {
      multiPartUpload.complete();
      multiPartUpload = null;
    }

    closed = true;
  }

  private MultipartUpload newMultipartUpload() throws IOException {
    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(bucketName, key, new ObjectMetadata());

    try {
      return new MultipartUpload(s3Client.initiateMultipartUpload(initRequest).getUploadId());
    } catch (AmazonClientException e) {
      throw S3StorageClient.handleAmazonException(e);
    }
  }

  // Placeholder listener for now, just logs the event progress.
  private static class ConnectProgressListener extends SyncProgressListener {
    public void progressChanged(ProgressEvent progressEvent) {
      if (log.isDebugEnabled()) {
        log.debug("Progress event {}", progressEvent);
      }
    }
  }

  private class MultipartUpload {
    private final String uploadId;
    private final List<PartETag> partETags;

    public MultipartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.partETags = new ArrayList<>();
      if (log.isDebugEnabled()) {
        log.debug(
            "Initiated multi-part upload for bucketName '{}' key '{}' with id '{}'",
            bucketName,
            key,
            uploadId);
      }
    }

    void uploadPart(ByteArrayInputStream inputStream, int partSize, boolean isLastPart) {
      int currentPartNumber = partETags.size() + 1;

      UploadPartRequest request =
          new UploadPartRequest()
              .withKey(key)
              .withBucketName(bucketName)
              .withUploadId(uploadId)
              .withInputStream(inputStream)
              .withPartNumber(currentPartNumber)
              .withPartSize(partSize)
              .withLastPart(isLastPart)
              .withGeneralProgressListener(progressListener);

      if (log.isDebugEnabled()) {
        log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
      }
      partETags.add(s3Client.uploadPart(request).getPartETag());
    }

    /** To be invoked when closing the stream to mark upload is done. */
    void complete() {
      if (log.isDebugEnabled()) {
        log.debug("Completing multi-part upload for key '{}', id '{}'", key, uploadId);
      }
      CompleteMultipartUploadRequest completeRequest =
          new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags);
      s3Client.completeMultipartUpload(completeRequest);
    }

    public void abort() {
      if (log.isWarnEnabled()) {
        log.warn("Aborting multi-part upload with id '{}'", uploadId);
      }
      try {
        s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
      } catch (Exception e) {
        // ignoring failure on abort.
        if (log.isWarnEnabled()) {
          log.warn("Unable to abort multipart upload, you may need to purge uploaded parts: ", e);
        }
      }
    }
  }
}
