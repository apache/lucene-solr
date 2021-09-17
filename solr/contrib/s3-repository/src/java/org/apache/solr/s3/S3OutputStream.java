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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

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

  private final S3Client s3Client;
  private final String bucketName;
  private final String key;
  private volatile boolean closed;
  private final ByteBuffer buffer;
  private MultipartUpload multiPartUpload;

  public S3OutputStream(S3Client s3Client, String key, String bucketName) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.key = key;
    this.closed = false;
    this.buffer = ByteBuffer.allocate(PART_SIZE);
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
      uploadPart();
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
      uploadPart();

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

  private void uploadPart() throws IOException {
    int size = buffer.position() - buffer.arrayOffset();

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
    try (ByteArrayInputStream inputStream =
        new ByteArrayInputStream(buffer.array(), buffer.arrayOffset(), size)) {
      multiPartUpload.uploadPart(inputStream, size);
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
    if (buffer.position() - buffer.arrayOffset() >= MIN_PART_SIZE) {
      uploadPart();
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    // flush first
    uploadPart();

    if (multiPartUpload != null) {
      multiPartUpload.complete();
      multiPartUpload = null;
    }

    closed = true;
  }

  private MultipartUpload newMultipartUpload() throws IOException {
    try {
      return new MultipartUpload(
          s3Client.createMultipartUpload(b -> b.bucket(bucketName).key(key)).uploadId());
    } catch (SdkException e) {
      throw S3StorageClient.handleAmazonException(e);
    }
  }

  private class MultipartUpload {
    private final String uploadId;
    private final List<CompletedPart> completedParts;

    public MultipartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.completedParts = new ArrayList<>();
      if (log.isDebugEnabled()) {
        log.debug(
            "Initiated multi-part upload for bucketName '{}' key '{}' with id '{}'",
            bucketName,
            key,
            uploadId);
      }
    }

    void uploadPart(ByteArrayInputStream inputStream, long partSize) {
      int currentPartNumber = completedParts.size() + 1;

      UploadPartRequest request =
          UploadPartRequest.builder()
              .key(key)
              .bucket(bucketName)
              .uploadId(uploadId)
              .partNumber(currentPartNumber)
              .build();

      if (log.isDebugEnabled()) {
        log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
      }
      UploadPartResponse response =
          s3Client.uploadPart(request, RequestBody.fromInputStream(inputStream, partSize));
      completedParts.add(
          CompletedPart.builder().partNumber(currentPartNumber).eTag(response.eTag()).build());
    }

    /** To be invoked when closing the stream to mark upload is done. */
    void complete() {
      if (log.isDebugEnabled()) {
        log.debug("Completing multi-part upload for key '{}', id '{}'", key, uploadId);
      }
      s3Client.completeMultipartUpload(
          b ->
              b.bucket(bucketName)
                  .key(key)
                  .uploadId(uploadId)
                  .multipartUpload(mub -> mub.parts(completedParts)));
    }

    public void abort() {
      if (log.isWarnEnabled()) {
        log.warn("Aborting multi-part upload with id '{}'", uploadId);
      }
      try {
        s3Client.abortMultipartUpload(b -> b.bucket(bucketName).key(key).uploadId(uploadId));
      } catch (Exception e) {
        // ignoring failure on abort.
        log.error("Unable to abort multipart upload, you may need to purge uploaded parts: ", e);
      }
    }
  }
}
