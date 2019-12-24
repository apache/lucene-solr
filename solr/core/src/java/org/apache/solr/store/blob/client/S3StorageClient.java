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

package org.apache.solr.store.blob.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.solr.common.StringUtils;
import org.apache.solr.util.FileUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.google.common.collect.Iterables;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobClientUtils;
import org.apache.solr.store.blob.client.ToFromJson;

/**
 * This class implements an AmazonS3 client for reading and writing search index
 * data to AWS S3.
 */
public class S3StorageClient implements CoreStorageClient {

  private final AmazonS3 s3Client;

  /** The S3 bucket where we write all of our blobs to */
  private final String blobBucketName;

  // S3 has a hard limit of 1000 keys per batch delete request
  private static final int MAX_KEYS_PER_BATCH_DELETE = 1000;

  /**
   * Construct a new S3StorageClient that is an implementation of the
   * CoreStorageClient using AWS S3 as the underlying blob store service provider.
   */
  public S3StorageClient() throws IOException {
    String credentialsFilePath = AmazonS3Configs.CREDENTIALS_FILE_PATH.getValue();

    // requires credentials file on disk to authenticate with S3
    if (!FileUtils.fileExists(credentialsFilePath)) {
      throw new IOException("Credentials file does not exist in " + credentialsFilePath);
    }
    
    /*
     * default s3 client builder loads credentials from disk and handles token refreshes
     */
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
    s3Client = builder
        .withPathStyleAccessEnabled(true)
        .withRegion(Regions.fromName(AmazonS3Configs.REGION.getValue()))
        .build();
    
    blobBucketName = AmazonS3Configs.BUCKET_NAME.getValue();
  }

  @Override
  public void pushCoreMetadata(String sharedStoreName, String blobCoreMetadataName, BlobCoreMetadata bcm)
      throws BlobException {
    try {
      ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
      String json = converter.toJson(bcm);

      String blobCoreMetadataPath = getBlobMetadataPath(sharedStoreName, blobCoreMetadataName);
      /*
       * Encodes contents of the string into an S3 object. If no exception is thrown
       * then the object is guaranteed to have been stored
       */
      s3Client.putObject(blobBucketName, blobCoreMetadataPath, json);
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public BlobCoreMetadata pullCoreMetadata(String sharedStoreName, String blobCoreMetadataName) throws BlobException {
    try {
      String blobCoreMetadataPath = getBlobMetadataPath(sharedStoreName, blobCoreMetadataName);

      if (!coreMetadataExists(sharedStoreName, blobCoreMetadataName)) {
        return null;
      }

      String decodedJson = s3Client.getObjectAsString(blobBucketName, blobCoreMetadataPath);
      ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
      return converter.fromJson(decodedJson, BlobCoreMetadata.class);
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public InputStream pullStream(String path) throws BlobException {
    try {
      S3Object requestedObject = s3Client.getObject(blobBucketName, path);
      // This InputStream instance needs to be closed by the caller
      return requestedObject.getObjectContent();
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public String pushStream(String blobName, InputStream is, long contentLength, String fileNamePrefix)
      throws BlobException {
    try {
      /*
       * This object metadata is associated per blob. This is different than the Solr
       * Core metadata {@link BlobCoreMetadata} which sits as a separate blob object
       * in the store. At minimum, ObjectMetadata requires the content length of the
       * object to be set in the request header.
       */
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(contentLength);

      String blobPath = BlobClientUtils.generateNewBlobCorePath(blobName, fileNamePrefix);
      PutObjectRequest putRequest = new PutObjectRequest(blobBucketName, blobPath, is, objectMetadata);

      s3Client.putObject(putRequest);
      is.close();
      return blobPath;
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public boolean coreMetadataExists(String sharedStoreName, String blobCoreMetadataName) throws BlobException {
    try {
      return s3Client.doesObjectExist(blobBucketName, getBlobMetadataPath(sharedStoreName, blobCoreMetadataName));
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  private String getBlobMetadataPath(String sharedStoreName, String blobCoreMetadataName) {
    return BlobClientUtils.concatenatePaths(sharedStoreName, blobCoreMetadataName);
  }

  /**
   * Deletes all blob files associated with this blobName.
   * 
   * This is simply added here to handle cleaning up cores in the blob store for
   * testing. Does not handle failed delete exceptions and no synchronization.
   * 
   * First sends a request to the BlobStore and gets a list of all blob file
   * summaries prefixed by the given blobName. Gets the key for each blob and
   * sends a delete request for all of those keys.
   */
  @Override
  public void deleteCore(String blobName) throws BlobException {
    try {
      ListObjectsRequest listRequest = new ListObjectsRequest();
      listRequest.setBucketName(blobBucketName);
      listRequest.setPrefix(blobName);

      List<String> blobFiles = new LinkedList<>();
      ObjectListing objectListing = s3Client.listObjects(listRequest);
      iterateObjectListingAndConsume(objectListing, input -> {
        String key = input.getKey();
        blobFiles.add(key);
      });
      deleteObjects(blobFiles);
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public void deleteBlobs(Collection<String> paths) throws BlobException {
    try {
      /*
       * Per the S3 docs:
       * https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/
       * s3/model/DeleteObjectsResult.html An exception is thrown if there's a client
       * error processing the request or in the Blob store itself. However there's no
       * guarantee the delete did not happen if an exception is thrown.
       */
      deleteObjects(paths);
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public BlobstoreProviderType getStorageProvider() {
    return BlobstoreProviderType.S3;
  }

  @Override
  public String getBucketRegion() {
    return s3Client.getRegionName();
  }

  @Override
  public String getBucketName() {
    return blobBucketName;
  }

  @Override
  public String getEndpoint() {
    return "s3." + s3Client.getBucketLocation(blobBucketName) + ".amazonaws.com";
  }

  @Override
  public boolean doesBucketExist() throws BlobException {
    try {
      return s3Client.doesBucketExistV2(blobBucketName);
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public void shutdown() {
    s3Client.shutdown();
  }

  @Override
  public List<String> listCoreBlobFilesOlderThan(String blobName, long timestamp) throws BlobException {
    ListObjectsRequest listRequest = new ListObjectsRequest();
    listRequest.setBucketName(blobBucketName);
    listRequest.setPrefix(blobName);

    List<String> blobFiles = new LinkedList<>();
    try {
      ObjectListing objectListing = s3Client.listObjects(listRequest);
      iterateObjectListingAndConsume(objectListing, object -> {
        long lastModifiedTimestamp = object.getLastModified().getTime();
        if (lastModifiedTimestamp < timestamp) {
          blobFiles.add(object.getKey());
        }
      });
      return blobFiles;
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public List<String> listCommonBlobPrefix(String prefix) throws BlobException {
    ListObjectsRequest listRequest = new ListObjectsRequest();
    listRequest.setBucketName(blobBucketName);
    listRequest.setPrefix(prefix);
    listRequest.setDelimiter(BlobClientUtils.BLOB_FILE_PATH_DELIMITER);
    List<String> commonPrefixList = new LinkedList<>();
    try {
      ObjectListing objectListing = s3Client.listObjects(listRequest);

      while (true) {
        // strip the trailing delimiter character that gets appended to each string
        // prefix
        List<String> prefixes = objectListing.getCommonPrefixes().stream()
            .map(commonPrefix -> commonPrefix.substring(0,
                commonPrefix.length() - BlobClientUtils.BLOB_FILE_PATH_DELIMITER.length()))
            .collect(Collectors.toList());

        commonPrefixList.addAll(prefixes);
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
      return commonPrefixList;
    } catch (AmazonServiceException ase) {
      throw handleAmazonServiceException(ase);
    } catch (AmazonClientException ace) {
      throw new BlobClientException(ace);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  private BlobException handleAmazonServiceException(AmazonServiceException ase) {
    String errMessage = String.format(Locale.ROOT,
        "An AmazonServiceException was thrown! [serviceName=%s] "
            + "[awsRequestId=%s] [httpStatus=%s] [s3ErrorCode=%s] [s3ErrorType=%s] [message=%s]",
        ase.getServiceName(), ase.getRequestId(), ase.getStatusCode(), ase.getErrorCode(), ase.getErrorType(),
        ase.getErrorMessage());
    return new BlobServiceException(errMessage, ase);
  }

  // Note: we need to batch our keys into chunks for the s3 client, but the GCS
  // client library does this for us.
  private void deleteObjects(Collection<String> paths) {
    List<DeleteObjectsRequest> deleteRequests = new LinkedList<>();

    // batch our deletes to MAX_KEYS_PER_BATCH_DELETE keys per request
    Iterables.partition(Iterables.transform(paths, p -> new KeyVersion(p)), MAX_KEYS_PER_BATCH_DELETE)
        .forEach(batch -> deleteRequests.add(createBatchDeleteRequest(batch)));

    for (DeleteObjectsRequest req : deleteRequests) {
      s3Client.deleteObjects(req);
    }
  }

  private DeleteObjectsRequest createBatchDeleteRequest(List<KeyVersion> keysToDelete) {
    DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(blobBucketName);
    deleteRequest.setKeys(keysToDelete);
    return deleteRequest;
  }

  // helper to iterate object listings which may be truncated
  private void iterateObjectListingAndConsume(ObjectListing objectListing, Consumer<S3ObjectSummary> consumer) {
    while (true) {
      Iterator<S3ObjectSummary> iter = objectListing.getObjectSummaries().iterator();
      while (iter.hasNext()) {
        consumer.accept(iter.next());
      }

      if (objectListing.isTruncated()) {
        objectListing = s3Client.listNextBatchOfObjects(objectListing);
      } else {
        break;
      }
    }
  }

  /**
   * S3 configurations that are stored as environment variables
   */
  private enum AmazonS3Configs {
    CREDENTIALS_FILE_PATH("AWS_CREDENTIAL_PROFILES_FILE"), 
    BUCKET_NAME("AWS_BUCKET"), 
    REGION("AWS_REGION");

    private String environmentVar;

    AmazonS3Configs(String envVar) {
      this.environmentVar = envVar;
    }

    public String getValue() {
      String value = System.getenv(environmentVar);

      if (StringUtils.isEmpty(value)) {
        throw new IllegalArgumentException("S3StorageClient could not load AmazonS3 configuration in environment variable " + environmentVar);
      }
      return value;
    }
  }
}