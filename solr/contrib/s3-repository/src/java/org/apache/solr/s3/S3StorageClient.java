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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import java.util.stream.Stream;
import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * Creates a {@link S3Client} for communicating with AWS S3. Utilizes the default credential
 * provider chain; reference <a
 * href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">AWS SDK
 * docs</a> for details on where this client will fetch credentials from, and the order of
 * precedence.
 */
public class S3StorageClient {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String S3_FILE_PATH_DELIMITER = "/";

  // S3 has a hard limit of 1000 keys per batch delete request
  private static final int MAX_KEYS_PER_BATCH_DELETE = 1000;

  // Metadata name used to identify flag directory entries in S3
  private static final String S3_DIR_CONTENT_TYPE = "application/x-directory";

  // Error messages returned by S3 for a key not found.
  private static final Set<String> NOT_FOUND_CODES = Sets.newHashSet("NoSuchKey", "404 Not Found");

  private final S3Client s3Client;

  // The S3 bucket where we read/write all data.
  private final String bucketName;

  S3StorageClient(
      String bucketName,
      String profile,
      String region,
      String proxyUrl,
      boolean proxyUseSystemSettings,
      String endpoint,
      boolean disableRetries) {
    this(
        createInternalClient(
            profile, region, proxyUrl, proxyUseSystemSettings, endpoint, disableRetries),
        bucketName);
  }

  @VisibleForTesting
  S3StorageClient(S3Client s3Client, String bucketName) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
  }

  private static S3Client createInternalClient(
      String profile,
      String region,
      String proxyUrl,
      boolean proxyUseSystemSettings,
      String endpoint,
      boolean disableRetries) {
    S3Configuration.Builder configBuilder = S3Configuration.builder().pathStyleAccessEnabled(true);
    if (!StringUtils.isEmpty(profile)) {
      configBuilder.profileName(profile);
    }

    ApacheHttpClient.Builder sdkHttpClientBuilder = ApacheHttpClient.builder();
    // If configured, add proxy
    ProxyConfiguration.Builder proxyConfigurationBuilder = ProxyConfiguration.builder();
    if (!StringUtils.isEmpty(proxyUrl)) {
      proxyConfigurationBuilder.endpoint(URI.create(proxyUrl));
    } else {
      proxyConfigurationBuilder.useSystemPropertyValues(proxyUseSystemSettings);
    }
    sdkHttpClientBuilder.proxyConfiguration(proxyConfigurationBuilder.build());
    sdkHttpClientBuilder.useIdleConnectionReaper(false);

    /*
     * Retry logic
     */
    RetryPolicy retryPolicy;
    if (disableRetries) {
      retryPolicy = RetryPolicy.none();
    } else {
      RetryMode.Resolver retryModeResolver = RetryMode.resolver();
      if (!StringUtils.isEmpty(profile)) {
        retryModeResolver.profileName(profile);
      }
      RetryMode retryMode = retryModeResolver.resolve();
      RetryPolicy.Builder retryPolicyBuilder = RetryPolicy.builder(retryMode);

      // Do not fail fast on rate limiting
      if (retryMode == RetryMode.ADAPTIVE) {
        retryPolicyBuilder.fastFailRateLimiting(false);
      }

      retryPolicy = retryPolicyBuilder.build();
    }

    /*
     * Set the default credentials provider
     */
    DefaultCredentialsProvider.Builder credentialsProviderBuilder =
        DefaultCredentialsProvider.builder();
    if (!StringUtils.isEmpty(profile)) {
      credentialsProviderBuilder.profileName(profile);
    }

    /*
     * Default s3 client builder loads credentials from disk and handles token refreshes
     */
    S3ClientBuilder clientBuilder =
        S3Client.builder()
            .credentialsProvider(credentialsProviderBuilder.build())
            .overrideConfiguration(builder -> builder.retryPolicy(retryPolicy))
            .serviceConfiguration(configBuilder.build())
            .httpClient(sdkHttpClientBuilder.build());

    if (!StringUtils.isEmpty(endpoint)) {
      clientBuilder.endpointOverride(URI.create(endpoint));
    }
    if (!StringUtils.isEmpty(region)) {
      clientBuilder.region(Region.of(region));
    }

    return clientBuilder.build();
  }

  /** Create a directory in S3, if it does not already exist. */
  void createDirectory(String path) throws S3Exception {
    String sanitizedDirPath = sanitizedDirPath(path);

    // Only create the directory if it does not already exist
    if (!pathExists(sanitizedDirPath)) {
      createDirectory(getParentDirectory(sanitizedDirPath));
      // TODO see https://issues.apache.org/jira/browse/SOLR-15359
      //            throw new S3Exception("Parent directory doesn't exist, path=" + path);

      try {
        // Create empty object with content type header
        PutObjectRequest putRequest =
            PutObjectRequest.builder()
                .bucket(bucketName)
                .contentType(S3_DIR_CONTENT_TYPE)
                .key(sanitizedDirPath)
                .build();
        s3Client.putObject(putRequest, RequestBody.empty());
      } catch (SdkClientException ase) {
        throw handleAmazonException(ase);
      }
    }
  }

  /**
   * Delete files from S3. Deletion order is not guaranteed.
   *
   * @throws S3NotFoundException if the number of deleted objects does not match {@code entries}
   *     size
   */
  void delete(Collection<String> paths) throws S3Exception {
    Set<String> entries = new HashSet<>();
    for (String path : paths) {
      entries.add(sanitizedFilePath(path));
    }

    Collection<String> deletedPaths = deleteObjects(entries);

    // If we haven't deleted all requested objects, assume that's because some were missing
    if (entries.size() != deletedPaths.size()) {
      Set<String> notDeletedPaths = new HashSet<>(entries);
      entries.removeAll(deletedPaths);
      throw new S3NotFoundException(notDeletedPaths.toString());
    }
  }

  /**
   * Delete directory, all the files and sub-directories from S3.
   *
   * @param path Path to directory in S3.
   */
  void deleteDirectory(String path) throws S3Exception {
    path = sanitizedDirPath(path);

    // Get all the files and subdirectories
    Set<String> entries = listAll(path);
    if (pathExists(path)) {
      entries.add(path);
    }

    deleteObjects(entries);
  }

  /**
   * List all the files and sub-directories directly under given path.
   *
   * @param path Path to directory in S3.
   * @return Files and sub-directories in path.
   */
  String[] listDir(String path) throws S3Exception {
    path = sanitizedDirPath(path);

    final String prefix = path;

    try {
      ListObjectsV2Iterable objectListing =
          s3Client.listObjectsV2Paginator(
              builder ->
                  builder
                      .bucket(bucketName)
                      .prefix(prefix)
                      .delimiter(S3_FILE_PATH_DELIMITER)
                      .build());

      return Stream.concat(
              objectListing.contents().stream().map(S3Object::key),
              objectListing.commonPrefixes().stream().map(CommonPrefix::prefix))
          .filter(s -> s.startsWith(prefix))
          .map(s -> s.substring(prefix.length()))
          .filter(s -> !s.isEmpty())
          .filter(
              s -> {
                int slashIndex = s.indexOf(S3_FILE_PATH_DELIMITER);
                return slashIndex == -1 || slashIndex == s.length() - 1;
              })
          .map(
              s -> {
                if (s.endsWith(S3_FILE_PATH_DELIMITER)) {
                  return s.substring(0, s.length() - 1);
                }
                return s;
              })
          .toArray(String[]::new);
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Check if path exists.
   *
   * @param path to File/Directory in S3.
   * @return true if path exists, otherwise false?
   */
  boolean pathExists(String path) throws S3Exception {
    final String s3Path = sanitizedPath(path);

    // for root return true
    if (s3Path.isEmpty() || S3_FILE_PATH_DELIMITER.equals(s3Path)) {
      return true;
    }

    try {
      s3Client.headObject(builder -> builder.bucket(bucketName).key(s3Path));
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Check if path is directory.
   *
   * @param path to File/Directory in S3.
   * @return true if path is directory, otherwise false.
   */
  boolean isDirectory(String path) throws S3Exception {
    final String s3Path = sanitizedDirPath(path);

    try {
      HeadObjectResponse objectMetadata =
          s3Client.headObject(builder -> builder.bucket(bucketName).key(s3Path));
      String contentType = objectMetadata.contentType();

      return !StringUtils.isEmpty(contentType) && contentType.equalsIgnoreCase(S3_DIR_CONTENT_TYPE);
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Get length of file in bytes.
   *
   * @param path to file in S3.
   * @return length of file.
   */
  long length(String path) throws S3Exception {
    String s3Path = sanitizedFilePath(path);
    try {
      HeadObjectResponse objectMetadata =
          s3Client.headObject(b -> b.bucket(bucketName).key(s3Path));
      String contentType = objectMetadata.contentType();

      if (StringUtils.isEmpty(contentType) || !contentType.equalsIgnoreCase(S3_DIR_CONTENT_TYPE)) {
        return objectMetadata.contentLength();
      }
      throw new S3Exception("Path is Directory");
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Open a new {@link InputStream} to file for read. Caller needs to close the stream.
   *
   * @param path to file in S3.
   * @return InputStream for file.
   */
  InputStream pullStream(String path) throws S3Exception {
    final String s3Path = sanitizedFilePath(path);

    try {
      // This InputStream instance needs to be closed by the caller
      return s3Client.getObject(b -> b.bucket(bucketName).key(s3Path));
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Open a new {@link OutputStream} to file for write. Caller needs to close the stream.
   *
   * @param path to file in S3.
   * @return OutputStream for file.
   */
  OutputStream pushStream(String path) throws S3Exception {
    path = sanitizedFilePath(path);

    if (!parentDirectoryExist(path)) {
      throw new S3Exception("Parent directory doesn't exist of path: " + path);
    }

    try {
      return new S3OutputStream(s3Client, path, bucketName);
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /** Override {@link Closeable} since we throw no exception. */
  void close() {
    s3Client.close();
  }

  /** Any file path that specifies a non-existent file will not be treated as an error. */
  private Collection<String> deleteObjects(Collection<String> paths) throws S3Exception {
    try {
      /*
       * Per the S3 docs:
       * https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/DeleteObjectsResult.html
       * An exception is thrown if there's a client error processing the request or in S3 itself.
       * However, there's no guarantee the delete did not happen if an exception is thrown.
       */
      return deleteObjects(paths, MAX_KEYS_PER_BATCH_DELETE);
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Batch deletes from S3.
   *
   * @param entries collection of S3 keys of the files to be deleted.
   * @param batchSize number of deletes to send to S3 at a time
   */
  @VisibleForTesting
  Collection<String> deleteObjects(Collection<String> entries, int batchSize) throws S3Exception {
    List<ObjectIdentifier> keysToDelete =
        entries.stream()
            .map(s -> ObjectIdentifier.builder().key(s).build())
            .sorted(Comparator.comparing(ObjectIdentifier::key).reversed())
            .collect(Collectors.toList());

    List<List<ObjectIdentifier>> partitions = Lists.partition(keysToDelete, batchSize);
    Set<String> deletedPaths = new HashSet<>();

    boolean deleteIndividually = false;
    for (List<ObjectIdentifier> partition : partitions) {
      DeleteObjectsRequest request = createBatchDeleteRequest(partition);

      try {
        DeleteObjectsResponse response = s3Client.deleteObjects(request);

        response.deleted().stream().map(DeletedObject::key).forEach(deletedPaths::add);
      } catch (AwsServiceException ase) {
        if (ase.statusCode() == 501) {
          // This means that the batch-delete is not implemented by this S3 server
          deleteIndividually = true;
          break;
        } else {
          throw handleAmazonException(ase);
        }
      } catch (SdkException sdke) {
        throw handleAmazonException(sdke);
      }
    }

    if (deleteIndividually) {
      for (ObjectIdentifier k : keysToDelete) {
        try {
          s3Client.deleteObject(b -> b.bucket(bucketName).key(k.key()));
          deletedPaths.add(k.key());
        } catch (SdkException sdke) {
          throw new S3Exception("Could not delete object with key: " + k.key(), sdke);
        }
      }
    }

    return deletedPaths;
  }

  private DeleteObjectsRequest createBatchDeleteRequest(List<ObjectIdentifier> keysToDelete) {
    return DeleteObjectsRequest.builder()
        .bucket(bucketName)
        .delete(Delete.builder().objects(keysToDelete).build())
        .build();
  }

  private Set<String> listAll(String path) throws S3Exception {
    String prefix = sanitizedDirPath(path);

    try {
      ListObjectsV2Iterable objectListing =
          s3Client.listObjectsV2Paginator(
              builder -> builder.bucket(bucketName).prefix(prefix).build());

      return objectListing.contents().stream()
          .map(S3Object::key)
          // This filtering is needed only for S3mock. Real S3 does not ignore the trailing
          // '/' in the prefix.
          .filter(s -> s.startsWith(prefix))
          .collect(Collectors.toSet());
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  private boolean parentDirectoryExist(String path) throws S3Exception {
    // Get the last non-slash character of the string, to find the parent directory
    String parentDirectory = getParentDirectory(path);

    // If we have no specific parent directory, we consider parent is root (and always exists)
    if (parentDirectory.isEmpty() || parentDirectory.equals(S3_FILE_PATH_DELIMITER)) {
      return true;
    }

    // Check for existence twice, because s3Mock has issues in the tests
    return pathExists(parentDirectory);
  }

  private String getParentDirectory(String path) {
    if (!path.contains(S3_FILE_PATH_DELIMITER)) {
      return "";
    }

    // Get the last non-slash character of the string, to find the parent directory
    int fromEnd = path.length() - 1;
    if (path.endsWith(S3_FILE_PATH_DELIMITER)) {
      fromEnd -= 1;
    }
    return fromEnd > 0
        ? path.substring(0, path.lastIndexOf(S3_FILE_PATH_DELIMITER, fromEnd) + 1)
        : "";
  }

  /** Ensures path adheres to some rules: -Doesn't start with a leading slash */
  String sanitizedPath(String path) throws S3Exception {
    // Trim space from start and end
    String sanitizedPath = path.trim();

    // Path should start with file delimiter
    if (sanitizedPath.startsWith(S3_FILE_PATH_DELIMITER)) {
      // throw new S3Exception("Invalid Path. Path needs to start with '/'");
      sanitizedPath = sanitizedPath.substring(1).trim();
    }

    return sanitizedPath;
  }

  /**
   * Ensures file path adheres to some rules: -Overall Path rules from `sanitizedPath` -Throw an
   * error if it ends with a trailing slash
   */
  String sanitizedFilePath(String path) throws S3Exception {
    // Trim space from start and end
    String sanitizedPath = sanitizedPath(path);

    if (sanitizedPath.endsWith(S3_FILE_PATH_DELIMITER)) {
      throw new S3Exception("Invalid Path. Path for file can't end with '/'");
    }

    if (sanitizedPath.isEmpty()) {
      throw new S3Exception("Invalid Path. Path cannot be empty");
    }

    return sanitizedPath;
  }

  /**
   * Ensures directory path adheres to some rules: -Overall Path rules from `sanitizedPath` -Add a
   * trailing slash if one does not exist
   */
  String sanitizedDirPath(String path) throws S3Exception {
    // Trim space from start and end
    String sanitizedPath = sanitizedPath(path);

    if (!sanitizedPath.endsWith(S3_FILE_PATH_DELIMITER)) {
      sanitizedPath += S3_FILE_PATH_DELIMITER;
    }

    return sanitizedPath;
  }

  /**
   * Best effort to handle Amazon exceptions as checked exceptions. Amazon exception are all
   * subclasses of {@link RuntimeException} so some may still be uncaught and propagated.
   */
  static S3Exception handleAmazonException(SdkException sdke) {

    if (sdke instanceof AwsServiceException) {
      AwsServiceException ase = (AwsServiceException) sdke;
      String errMessage =
          String.format(
              Locale.ROOT,
              "An AmazonServiceException was thrown! [serviceName=%s] "
                  + "[awsRequestId=%s] [httpStatus=%s] [s3ErrorCode=%s] [message=%s]",
              ase.awsErrorDetails().serviceName(),
              ase.requestId(),
              ase.statusCode(),
              ase.awsErrorDetails().errorCode(),
              ase.awsErrorDetails().errorMessage());

      log.error(errMessage);

      if (sdke instanceof NoSuchKeyException
          || sdke instanceof NoSuchBucketException
          || (ase.statusCode() == 404
              && NOT_FOUND_CODES.contains(ase.awsErrorDetails().errorCode()))) {
        return new S3NotFoundException(errMessage, ase);
      } else {
        return new S3Exception(errMessage, ase);
      }
    }

    return new S3Exception(sdke);
  }
}
