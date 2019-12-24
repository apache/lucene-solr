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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.util.IOUtils;

/**
 * Class that handles reads and writes of solr blob files to the local file system.
 */
public class LocalStorageClient implements CoreStorageClient {
  
  /** The directory on the local file system where blobs will be stored. */
  public static final String BLOB_STORE_LOCAL_FS_ROOT_DIR_PROPERTY = "blob.local.dir";
  
  private final String blobStoreRootDir = System.getProperty(BLOB_STORE_LOCAL_FS_ROOT_DIR_PROPERTY, "/tmp/BlobStoreLocal/");

  public LocalStorageClient() throws IOException {
    File rootDir = new File(blobStoreRootDir);
    rootDir.mkdirs(); // Might create the directory... or not
    if (!rootDir.isDirectory()) {
      throw new IOException("Can't create local Blob root directory " + rootDir.getAbsolutePath());
    }
  }

  private File getCoreRootDir(String blobName) {
    return new File(BlobClientUtils.concatenatePaths(blobStoreRootDir, blobName));
  }

  @Override
  public String pushStream(String blobName, InputStream is, long contentLength, String fileNamePrefix) throws BlobException {
    try {
      createCoreStorage(blobName);
      String blobPath = createNewNonExistingBlob(blobName, fileNamePrefix);

      Files.copy(is, Paths.get(getBlobAbsolutePath(blobPath)), StandardCopyOption.REPLACE_EXISTING);

      assert new File(getBlobAbsolutePath(blobPath)).length() == contentLength;

      return blobPath;
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  /**
   * Picks a unique name for a new blob for the given core.<p>
   * The current implementation creates a file, but eventually we just pick up a random blob name then delegate to S3...
   * @return the blob file name, including the "path" part of the name
   */
  private String createNewNonExistingBlob(String blobName, String fileNamePrefix) throws BlobException {
    try {
      String blobPath = BlobClientUtils.generateNewBlobCorePath(blobName, fileNamePrefix);
      final File blobFile = new File(getBlobAbsolutePath(blobPath));
      if (blobFile.exists()) {
        // Not expecting this ever to happen. In theory we could just do "continue" here to try a new
        // name. For now throwing an exception to make sure we don't run into this...
        // continue;
        throw new IllegalStateException("The random file name chosen using UUID already exists. Very worrying! " + blobFile.getAbsolutePath());
      }

      return blobPath;
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public InputStream pullStream(String blobPath) throws BlobException {
    try {
      File blobFile = new File(getBlobAbsolutePath(blobPath));
      return new FileInputStream(blobFile);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public void pushCoreMetadata(String sharedStoreName, String blobCoreMetadataName, BlobCoreMetadata bcm) throws BlobException {
    try {
      createCoreStorage(sharedStoreName);
      ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
      String json = converter.toJson(bcm);

      // Constant path under which the core metadata is stored in the Blob store (the only blob stored under a constant path!)
      String blobMetadataPath = getBlobAbsolutePath(getBlobMetadataName(sharedStoreName, blobCoreMetadataName));
      final File blobMetadataFile = new File(blobMetadataPath); 

      // Writing to the file assumed atomic, the file cannot be observed midway. Might not hold here but should be the case
      // with a real S3 implementation.
      try (PrintWriter out = new PrintWriter(blobMetadataFile, StandardCharsets.UTF_8.name())){
        out.println(json);
      }  
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public BlobCoreMetadata pullCoreMetadata(String sharedStoreName, String blobCoreMetadataName) throws BlobException {
    try {
      if (!coreMetadataExists(sharedStoreName, blobCoreMetadataName)) {
        return null;
      }
      
      String blobMetadataPath = getBlobAbsolutePath(getBlobMetadataName(sharedStoreName, blobCoreMetadataName));
      File blobMetadataFile = new File(blobMetadataPath); 
      
      String json = new String(Files.readAllBytes(blobMetadataFile.toPath()), StandardCharsets.UTF_8);
      ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
      return converter.fromJson(json, BlobCoreMetadata.class);
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  @Override
  public boolean coreMetadataExists(String sharedStoreName, String blobCoreMetadataName) throws BlobException { 
    try {
      String blobMetadataPath = getBlobAbsolutePath(getBlobMetadataName(sharedStoreName, blobCoreMetadataName));
      File coreMetadataFile = new File(blobMetadataPath); 
      return coreMetadataFile.exists();
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }

  /**
   * Prefixes the given path with the blob store root directory on the local FS 
   */
  private String getBlobAbsolutePath(String blobPath) {
    return BlobClientUtils.concatenatePaths(blobStoreRootDir, blobPath);
  }
    
  private void createCoreStorage(String blobName) throws Exception {
    File coreRootDir = getCoreRootDir(blobName);
    coreRootDir.mkdirs();
    if (!coreRootDir.isDirectory()) {
      throw new IOException("Can't create Blob core root directory " + coreRootDir.getAbsolutePath());
    }
  }
    
  @Override
  public void deleteCore(String blobName) throws BlobException {
    try {
      Path path = Paths.get(getBlobAbsolutePath(blobName));
      Files.walk(path)
        // Since this traversal includes the root directory
        // we need to reverse because we can't delete non-empty directories 
        .sorted(Comparator.reverseOrder())
        .forEach(filePath -> {
          IOUtils.deleteFilesIgnoringExceptions(filePath);
        });
    } catch (Exception ex) {
      // In case the path doesn't exist, we'll just swallow the exception because it's not an issue,
      // especially in test clean up.
    }
  }
    
  /**
   * On the local FS implementation we'll just delete blob files individually
   */
  @Override
  public void deleteBlobs(Collection<String> paths) throws BlobException {
    try {
      for (String blobPath : paths) {
        final Path path = Paths.get(getBlobAbsolutePath(blobPath));
        Files.deleteIfExists(path);
      }
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }
  
  private String getBlobMetadataName(String blobName, String blobCoreMetadataName) {
    return BlobClientUtils.concatenatePaths(blobName, blobCoreMetadataName);
  }
    
  @Override
  public BlobstoreProviderType getStorageProvider() {
    return BlobstoreProviderType.LOCAL_FILE_SYSTEM;
  }
  
  @Override 
  public String getBucketRegion() {
    return "N/A";
  }
    
  @Override 
  public String getBucketName() {
    return "N/A";
  }
  
  @Override 
  public String getEndpoint() {
    return "N/A";
  }

  /**
   * The local file system is being used, there's nothing to connect to so this method will always return true.
   */
  @Override
  public boolean doesBucketExist() {
    return true;
  }

  @Override
  public void shutdown() {
      
  }
    
  @Override
  public List<String> listCoreBlobFilesOlderThan(String blobName, long timestamp) throws BlobException {
    try {
      Path path = Paths.get(getBlobAbsolutePath(blobName));
      List<String> blobFiles =
        Files.walk(path).map(Path::toFile)
        // We need to ignore the root directory as a path since this traversal includes it
        .filter(file -> (file.lastModified() < timestamp) && !file.isDirectory())
        .map(file -> BlobClientUtils.concatenatePaths(blobName, file.getName()))
        .collect(Collectors.toList());
      return blobFiles;
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }
    
  @Override
  public List<String> listCommonBlobPrefix(String prefix) throws BlobException {
    try {
      String rootBlobDir = getBlobAbsolutePath("");
      Path path = Paths.get(rootBlobDir);
      List<String> blobFiles =
        Files.walk(path).map(Path::toFile)
          .filter(file -> (!file.isDirectory()))
          .map(file -> {
            // extracts just the file system blob file name without the root dir
            String fileBlobKey = file.getAbsolutePath().substring(rootBlobDir.length());
            // extract the common prefix up to the delimiter
            return fileBlobKey.substring(0, fileBlobKey.indexOf(BlobClientUtils.BLOB_FILE_PATH_DELIMITER));
          })
        .distinct()
        .collect(Collectors.toList());
      return blobFiles;
    } catch (Exception ex) {
      throw new BlobException(ex);
    }
  }
}
