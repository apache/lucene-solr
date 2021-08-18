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

package org.apache.solr.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class LocalStorageGCSBackupRepository extends GCSBackupRepository {

  protected static Storage stashedStorage = null;

  @Override
  public Storage initStorage() {
    // LocalStorageHelper produces 'Storage' objects that track blob store state in non-static memory unique to each
    // Storage instance.  For various components in Solr to have a coherent view of the blob-space then, they need to
    // share a single 'Storage' object.
    synchronized (LocalStorageGCSBackupRepository.class) {
      storage = getSingletonStorage();
      initializeBackupLocation();
    }
    return storage;
  }

  public static void clearStashedStorage() {
    synchronized (LocalStorageGCSBackupRepository.class) {
      stashedStorage = null;
    }
  }

  // A reimplementation of delete functionality that avoids batching.  Batching is ideal in production use cases, but
  // isn't supported by the in-memory Storage implementation provided by LocalStorageHelper
  @Override
  public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
    final List<Boolean> results = Lists.newArrayList();

    final List<String> filesOrdered = files.stream().collect(Collectors.toList());
    for (String file : filesOrdered) {
      final String prefix = path.toString().endsWith("/") ? path.toString() : path.toString() + "/";
      results.add(storage.delete(BlobId.of(bucketName, prefix + file)));
    }

    if (!ignoreNoSuchFileException) {
      int failedDelete = results.indexOf(Boolean.FALSE);
      if (failedDelete != -1) {
        throw new NoSuchFileException("File " + filesOrdered.get(failedDelete) + " was not found");
      }
    }
  }

  @Override
  public void deleteDirectory(URI path) throws IOException {
    List<BlobId> blobIds = allBlobsAtDir(path);
    if (!blobIds.isEmpty()) {
      for (BlobId blob : blobIds) {
        storage.delete(blob);
      }
    }
  }

  protected Storage getSingletonStorage() {
    if (stashedStorage != null) {
      return stashedStorage;
    }

    // FakeStorageRpc isn't thread-safe, which causes flaky test failures when multiple cores attempt to backup files
    // simultaneously.  We work around this here by wrapping it in a delegating instance that adds a measure of thread safety.
    stashedStorage = new ConcurrentDelegatingStorage(LocalStorageHelper.customOptions(false).getService());
    return stashedStorage;
  }

  protected void initializeBackupLocation() {
    try {
      final String baseLocation = getBackupLocation(null);
      final URI baseLocationUri = createDirectoryURI(baseLocation);
      createDirectory(baseLocationUri);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
