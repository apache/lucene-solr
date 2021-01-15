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

package org.apache.solr.blob;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pushes a set of files to Blob, and works with listings. */
public class BlobPusher implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // WORK IN PROGRESS!!

  private final BlobStore blobStore;
  private final ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("blobPusher");

  public BlobPusher(BlobStore blobStore) {
    this.blobStore = blobStore;
  }

  public void push(
          String blobDirPath,
      Collection<BlobFile> writes,
      IOUtils.IOFunction<BlobFile, InputStream> inputStreamSupplier,
      Collection<String> deletes)
      throws IOException {

    // update "foreign" listings
    //      TODO David

    // send files to BlobStore and delete our files too
    log.debug("Pushing {}", writes);
    executeAll(pushFiles(blobDirPath, writes, inputStreamSupplier));
    log.debug("Deleting {}", deletes);
    deleteFiles(blobDirPath, deletes);

    // update "our" listing
    //      TODO David
  }

  private void executeAll(List<Callable<Void>> actions) throws IOException {
    try {
      for (Future<Void> future : executor.invokeAll(actions)) {
        future.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  private List<Callable<Void>> pushFiles(
          String blobDirPath,
      Collection<BlobFile> blobFiles,
      IOUtils.IOFunction<BlobFile, InputStream> inputStreamSupplier) {
    return blobFiles.stream()
        .map(
            (blobFile) ->
                (Callable<Void>)
                    () -> {
                      try (InputStream in = inputStreamSupplier.apply(blobFile)) {
                        blobStore.create(blobDirPath, blobFile.fileName(), in, blobFile.size());
                      }
                      return null;
                    })
        .collect(Collectors.toList());
  }

  private void deleteFiles(String blobDirPath, Collection<String> fileNames) throws IOException {
    blobStore.delete(blobDirPath, fileNames);
  }

  @Override
  public void close() {
    // TODO
  }
}
