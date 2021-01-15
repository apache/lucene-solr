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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.common.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobDirectory extends FilterDirectory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final IOContext SYNC_IO_CONTEXT = new IOContext();

  private final String blobDirPath;
  private final BlobPusher blobPusher;
  /**
   * Map of {@link BlobFileSupplier} for each file created by this directory. Keys are file names.
   * Each {@link BlobFileSupplier} keeps a reference to the {@link IndexOutput} created for the
   * file, to provide the checksums on {@link #sync(Collection)}. But it is able to free earlier the
   * reference each time an {@link IndexOutput} is closed, by getting the checksum at that time.
   */
  private final Map<String, BlobFileSupplier> blobFileSupplierMap;
  private final Set<String> synchronizedFileNames;
  private final Collection<String> deletedFileNames;
  private volatile boolean isOpen;

  public BlobDirectory(Directory delegate, String blobDirPath, BlobPusher blobPusher) {
    super(delegate);
    this.blobDirPath = blobDirPath;
    this.blobPusher = blobPusher;
    blobFileSupplierMap = new HashMap<>();
    synchronizedFileNames = new HashSet<>();
    deletedFileNames = new ArrayList<>();
  }

  @Override
  public void deleteFile(String name) throws IOException {
    log.debug("deleteFile {}", name);
    in.deleteFile(name);
    deletedFileNames.add(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    log.debug("createOutput {}", name);
    IndexOutput indexOutput = in.createOutput(name, context);
    BlobFileSupplier blobFileSupplier = new BlobFileSupplier(indexOutput);
    blobFileSupplierMap.put(name, blobFileSupplier);
    return new BlobIndexOutput(indexOutput, blobFileSupplier);
  }

  // createTempOutput(): We don't track tmp files since they are not synced.

  @Override
  public void sync(Collection<String> names) throws IOException {
    log.debug("sync {}", names);
    in.sync(names);
    synchronizedFileNames.addAll(names);
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    log.debug("rename {} to {}", source, dest);
    in.rename(source, dest);
    // Also rename the corresponding BlobFile.
    BlobFileSupplier blobFileSupplier = blobFileSupplierMap.remove(source);
    if (blobFileSupplier != null) {
      blobFileSupplier.rename(source, dest);
      blobFileSupplierMap.put(dest, blobFileSupplier);
    }
    // Also rename the tracked synchronized file.
    if (synchronizedFileNames.remove(source)) {
      synchronizedFileNames.add(dest);
    }
  }

  @Override
  public void syncMetaData() throws IOException {
    log.debug("syncMetaData");
    in.syncMetaData();
    syncToBlobStore();
  }

  private void syncToBlobStore() throws IOException {
    log.debug("File names to sync {}", synchronizedFileNames);

    Collection<BlobFile> writes = new ArrayList<>(synchronizedFileNames.size());
    for (String fileName : synchronizedFileNames) {
      BlobFileSupplier blobFileSupplier = blobFileSupplierMap.get(fileName);
      if (blobFileSupplier != null) {
        // Only sync files that were synced since this directory was released. Previous files don't
        // need to be synced.
        writes.add(blobFileSupplier.getBlobFile());
      }
    }

    log.debug("Sync to BlobStore writes={} deleted={}", writes, deletedFileNames);
    blobPusher.push(blobDirPath, writes, this::openInputStream, deletedFileNames);
    synchronizedFileNames.clear();
    deletedFileNames.clear();
  }

  private InputStream openInputStream(BlobFile blobFile) throws IOException {
    return new IndexInputInputStream(in.openInput(blobFile.fileName(), SYNC_IO_CONTEXT));
  }

  public void release() {
    log.debug("release");
    blobFileSupplierMap.clear();
    synchronizedFileNames.clear();
    deletedFileNames.clear();
  }

  // obtainLock(): We get the delegate Directory lock.

  @Override
  public void close() {
    log.debug("close");
    isOpen = false;
    IOUtils.closeQuietly(in);
    IOUtils.closeQuietly(blobPusher);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + in.toString() + ")";
  }

  @Override
  protected void ensureOpen() throws AlreadyClosedException {
    if (!isOpen) {
      throw new AlreadyClosedException("This Directory is closed");
    }
  }

  /**
   * Delegating {@link IndexOutput} that hooks the {@link #close()} method to compute the checksum.
   * The goal is to free the reference to the delegate {@link IndexOutput} when it is closed because
   * we only need it to get the checksum.
   */
  private static class BlobIndexOutput extends FilterIndexOutput {

    private final BlobFileSupplier blobFileSupplier;

    BlobIndexOutput(IndexOutput delegate, BlobFileSupplier blobFileSupplier) {
      super("Blob " + delegate.toString(), delegate.getName(), delegate);
      this.blobFileSupplier = blobFileSupplier;
    }

    @Override
    public void close() throws IOException {
      blobFileSupplier.getBlobFileFromIndexOutput();
      super.close();
    }
  }

  /**
   * Supplies the length and checksum of a file created in this directory. Keeps a reference to the
   * file {@link IndexOutput} to be able to get its final length and checksum. However we try to
   * free the reference as soon as we can (when the {@link IndexOutput} is closed so we know the
   * content is final).
   */
  private static class BlobFileSupplier {

    IndexOutput indexOutput;
    String name;
    BlobFile blobFile;

    BlobFileSupplier(IndexOutput indexOutput) {
      this.indexOutput = indexOutput;
      name = indexOutput.getName();
    }

    void rename(String source, String dest) {
      assert name.equals(source);
      name = dest;
      if (blobFile != null) {
        blobFile = new BlobFile(name, blobFile.size(), blobFile.checksum());
      }
    }

    BlobFile getBlobFile() throws IOException {
      if (blobFile == null) {
        getBlobFileFromIndexOutput();
      }
      return blobFile;
    }

    /**
     * Gets the {@link BlobFile} of the referenced {@link IndexOutput} and then frees the reference.
     */
    void getBlobFileFromIndexOutput() throws IOException {
      blobFile = new BlobFile(name, indexOutput.getFilePointer(), indexOutput.getChecksum());
      // log.debug("Freeing IndexOutput {}", indexOutput);
      indexOutput = null; // Free the reference since we have the checksum.
    }
  }
}
