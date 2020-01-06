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

import java.util.*;

/**
 * Builder for {@link BlobCoreMetadata}.
 */
public class BlobCoreMetadataBuilder {
  /**
   * Generation number in metadata of cores not existing on the Blob Store.
   */
  public static final long UNDEFINED_VALUE = -1L;

  final private String sharedBlobName;
  final private long generation;
  final private Set<BlobCoreMetadata.BlobFile> blobFiles;
  final private Set<BlobCoreMetadata.BlobFileToDelete> blobFilesToDelete;

  public BlobCoreMetadataBuilder(String sharedBlobName, long generation) {
    this.sharedBlobName = sharedBlobName;
    this.generation= generation;
    this.blobFiles = new HashSet<>();
    this.blobFilesToDelete = new HashSet<>();
  }

  /**
   * Builder used for "cloning" then modifying an existing instance of {@link BlobCoreMetadata}.
   * The new generation has to be passed in because it is final and can't be set later.
   */
  public BlobCoreMetadataBuilder(BlobCoreMetadata bcm, long generation) {
    this.sharedBlobName = bcm.getSharedBlobName();
    this.generation = generation;
    this.blobFiles = new HashSet<>(Arrays.asList(bcm.getBlobFiles()));
    this.blobFilesToDelete = new HashSet<>(Arrays.asList(bcm.getBlobFilesToDelete()));
  }

  public String getSharedBlobName() {
    return this.sharedBlobName;
  }

  /**
   * Builds a {@link BlobCoreMetadata} for a non existing core of a given name.
   */
  static public BlobCoreMetadata buildEmptyCoreMetadata(String sharedBlobName) {
    return (new BlobCoreMetadataBuilder(sharedBlobName, UNDEFINED_VALUE)).build();
  }

  /**
   * Adds a file to the set of "active" files listed in the metadata
   */
  public BlobCoreMetadataBuilder addFile(BlobCoreMetadata.BlobFile f) {
    this.blobFiles.add(f);
    return this;
  }

  /**
   * Removes a file from the set of "active" files listed in the metadata
   */
  public BlobCoreMetadataBuilder removeFile(BlobCoreMetadata.BlobFile f) {
    boolean removed = this.blobFiles.remove(f);
    assert removed; // If we remove things that are not there, likely a bug in our code
    return this;
  }
    
  /**
   * Adds a file to the set of files to delete listed in the metadata<p>
   * This method should always be called with {@link #removeFile(BlobCoreMetadata.BlobFile)} above. Possibly it's
   * better to only have a single method doing both operations (TODO).
   */
  public BlobCoreMetadataBuilder addFileToDelete(BlobCoreMetadata.BlobFileToDelete f) {
    this.blobFilesToDelete.add(f);
    return this;
  }
    
  /**
   * Returns an iterator on the set of files to delete.
   * The returned iterator will be used to remove files from the set (as they are enqueued for hard delete from the Blob store).
   */
  public Iterator<BlobCoreMetadata.BlobFileToDelete> getDeletedFilesIterator() {
    return this.blobFilesToDelete.iterator();
  }
    
  /**
   * Removes a file from the set of "deleted" files listed in the metadata
   */
  public BlobCoreMetadataBuilder removeFilesFromDeleted(Set<BlobCoreMetadata.BlobFileToDelete> files) {
    int originalSize = this.blobFilesToDelete.size();
    boolean removed = this.blobFilesToDelete.removeAll(files);
    int totalRemoved = originalSize - this.blobFilesToDelete.size();
    
    // If we remove things that are not there, likely a bug in our code
    assert removed && (totalRemoved == files.size()); 
    return this;
  }

  public BlobCoreMetadata build() {
    // TODO make this fail if we find more than one segments_N files (or if "N" does not match generation).
    BlobCoreMetadata.BlobFile[] blobFilesArray = this.blobFiles.toArray(new BlobCoreMetadata.BlobFile[this.blobFiles.size()]);
    BlobCoreMetadata.BlobFileToDelete[] blobFilesToDeleteArray = this.blobFilesToDelete.toArray(new BlobCoreMetadata.BlobFileToDelete[this.blobFilesToDelete.size()]);

    return new BlobCoreMetadata(this.sharedBlobName, blobFilesArray, blobFilesToDeleteArray, generation);
  }
}
