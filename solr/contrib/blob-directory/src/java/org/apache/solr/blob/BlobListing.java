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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A listing of files with references to files in other listings. Lucene/Solr agnostic. */
public class BlobListing {

  public static final class LocalEntry {
    private final BlobFile blobFile;
    /**
     * Set of relative paths to other listings that refer to this file. The current listing is "."
     * and is always first if present.
     */
    private final List<String> references;

    public LocalEntry(BlobFile blobFile, List<String> references) {
      this.blobFile = blobFile;
      this.references = references;
    }

    public BlobFile getBlobFile() {
      return blobFile;
    }

    public boolean localDeleted() {
      return references.get(0).equals(".");
    }

    public List<String> references() {
      return references;
    }

    @Override
    public String toString() {
      return "LocalEntry{" + "blobFile=" + blobFile + ", references=" + references + '}';
    }

    public LocalEntry copyWithRef(String path) {
      final int idx = Collections.binarySearch(references, path);
      assert idx < 0;
      final int insertionPoint = -idx - 1;
      ArrayList<String> newList = new ArrayList<>(references.size() + 1);
      newList.add(insertionPoint, path);
      return new LocalEntry(blobFile, newList);
    }
  }

  public static final class RefEntry {
    private final String fileName;
    private final String sourcePath;

    public RefEntry(String fileName, String sourcePath) {
      this.fileName = fileName;
      this.sourcePath = sourcePath;
    }

    public String fileName() {
      return fileName;
    }

    public String sourcePath() {
      return sourcePath;
    }

    @Override
    public String toString() {
      return "RefEntry{"
          + "fileName='"
          + fileName
          + '\''
          + ", sourcePath='"
          + sourcePath
          + '\''
          + '}';
    }
  }

  public static BlobListing fromJson(byte[] bytes) {
    throw new UnsupportedOperationException("TODO"); // TODO
  }

  public byte[] toJson() {
    throw new UnsupportedOperationException("TODO"); // TODO
  }

  private final Map<BlobFile, LocalEntry> localFiles;
  private final Map<String, RefEntry> refFiles;

  public BlobListing(Map<BlobFile, LocalEntry> localFiles, Map<String, RefEntry> refFiles) {
    this.localFiles = localFiles;
    this.refFiles = refFiles;

    final Set<String> refFileSet = refFiles.keySet();
    assert localFiles.keySet().stream().noneMatch(bf -> refFileSet.contains(bf.fileName()));
    // TODO assert sorted
  }

  public LocalEntry lookupLocalEntry(BlobFile blobFile) {
    return localFiles.get(blobFile);
  }

  public BlobFile lookupLocalBlobFile(String fileName) {
    for (BlobFile blobFile : localFiles.keySet()) {
      if (blobFile.fileName().equals(fileName)) {
        return blobFile;
      }
    }
    return null;
  }

  public RefEntry lookupRemoteEntry(String fileName) {
    return refFiles.get(fileName);
  }
}
