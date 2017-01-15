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

package org.apache.lucene.replicator.nrt;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

// TODO: can we factor/share with IFD: this is doing exactly the same thing, but on the replica side

class ReplicaFileDeleter {
  private final Map<String,Integer> refCounts = new HashMap<String,Integer>();
  private final Directory dir;
  private final Node node;

  public ReplicaFileDeleter(Node node, Directory dir) throws IOException {
    this.dir = dir;
    this.node = node;
  }

  /** Used only by asserts: returns true if the file exists
   *  (can be opened), false if it cannot be opened, and
   *  (unlike Java's File.exists) throws IOException if
   *  there's some unexpected error. */
  private static boolean slowFileExists(Directory dir, String fileName) throws IOException {
    try {
      dir.openInput(fileName, IOContext.DEFAULT).close();
      return true;
    } catch (NoSuchFileException | FileNotFoundException e) {
      return false;
    }
  }

  public synchronized void incRef(Collection<String> fileNames) throws IOException {
    for(String fileName : fileNames) {

      assert slowFileExists(dir, fileName): "file " + fileName + " does not exist!";

      Integer curCount = refCounts.get(fileName);
      if (curCount == null) {
        refCounts.put(fileName, 1);
      } else {
        refCounts.put(fileName, curCount.intValue() + 1);
      }
    }
  }

  public synchronized void decRef(Collection<String> fileNames) throws IOException {
    Set<String> toDelete = new HashSet<>();
    for(String fileName : fileNames) {
      Integer curCount = refCounts.get(fileName);
      assert curCount != null: "fileName=" + fileName;
      assert curCount.intValue() > 0;
      if (curCount.intValue() == 1) {
        refCounts.remove(fileName);
        toDelete.add(fileName);
      } else {
        refCounts.put(fileName, curCount.intValue() - 1);
      }
    }

    delete(toDelete);

    // TODO: this local IR could incRef files here, like we do now with IW's NRT readers ... then we can assert this again:

    // we can't assert this, e.g a search can be running when we switch to a new NRT point, holding a previous IndexReader still open for
    // a bit:
    /*
    // We should never attempt deletion of a still-open file:
    Set<String> delOpen = ((MockDirectoryWrapper) dir).getOpenDeletedFiles();
    if (delOpen.isEmpty() == false) {
      node.message("fail: we tried to delete these still-open files: " + delOpen);
      throw new AssertionError("we tried to delete these still-open files: " + delOpen);
    }
    */
  }

  private synchronized void delete(Collection<String> toDelete) throws IOException {
    if (Node.VERBOSE_FILES) {
      node.message("now delete " + toDelete.size() + " files: " + toDelete);
    }

    // First pass: delete any segments_N files.  We do these first to be certain stale commit points are removed
    // before we remove any files they reference, in case we crash right now:
    for (String fileName : toDelete) {
      assert refCounts.containsKey(fileName) == false;
      if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
        delete(fileName);
      }
    }

    // Only delete other files if we were able to remove the segments_N files; this way we never
    // leave a corrupt commit in the index even in the presense of virus checkers:
    for(String fileName : toDelete) {
      assert refCounts.containsKey(fileName) == false;
      if (fileName.startsWith(IndexFileNames.SEGMENTS) == false) {
        delete(fileName);
      }
    }

  }

  private synchronized void delete(String fileName) throws IOException {
    if (Node.VERBOSE_FILES) {
      node.message("file " + fileName + ": now delete");
    }
    dir.deleteFile(fileName);
  }

  public synchronized Integer getRefCount(String fileName) {
    return refCounts.get(fileName);
  }

  public synchronized void deleteIfNoRef(String fileName) throws IOException {
    if (refCounts.containsKey(fileName) == false) {
      deleteNewFile(fileName);
    }
  }

  public synchronized void deleteNewFile(String fileName) throws IOException {
    delete(fileName);
  }

  /*
  public synchronized Set<String> getPending() {
    return new HashSet<String>(pending);
  }
  */

  public synchronized void deleteUnknownFiles(String segmentsFileName) throws IOException {
    Set<String> toDelete = new HashSet<>();
    for(String fileName : dir.listAll()) {
      if (refCounts.containsKey(fileName) == false &&
          fileName.equals("write.lock") == false &&
          fileName.equals(segmentsFileName) == false) {
        node.message("will delete unknown file \"" + fileName + "\"");
        toDelete.add(fileName);
      }
    }

    delete(toDelete);
  }
}
