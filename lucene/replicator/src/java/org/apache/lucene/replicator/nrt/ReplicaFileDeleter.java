package org.apache.lucene.replicator.nrt;

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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

// TODO: can we factor/share with IFD: this is doing exactly the same thing, but on the replica side

// TODO: once LUCENE-6835 is in, this class becomes a lot simpler?

class ReplicaFileDeleter {
  private final Map<String,Integer> refCounts = new HashMap<String,Integer>();
  private final Set<String> pending = new HashSet<String>();
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
  static boolean slowFileExists(Directory dir, String fileName) throws IOException {
    try {
      dir.openInput(fileName, IOContext.DEFAULT).close();
      return true;
    } catch (NoSuchFileException | FileNotFoundException e) {
      return false;
    }
  }

  public synchronized void incRef(Collection<String> fileNames) throws IOException {
    for(String fileName : fileNames) {

      if (pending.contains(fileName)) {
        throw new IllegalStateException("cannot incRef file \"" + fileName + "\": it is pending delete");
      }

      assert slowFileExists(dir, fileName): "file " + fileName + " does not exist!";

      Integer curCount = refCounts.get(fileName);
      if (curCount == null) {
        refCounts.put(fileName, 1);
      } else {
        refCounts.put(fileName, curCount.intValue() + 1);
      }
    }
  }

  public synchronized void decRef(Collection<String> fileNames) {
    // We don't delete the files immediately when their RC drops to 0; instead, we add to the pending set, and then call deletePending in
    // the end:
    for(String fileName : fileNames) {
      Integer curCount = refCounts.get(fileName);
      assert curCount != null: "fileName=" + fileName;
      assert curCount.intValue() > 0;
      if (curCount.intValue() == 1) {
        refCounts.remove(fileName);
        pending.add(fileName);
      } else {
        refCounts.put(fileName, curCount.intValue() - 1);
      }
    }

    deletePending();

    // TODO: this local IR could incRef files here, like we do now with IW ... then we can assert this again:

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

  private synchronized boolean delete(String fileName) {
    try {
      if (Node.VERBOSE_FILES) {
        node.message("file " + fileName + ": now delete");
      }
      dir.deleteFile(fileName);
      pending.remove(fileName);
      return true;
    } catch (FileNotFoundException|NoSuchFileException missing) {
      // This should never happen: we should only be asked to track files that do exist
      node.message("file " + fileName + ": delete failed: " + missing);
      throw new IllegalStateException("file " + fileName + ": we attempted delete but the file does not exist?", missing);
    } catch (IOException ioe) {
      // nocommit remove this retry logic!  it's Directory's job now...
      if (Node.VERBOSE_FILES) {
        node.message("file " + fileName + ": delete failed: " + ioe + "; will retry later");
      }
      pending.add(fileName);
      return false;
    }
  }

  public synchronized Integer getRefCount(String fileName) {
    return refCounts.get(fileName);
  }

  public synchronized boolean isPending(String fileName) {
    return pending.contains(fileName);
  }

  public synchronized void deletePending() {
    if (Node.VERBOSE_FILES) {
      node.message("now deletePending: " + pending.size() + " files to try: " + pending);
    }

    // Clone the set because it will change as we iterate:
    List<String> toDelete = new ArrayList<>(pending);

    // First pass: delete any segments_N files.  We do these first to be certain stale commit points are removed
    // before we remove any files they reference.  If any delete of segments_N fails, we leave all other files
    // undeleted so index is never in a corrupt state:
    for (String fileName : toDelete) {
      Integer rc = refCounts.get(fileName);
      if (rc != null && rc > 0) {
        // Should never happen!  This means we are about to pending-delete a referenced index file
        throw new IllegalStateException("file \"" + fileName + "\" is in pending delete set but has non-zero refCount=" + rc);
      } else if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
        if (delete(fileName) == false) {
          if (Node.VERBOSE_FILES) {
            node.message("failed to remove commit point \"" + fileName + "\"; skipping deletion of all other pending files");
          }
          return;
        }
      }
    }

    // Only delete other files if we were able to remove the segments_N files; this way we never
    // leave a corrupt commit in the index even in the presense of virus checkers:
    for(String fileName : toDelete) {
      if (fileName.startsWith(IndexFileNames.SEGMENTS) == false) {
        delete(fileName);
      }
    }

    Set<String> copy = new HashSet<String>(pending);
    pending.clear();
    for(String fileName : copy) {
      delete(fileName);
    }
  }

  /** Necessary in case we had tried to delete this fileName before, it failed, but then it was later overwritten (because primary changed
   *  and new primary didn't know this segment name had been previously attempted) and now has > 0 refCount */
  public synchronized void clearPending(Collection<String> fileNames) {
    for(String fileName : fileNames) {
      if (pending.remove(fileName)) {
        node.message("file " + fileName + ": deleter.clearPending now clear from pending");
      }
    }
  }

  public synchronized void deleteIfNoRef(String fileName) {
    if (refCounts.containsKey(fileName) == false) {
      deleteNewFile(fileName);
    }
  }

  public synchronized void deleteNewFile(String fileName) {
    delete(fileName);
  }

  public synchronized Set<String> getPending() {
    return new HashSet<String>(pending);
  }

  public synchronized void deleteUnknownFiles(String segmentsFileName) throws IOException {
    for(String fileName : dir.listAll()) {
      if (refCounts.containsKey(fileName) == false &&
          fileName.equals("write.lock") == false &&
          fileName.equals(segmentsFileName) == false) {
        node.message("will delete unknown file \"" + fileName + "\"");
        pending.add(fileName);
      }
    }

    deletePending();
  }
}
