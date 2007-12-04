package org.apache.lucene.index;

/**
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

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/** A {@link DeletionPolicy} that wraps around any other
 *  {@link DeletionPolicy} and adds the ability to hold and
 *  later release a single "snapshot" of an index.  While
 *  the snapshot is held, the {@link IndexWriter} will not
 *  remove any files associated with it even if the index is
 *  otherwise being actively, arbitrarily changed.  Because
 *  we wrap another arbitrary {@link DeletionPolicy}, this
 *  gives you the freedom to continue using whatever {@link
 *  DeletionPolicy} you would normally want to use with your
 *  index. */

public class SnapshotDeletionPolicy implements IndexDeletionPolicy {

  private IndexCommitPoint lastCommit;
  private IndexDeletionPolicy primary;
  private IndexCommitPoint snapshot;

  public SnapshotDeletionPolicy(IndexDeletionPolicy primary) {
    this.primary = primary;
  }

  public synchronized void onInit(List commits) throws IOException {
    primary.onInit(wrapCommits(commits));
    lastCommit = (IndexCommitPoint) commits.get(commits.size()-1);
  }

  public synchronized void onCommit(List commits) throws IOException {
    primary.onCommit(wrapCommits(commits));
    lastCommit = (IndexCommitPoint) commits.get(commits.size()-1);
  }

  /** Take a snapshot of the most recent commit to the
   *  index.  You must call release() to free this snapshot.
   *  Note that while the snapshot is held, the files it
   *  references will not be deleted, which will consume
   *  additional disk space in your index. If you take a
   *  snapshot at a particularly bad time (say just before
   *  you call optimize()) then in the worst case this could
   *  consume an extra 1X of your total index size, until
   *  you release the snapshot. */
  public synchronized IndexCommitPoint snapshot() {
    if (snapshot == null)
      snapshot = lastCommit;
    else
      throw new IllegalStateException("snapshot is already set; please call release() first");
    return snapshot;
  }

  /** Release the currently held snapshot. */
  public synchronized void release() {
    if (snapshot != null)
      snapshot = null;
    else
      throw new IllegalStateException("snapshot was not set; please call snapshot() first");
  }

  private class MyCommitPoint implements IndexCommitPoint {
    IndexCommitPoint cp;
    MyCommitPoint(IndexCommitPoint cp) {
      this.cp = cp;
    }
    public String getSegmentsFileName() {
      return cp.getSegmentsFileName();
    }
    public Collection getFileNames() throws IOException {
      return cp.getFileNames();
    }
    public void delete() {
      synchronized(SnapshotDeletionPolicy.this) {
        // Suppress the delete request if this commit point is
        // our current snapshot.
        if (snapshot != cp)
          cp.delete();
      }
    }
  }

  private List wrapCommits(List commits) {
    final int count = commits.size();
    List myCommits = new ArrayList(count);
    for(int i=0;i<count;i++)
      myCommits.add(new MyCommitPoint((IndexCommitPoint) commits.get(i)));
    return myCommits;
  }
}
