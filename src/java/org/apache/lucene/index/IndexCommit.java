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
import java.io.IOException;
import org.apache.lucene.store.Directory;

/**
 * <p>Expert: represents a single commit into an index as seen by the
 * {@link IndexDeletionPolicy} or {@link IndexReader}.</p>
 *
 * <p> Changes to the content of an index are made visible
 * only after the writer who made that change commits by
 * writing a new segments file
 * (<code>segments_N</code>). This point in time, when the
 * action of writing of a new segments file to the directory
 * is completed, is an index commit.</p>
 *
 * <p>Each index commit point has a unique segments file
 * associated with it. The segments file associated with a
 * later index commit point would have a larger N.</p>
 *
 * <p><b>WARNING</b>: This API is a new and experimental and
 * may suddenly change. </p>
*/

public abstract class IndexCommit implements IndexCommitPoint {

  /**
   * Get the segments file (<code>segments_N</code>) associated 
   * with this commit point.
   */
  public abstract String getSegmentsFileName();

  /**
   * Returns all index files referenced by this commit point.
   */
  public abstract Collection getFileNames() throws IOException;

  /**
   * Returns the {@link Directory} for the index.
   */
  public abstract Directory getDirectory();
  
  /**
   * Delete this commit point.  This only applies when using
   * the commit point in the context of IndexWriter's
   * IndexDeletionPolicy.
   * <p>
   * Upon calling this, the writer is notified that this commit 
   * point should be deleted. 
   * <p>
   * Decision that a commit-point should be deleted is taken by the {@link IndexDeletionPolicy} in effect
   * and therefore this should only be called by its {@link IndexDeletionPolicy#onInit onInit()} or 
   * {@link IndexDeletionPolicy#onCommit onCommit()} methods.
  */
  public void delete() {
    throw new UnsupportedOperationException("This IndexCommit does not support this method.");
  }

  public boolean isDeleted() {
    throw new UnsupportedOperationException("This IndexCommit does not support this method.");
  }

  /**
   * Returns true if this commit is an optimized index.
   */
  public boolean isOptimized() {
    throw new UnsupportedOperationException("This IndexCommit does not support this method.");
  }

  /**
   * Two IndexCommits are equal if both their Directory and versions are equal.
   */
  public boolean equals(Object other) {
    if (other instanceof IndexCommit) {
      IndexCommit otherCommit = (IndexCommit) other;
      return otherCommit.getDirectory().equals(getDirectory()) && otherCommit.getVersion() == getVersion();
    } else
      return false;
  }

  public int hashCode() {
    return getDirectory().hashCode() + getSegmentsFileName().hashCode();
  }

  /** Returns the version for this IndexCommit.  This is the
      same value that {@link IndexReader#getVersion} would
      return if it were opened on this commit. */
  public long getVersion() {
    throw new UnsupportedOperationException("This IndexCommit does not support this method.");
  }

  /** Returns the generation (the _N in segments_N) for this
      IndexCommit */
  public long getGeneration() {
    throw new UnsupportedOperationException("This IndexCommit does not support this method.");
  }

  /** Convenience method that returns the last modified time
   *  of the segments_N file corresponding to this index
   *  commit, equivalent to
   *  getDirectory().fileModified(getSegmentsFileName()). */
  public long getTimestamp() throws IOException {
    return getDirectory().fileModified(getSegmentsFileName());
  }
}
