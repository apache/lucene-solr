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

/**
 * <p>Expert: represents a single commit into an index as seen by the
 * {@link IndexDeletionPolicy}. 
 * <p>
 * Changes to the content of an index are made visible only
 * after the writer who made that change had written to the
 * directory a new segments file (<code>segments_N</code>). This point in 
 * time, when the action of writing of a new segments file to the
 * directory is completed, is therefore an index commit point.
 * <p>
 * Each index commit point has a unique segments file associated
 * with it. The segments file associated with a later 
 * index commit point would have a larger N.
 */

public interface IndexCommitPoint {

  /**
   * Get the segments file (<code>segments_N</code>) associated 
   * with this commit point.
   */
  public String getSegmentsFileName();
  
  /**
   * Delete this commit point.
   * <p>
   * Upon calling this, the writer is notified that this commit 
   * point should be deleted. 
   * <p>
   * Decision that a commit-point should be deleted is taken by the {@link IndexDeletionPolicy} in effect
   * and therefore this should only be called by its {@link IndexDeletionPolicy#onInit onInit()} or 
   * {@link IndexDeletionPolicy#onCommit onCommit()} methods.
  */
  public void delete();
}
