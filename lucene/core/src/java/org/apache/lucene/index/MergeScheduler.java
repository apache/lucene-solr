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
package org.apache.lucene.index;


import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RateLimitedIndexOutput;
import org.apache.lucene.util.InfoStream;

/** <p>Expert: {@link IndexWriter} uses an instance
 *  implementing this interface to execute the merges
 *  selected by a {@link MergePolicy}.  The default
 *  MergeScheduler is {@link ConcurrentMergeScheduler}.</p>
 * @lucene.experimental
*/
public abstract class MergeScheduler implements Closeable {

  /** Sole constructor. (For invocation by subclass
   *  constructors, typically implicit.) */
  protected MergeScheduler() {
  }

  /** Run the merges provided by {@link MergeSource#getNextMerge()}.
   * @param mergeSource the {@link IndexWriter} to obtain the merges from.
   * @param trigger the {@link MergeTrigger} that caused this merge to happen */
  public abstract void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException;

  /** 
   * Wraps the incoming {@link Directory} so that we can merge-throttle it
   * using {@link RateLimitedIndexOutput}. 
   */
  public Directory wrapForMerge(OneMerge merge, Directory in) {
    // A no-op by default.
    return in;
  }

  /** Close this MergeScheduler. */
  @Override
  public abstract void close() throws IOException;

  /** For messages about merge scheduling */
  protected InfoStream infoStream;

  /** IndexWriter calls this on init. */
  void initialize(InfoStream infoStream, Directory directory) throws IOException {
    this.infoStream = infoStream;
  }

  /**
   * Returns true if infoStream messages are enabled. This method is usually used in
   * conjunction with {@link #message(String)}:
   * 
   * <pre class="prettyprint">
   * if (verbose()) {
   *   message(&quot;your message&quot;);
   * }
   * </pre>
   */
  protected boolean verbose() {
    return infoStream != null && infoStream.isEnabled("MS");
  }
 
  /**
   * Outputs the given message - this method assumes {@link #verbose()} was
   * called and returned true.
   */
  protected void message(String message) {
    infoStream.message("MS", message);
  }

  /**
   * Provides access to new merges and executes the actual merge
   * @lucene.experimental
   */
  public interface MergeSource {
    /**
     * The {@link MergeScheduler} calls this method to retrieve the next
     * merge requested by the MergePolicy
     */
    MergePolicy.OneMerge getNextMerge();

    /**
     * Does finishing for a merge.
     */
    void onMergeFinished(MergePolicy.OneMerge merge);

    /**
     * Expert: returns true if there are merges waiting to be scheduled.
     */
    boolean hasPendingMerges();

    /**
     * Merges the indicated segments, replacing them in the stack with a
     * single segment.
     */
    void merge(MergePolicy.OneMerge merge) throws IOException;
  }
}
