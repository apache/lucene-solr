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

package org.apache.lucene.concordance.classic;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract class to handle basic information for a ConcordanceWindowSearcher
 */
public abstract class AbstractConcordanceWindowCollector {
  //value to use if all windows should be collected
  public static final int COLLECT_ALL = -1;

  private final ConcordanceSorter sorter = new ConcordanceSorter();
  private final int maxWindows;
  private Set<String> docIds = new HashSet<String>();
  private boolean hitMax = false;
  private long totalDocs = 0;

  /**
   * @param maxWindows maximum windows to collect
   */
  public AbstractConcordanceWindowCollector(int maxWindows) {
    this.maxWindows = maxWindows;
  }

  /**
   * Collect/process this window
   *
   * @param w window to be processed
   */
  public abstract void collect(ConcordanceWindow w);

  /**
   * @return number of windows collected
   */
  public abstract int size();

  /**
   * @return collected windows (unsorted)
   */
  public abstract List<ConcordanceWindow> getWindows();

  /**
   * @param docId unique key for a document
   */
  public void addDocId(String docId) {
    docIds.add(docId);
  }

  /**
   * Sort according to {@link #sorter} and return windows
   *
   * @return sorted list of windows
   */
  public List<ConcordanceWindow> getSortedWindows() {
    List<ConcordanceWindow> windows = getWindows();
    Collections.sort(windows, sorter);
    return windows;
  }

  /**
   * @return whether or not the searcher collected the maximum number of
   * windows and stopped early.
   */
  public boolean getHitMax() {
    return hitMax;
  }

  /**
   * @param hitMax did the searcher collect the maximum number of windows
   *               and stop early
   */
  public void setHitMax(boolean hitMax) {
    this.hitMax = hitMax;
  }

  /**
   * @return the maximum number of windows to collect.
   * Can be equal to {@link #COLLECT_ALL}
   */
  public int getMaxWindows() {
    return maxWindows;
  }

  /**
   * @param totalDocs add this value to {@link #totalDocs}
   */
  public void incrementTotalDocs(long totalDocs) {
    this.totalDocs += totalDocs;
  }

  /**
   * @return total number of documents in all indices
   */
  public long getTotalDocs() {
    return totalDocs;
  }

  /**
   * @param totalDocs see {@link #getTotalDocs()}
   */
  public void setTotalDocs(long totalDocs) {
    this.totalDocs = totalDocs;
  }

  /**
   * @return number of windows in results
   */
  public int getNumWindows() {
    List<ConcordanceWindow> windows = getWindows();
    if (windows != null) {
      return windows.size();
    }
    return 0;
  }

  /**
   * @return number of documents in results
   */
  public int getNumDocs() {
    return docIds.size();
  }
}
