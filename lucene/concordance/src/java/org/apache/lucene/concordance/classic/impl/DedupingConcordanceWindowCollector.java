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

package org.apache.lucene.concordance.classic.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.concordance.classic.AbstractConcordanceWindowCollector;
import org.apache.lucene.concordance.classic.ConcordanceWindow;

/**
 * Like ConcordanceWindowCollector, but this collector
 * doesn't store duplicate windows.  Windows are defined as duplicates by
 * {@link #buildEqualityKey(ConcordanceWindow, StringBuilder)}.
 */
public class DedupingConcordanceWindowCollector extends AbstractConcordanceWindowCollector {

  Map<String, ConcordanceWindow> map = new HashMap<String, ConcordanceWindow>();
  private StringBuilder sb = new StringBuilder();

  /**
   * @param maxHits maximum number of windows to store.  This could potentially
   *                visit lots more windows than maxHits.
   */
  public DedupingConcordanceWindowCollector(int maxHits) {
    super(maxHits);
  }

  @Override
  public void collect(ConcordanceWindow w) {
    if (getHitMax() == true) {
      return;
    }
    buildEqualityKey(w, sb);
    String key = sb.toString();
    ConcordanceWindow oldWindow = map.get(key);
    if (oldWindow == null) {
      //we would have added a new window here
      if (getMaxWindows() != AbstractConcordanceWindowCollector.COLLECT_ALL &&
          map.size() >= getMaxWindows()) {
        setHitMax(true);
        return;
      }
      oldWindow = w;
    } else {
      //if the old window existed (i.e. new window is a duplicate)
      //keep incrementing the count
      oldWindow.incrementCount();
    }

    map.put(key, oldWindow);
    addDocId(w.getUniqueDocID());
  }


  /**
   * number of windows collected
   */
  @Override
  public int size() {
    return map.size();
  }

  @Override
  public List<ConcordanceWindow> getWindows() {
    List<ConcordanceWindow> windows = new ArrayList<>();
    windows.addAll(map.values());
    return windows;
  }

  /**
   * Public for easy overriding.  Generate a key to be used to determine
   * whether two windows are the same.  Some implementations
   * might want to lowercase, some might want genuine case folding,
   * some might want to strip non-alphanumerics, etc.
   * <p>
   * If you are overriding this, make sure to call sb.setLength(0)!
   *
   * @param w  ConcordanceWindow
   * @param sb reuseable StringBuilder; sb.setLength(0) is called before use!
   */
  public void buildEqualityKey(ConcordanceWindow w, StringBuilder sb) {
    sb.setLength(0);
    sb.append(w.getPre().toLowerCase());
    sb.append(">>>");
    sb.append(w.getTarget().toLowerCase());
    sb.append("<<<");
    sb.append(w.getPost().toLowerCase());
  }
}
