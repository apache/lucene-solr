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

import java.util.Map;

/**
 * Key element in a concordance view of data. A window consists of the words
 * before a target term (pre), the target term and then the words after the
 * target term (post). A window also has a sort key to allow for various methods
 * of sorting.
 * <p>
 * For various applications, it has also been useful to store a unique document key,
 * character offset (start and end) of the full
 * window as well as metadata from the document for the given window.
 * <p>
 * This class is experimental and may change in incompatible ways in the future.
 */
public class ConcordanceWindow {

  private final ConcordanceSortKey sortKey;
  private final String pre;
  private final String target;
  private final String post;
  private final int charStart;
  private final int charEnd;
  private final String uniqueDocID;
  //used by hide duplicates to count more than one occurrence of a window
  private int count = 1;
  private Map<String, String> metadata;

  /**
   * @param uniqueDocID string representing what should be a unique document identifier
   * @param charStart   character offset start for the window
   * @param charEnd     character offset end for the window
   * @param pre         words before the target in reading order and unanalyzed
   * @param target      target string
   * @param post        string after the target in reading order and unanalyzed
   * @param sortKey     key to use for sorting this window
   * @param metadata    metadata to store with this window
   */
  public ConcordanceWindow(String uniqueDocID, int charStart, int charEnd, String pre,
                           String target, String post, ConcordanceSortKey sortKey, Map<String, String> metadata) {
    this.pre = pre;
    this.target = target;
    this.post = post;
    this.uniqueDocID = uniqueDocID;
    this.charStart = charStart;
    this.charEnd = charEnd;
    this.metadata = metadata;
    this.sortKey = sortKey;
  }

  public String getUniqueDocID() {
    return uniqueDocID;
  }

  public int getStart() {
    return charStart;
  }

  public int getEnd() {
    return charEnd;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public String getPre() {
    return pre;
  }

  public String getPost() {
    return post;
  }

  public String getTarget() {
    return target;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public void incrementCount() {
    count++;
  }

  public int getSize() {
    int size = 0;
    if (pre != null) {
      size += pre.length();
    }
    if (target != null) {
      size += target.length();
    }
    if (post != null) {
      size += post.length();
    }
    return size;
  }

  public ConcordanceSortKey getSortKey() {
    return sortKey;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((post == null) ? 0 : post.hashCode());
    result = prime * result + ((pre == null) ? 0 : pre.hashCode());
    result = prime * result + ((target == null) ? 0 : target.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ConcordanceWindow)) {
      return false;
    }
    ConcordanceWindow other = (ConcordanceWindow) obj;
    if (post == null) {
      if (other.post != null) {
        return false;
      }
    } else if (!post.equals(other.post)) {
      return false;
    }
    if (pre == null) {
      if (other.pre != null) {
        return false;
      }
    } else if (!pre.equals(other.pre)) {
      return false;
    }
    if (target == null) {
      if (other.target != null) {
        return false;
      }
    } else if (!target.equals(other.target)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(pre).append(">>>").append(target).append("<<<").append(post);
    return sb.toString();
  }
}
