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

/**
 * Simple comparable class to allow for subclassing.
 */
public class ConcordanceSortKey implements Comparable<ConcordanceSortKey> {

  private final String concSortString;

  public ConcordanceSortKey(String s) {
    this.concSortString = s;
  }

  @Override
  public int compareTo(ConcordanceSortKey other) {
    return concSortString.compareTo(other.concSortString);
  }

  @Override
  public int hashCode() {
    return concSortString.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof ConcordanceSortKey))
      return false;
    ConcordanceSortKey other = (ConcordanceSortKey) obj;
    if (concSortString == null) {
      if (other.concSortString != null)
        return false;
    } else if (!concSortString.equals(other.concSortString))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return concSortString;
  }


}
