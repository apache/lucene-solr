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
package org.apache.lucene.analysis.hunspell;

import java.util.Objects;

class Root<T extends CharSequence> implements Comparable<Root<T>> {
  final T word;
  final int entryId;

  Root(T word, int entryId) {
    this.word = word;
    this.entryId = entryId;
  }

  @Override
  public String toString() {
    return word.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Root)) return false;
    @SuppressWarnings("unchecked")
    Root<T> root = (Root<T>) o;
    return entryId == root.entryId && word.equals(root.word);
  }

  @Override
  public int hashCode() {
    return Objects.hash(word, entryId);
  }

  @Override
  public int compareTo(Root<T> o) {
    return CharSequence.compare(word, o.word);
  }
}
