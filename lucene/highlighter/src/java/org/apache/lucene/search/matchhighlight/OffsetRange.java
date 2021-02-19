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
package org.apache.lucene.search.matchhighlight;

import java.util.Objects;

/** A non-empty range of offset positions. */
public class OffsetRange {
  /** Start index, inclusive. */
  public final int from;

  /** End index, exclusive. */
  public final int to;

  /**
   * @param from Start index, inclusive.
   * @param to End index, exclusive.
   */
  public OffsetRange(int from, int to) {
    assert from <= to : "A non-empty offset range is required: " + from + "-" + to;
    this.from = from;
    this.to = to;
  }

  public int length() {
    return to - from;
  }

  @Override
  public String toString() {
    return "[from=" + from + ", to=" + to + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (other instanceof OffsetRange) {
      OffsetRange that = (OffsetRange) other;
      return from == that.from && to == that.to;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to);
  }

  /**
   * Returns a sub-range of this range (a copy). Subclasses should override and return an
   * appropriate type covariant so that payloads are not lost.
   */
  public OffsetRange slice(int from, int to) {
    assert from >= this.from;
    assert to <= this.to;
    return new OffsetRange(from, to);
  }
}
