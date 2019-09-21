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
package org.apache.lucene.search;

import java.util.Objects;

/**
 * Description of the total number of hits of a query. The total hit count
 * can't generally be computed accurately without visiting all matches, which
 * is costly for queries that match lots of documents. Given that it is often
 * enough to have a lower bounds of the number of hits, such as
 * "there are more than 1000 hits", Lucene has options to stop counting as soon
 * as a threshold has been reached in order to improve query times.
 */
public final class TotalHits {

  /** How the {@link TotalHits#value} should be interpreted. */
  public enum Relation {
    /**
     * The total hit count is equal to {@link TotalHits#value}.
     */
    EQUAL_TO,
    /**
     * The total hit count is greater than or equal to {@link TotalHits#value}.
     */
    GREATER_THAN_OR_EQUAL_TO
  }

  /**
   * The value of the total hit count. Must be interpreted in the context of
   * {@link #relation}.
   */
  public final long value;

  /**
   * Whether {@link #value} is the exact hit count, in which case
   * {@link #relation} is equal to {@link Relation#EQUAL_TO}, or a lower bound
   * of the total hit count, in which case {@link #relation} is equal to
   * {@link Relation#GREATER_THAN_OR_EQUAL_TO}.
   */
  public final Relation relation;

  /** Sole constructor. */
  public TotalHits(long value, Relation relation) {
    if (value < 0) {
      throw new IllegalArgumentException("value must be >= 0, got " + value);
    }
    this.value = value;
    this.relation = Objects.requireNonNull(relation);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TotalHits totalHits = (TotalHits) o;
    return value == totalHits.value && relation == totalHits.relation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, relation);
  }

  @Override
  public String toString() {
    return value + (relation == Relation.EQUAL_TO ? "" : "+") + " hits";
  }

}
