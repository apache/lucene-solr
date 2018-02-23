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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class IntervalFunction {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  public abstract IntervalIterator apply(List<IntervalIterator> iterators);

  public static final IntervalFunction ORDERED = new SingletonFunction("ORDERED") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return Intervals.orderedIntervalIterator(intervalIterators);
    }
  };

  public static class OrderedNearFunction extends IntervalFunction {

    public OrderedNearFunction(int minWidth, int maxWidth) {
      this.minWidth = minWidth;
      this.maxWidth = maxWidth;
    }

    final int minWidth;
    final int maxWidth;

    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return Intervals.innerWidthFilter(Intervals.orderedIntervalIterator(intervalIterators), minWidth, maxWidth);
    }

    @Override
    public String toString() {
      return "ONEAR[" + minWidth + "/" + maxWidth + "]";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OrderedNearFunction that = (OrderedNearFunction) o;
      return minWidth == that.minWidth &&
          maxWidth == that.maxWidth;
    }

    @Override
    public int hashCode() {
      return Objects.hash(minWidth, maxWidth);
    }
  }

  public static final IntervalFunction UNORDERED = new SingletonFunction("UNORDERED") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return Intervals.unorderedIntervalIterator(intervalIterators);
    }
  };

  public static class UnorderedNearFunction extends IntervalFunction {

    final int minWidth;
    final int maxWidth;

    public UnorderedNearFunction(int minWidth, int maxWidth) {
      this.minWidth = minWidth;
      this.maxWidth = maxWidth;
    }

    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return Intervals.innerWidthFilter(Intervals.unorderedIntervalIterator(intervalIterators), minWidth, maxWidth);
    }

    @Override
    public String toString() {
      return "ONEAR[" + minWidth + "/" + maxWidth + "]";
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UnorderedNearFunction that = (UnorderedNearFunction) o;
      return minWidth == that.minWidth &&
          maxWidth == that.maxWidth;
    }

    @Override
    public int hashCode() {
      return Objects.hash(minWidth, maxWidth);
    }
  }

  private static abstract class SingletonFunction extends IntervalFunction {

    private final String name;

    protected SingletonFunction(String name) {
      this.name = name;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public String toString() {
      return name;
    }

  }

}
