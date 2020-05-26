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

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 * A wrapper over {@code FieldComparator} that provides a leaf comparator that can filter non-competitive docs.
 */
abstract class FilteringFieldComparator<T> extends FieldComparator<T> {
  protected final FieldComparator<T> in;
  protected final boolean reverse;
  // singleSort is true, if sort is based on a single sort field. As there are no other sorts configured
  // as tie breakers, we can filter out docs with equal values.
  protected final boolean singleSort;
  protected boolean hasTopValue = false;

  public FilteringFieldComparator(FieldComparator<T> in, boolean reverse, boolean singleSort) {
    this.in = in;
    this.reverse = reverse;
    this.singleSort = singleSort;
  }

  @Override
  public abstract FilteringLeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException;

  @Override
  public int compare(int slot1, int slot2) {
    return in.compare(slot1, slot2);
  }

  @Override
  public T value(int slot) {
    return in.value(slot);
  }

  @Override
  public void setTopValue(T value) {
    in.setTopValue(value);
    hasTopValue = true;
  }

  @Override
  public int compareValues(T first, T second) {
    return in.compareValues(first, second);
  }


  /**
   * Try to wrap a given field comparator to add to it a functionality to skip over non-competitive docs.
   * If for the given comparator the skip functionality is not implemented, return the comparator itself.
   * @param comparator – comparator to wrap
   * @param reverse – if this sort is reverse
   * @param singleSort – true if this sort is based on a single field and there are no other sort fields for tie breaking
   * @return comparator wrapped as a filtering comparator or the original comparator if the filtering functionality
   * is not implemented for it
   */
  public static FieldComparator<?> wrapToFilteringComparator(FieldComparator<?> comparator, boolean reverse, boolean singleSort) {
    Class<?> comparatorClass = comparator.getClass();
    if (comparatorClass == FieldComparator.LongComparator.class){
      return new FilteringNumericComparator<>((FieldComparator.LongComparator) comparator, reverse, singleSort);
    }
    if (comparatorClass == FieldComparator.IntComparator.class){
      return new FilteringNumericComparator<>((FieldComparator.IntComparator) comparator, reverse, singleSort);
    }
    if (comparatorClass == FieldComparator.DoubleComparator.class){
      return new FilteringNumericComparator<>((FieldComparator.DoubleComparator) comparator, reverse, singleSort);
    }
    if (comparatorClass == FieldComparator.FloatComparator.class){
      return new FilteringNumericComparator<>((FieldComparator.FloatComparator) comparator, reverse, singleSort);
    }
    return comparator;
  }

}


