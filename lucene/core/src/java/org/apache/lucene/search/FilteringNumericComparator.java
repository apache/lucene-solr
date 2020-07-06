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
 * A wrapper over {@code NumericComparator} that provides a leaf comparator that can filter non-competitive docs.
 */
class FilteringNumericComparator<T extends Number> extends FilteringFieldComparator<T> {
  public FilteringNumericComparator(NumericComparator<T> in, boolean reverse, boolean singleSort) {
    super(in, reverse, singleSort);
  }

  @Override
  public final FilteringLeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    LeafFieldComparator inLeafComparator = in.getLeafComparator(context);
    Class<?> comparatorClass = inLeafComparator.getClass();
    if (comparatorClass == FieldComparator.LongComparator.class) {
      return new FilteringNumericLeafComparator.FilteringLongLeafComparator((FieldComparator.LongComparator) inLeafComparator, context,
          ((LongComparator) inLeafComparator).field, reverse, singleSort, hasTopValue);
    } if (comparatorClass == FieldComparator.IntComparator.class) {
      return new FilteringNumericLeafComparator.FilteringIntLeafComparator((FieldComparator.IntComparator) inLeafComparator, context,
          ((IntComparator) inLeafComparator).field, reverse, singleSort, hasTopValue);
    } else if (comparatorClass == FieldComparator.DoubleComparator.class) {
      return new FilteringNumericLeafComparator.FilteringDoubleLeafComparator((FieldComparator.DoubleComparator) inLeafComparator, context,
          ((DoubleComparator) inLeafComparator).field, reverse, singleSort, hasTopValue);
    } else if (comparatorClass == FieldComparator.FloatComparator.class) {
      return new FilteringNumericLeafComparator.FilteringFloatLeafComparator((FieldComparator.FloatComparator) inLeafComparator, context,
          ((FloatComparator) inLeafComparator).field, reverse, singleSort, hasTopValue);
    } else {
      throw new IllegalStateException("Unexpected numeric class of ["+ comparatorClass + "] for [FieldComparator]!");
    }
  }

}
