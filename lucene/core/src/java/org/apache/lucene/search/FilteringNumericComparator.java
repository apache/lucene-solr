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
public class FilteringNumericComparator<T extends Number> extends FilteringFieldComparator<T> {
  public FilteringNumericComparator(NumericComparator<T> in, boolean reverse, boolean singleSort) {
    super(in, reverse, singleSort);
  }

  @Override
  public final FilteringLeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    ((NumericComparator) in).doSetNextReader(context);
    if (in instanceof FieldComparator.LongComparator) {
      return new FilteringNumericLeafComparator.FilteringLongLeafComparator((FieldComparator.LongComparator) in, context,
          ((LongComparator) in).field, reverse, singleSort, hasTopValue);
    } else if (in instanceof FieldComparator.IntComparator) {
      return new FilteringNumericLeafComparator.FilteringIntLeafComparator((FieldComparator.IntComparator) in, context,
          ((IntComparator) in).field, reverse, singleSort, hasTopValue);
    } else if (in instanceof FieldComparator.DoubleComparator) {
      return new FilteringNumericLeafComparator.FilteringDoubleLeafComparator((FieldComparator.DoubleComparator) in, context,
          ((DoubleComparator) in).field, reverse, singleSort, hasTopValue);
    } else { // instanceof FieldComparator.FloatComparator
      return new FilteringNumericLeafComparator.FilteringFloatLeafComparator((FieldComparator.FloatComparator) in, context,
          ((FloatComparator) in).field, reverse, singleSort, hasTopValue);
    }
  }

}