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

/**
 * Decorates a wrapped FieldComparator to add a functionality to skip over non-competitive docs.
 * FilteringFieldComparator provides two additional functions for a FieldComparator:
 * 1) {@code competitiveIterator()} that returns an iterator over
 *      competitive docs that are stronger than already collected docs.
 * 2) {@code setCanUpdateIterator()} that notifies the comparator when it is ok to start updating its internal iterator.
 *  This method is called from a collector to inform the comparator to start updating its iterator.
 */
public abstract class FilteringFieldComparator<T> extends FieldComparator<T> {
    final FieldComparator<T> in;

    public FilteringFieldComparator(FieldComparator<T> in) {
        this.in = in;
    }

    protected abstract DocIdSetIterator competitiveIterator();

    protected abstract void setCanUpdateIterator() throws IOException;
    
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
    }

    @Override
    public int compareValues(T first, T second) {
        return in.compareValues(first, second);
    }


    /**
     * Try to wrap a given field comparator to add to it a functionality to skip over non-competitive docs.
     * If for the given comparator the skip functionality is not implemented, return the comparator itself.
     */
    public static FieldComparator<?> wrapToFilteringComparator(FieldComparator<?> comparator, boolean reverse) {
        if (comparator instanceof FieldComparator.LongComparator){
            return new FilteringNumericComparator.FilteringLongComparator((FieldComparator.LongComparator) comparator, reverse);
        }
        if (comparator instanceof FieldComparator.IntComparator){
            return new FilteringNumericComparator.FilteringIntComparator((FieldComparator.IntComparator) comparator, reverse);
        }
        if (comparator instanceof FieldComparator.DoubleComparator){
            return new FilteringNumericComparator.FilteringDoubleComparator((FieldComparator.DoubleComparator) comparator, reverse);
        }
        if (comparator instanceof FieldComparator.FloatComparator){
            return new FilteringNumericComparator.FilteringFloatComparator((FieldComparator.FloatComparator) comparator, reverse);
        }
        return comparator;
    }

}


