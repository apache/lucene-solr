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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * Decorates a wrapped FieldComparator to add a functionality to skip over non-competitive docs.
 * IterableFieldComparator provides two additional functions for a FieldComparator:
 * 1) {@code iterator()} that returns an iterator over competitive docs that are stronger that already collected docs.
 * 2) {@code updateIterator()} that allows to update an iterator. This method is called from a collector to inform
 *      the comparator to update its iterator.
 */
public abstract class IterableFieldComparator<T> extends FieldComparator<T> {
    final FieldComparator<T> in;

    public IterableFieldComparator(FieldComparator<T> in) {
        this.in = in;
    }

    public abstract DocIdSetIterator competitiveIterator();

    public abstract void updateCompetitiveIterator() throws IOException;

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

    public static abstract class IterableNumericComparator<T extends Number> extends IterableFieldComparator<T> implements LeafFieldComparator {
        private final boolean reverse;
        private boolean hasTopValue = false;
        private DocIdSetIterator iterator = null;
        private PointValues pointValues;
        private final int bytesCount;
        private final byte[] minValueAsBytes;
        private final byte[] maxValueAsBytes;
        private boolean minValueExist = false;
        private boolean maxValueExist = false;
        private int maxDoc;
        private int maxDocVisited;
        private int updateCounter = 0;
        private final String field;

        public IterableNumericComparator(NumericComparator<T> in, boolean reverse, int bytesCount) {
            super(in);
            this.field = in.field;
            this.bytesCount = bytesCount;
            this.reverse = reverse;
            minValueAsBytes = new byte[bytesCount];
            maxValueAsBytes = new byte[bytesCount];
            if (reverse) {
                minValueExist = true;
            } else {
                maxValueExist = true;
            }
        }

        public DocIdSetIterator competitiveIterator() {
            return iterator;
        }

        @Override
        public void setTopValue(T value) {
            hasTopValue = true;
            if (reverse) {
                maxValueExist = true;
            } else {
                minValueExist = true;
            }
            in.setTopValue(value);
        }

        @Override
        public void setBottom(int slot) throws IOException {
            ((NumericComparator) in).setBottom(slot);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            return ((NumericComparator) in).compareBottom(doc);
        }

        @Override
        public int compareTop(int doc) throws IOException {
            return ((NumericComparator) in).compareTop(doc);
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            ((NumericComparator) in).copy(slot, doc);
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {}

        @Override
        public final LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
            ((NumericComparator) in).doSetNextReader(context);
            pointValues = context.reader().getPointValues(field);
            iterator = pointValues == null ? null : ((NumericComparator)in).currentReaderValues;
            maxDoc = context.reader().maxDoc();
            maxDocVisited = 0;
            return this;
        }

        // update its iterator to include possibly only docs that are "stronger" than the current bottom entry
        public void updateCompetitiveIterator() throws IOException {
            if (pointValues == null) return;
            updateCounter++;
            if (updateCounter > 256 && (updateCounter & 0x1f) != 0x1f) { // Start sampling if we get called too much
                return;
            }
            if (reverse == false) {
                encodeBottom(maxValueAsBytes);
                if (hasTopValue) {
                    encodeTop(minValueAsBytes);
                }
            } else {
                encodeBottom(minValueAsBytes);
                if (hasTopValue) {
                    encodeTop(maxValueAsBytes);
                }
            };
            DocIdSetBuilder result = new DocIdSetBuilder(maxDoc);
            PointValues.IntersectVisitor visitor = new PointValues.IntersectVisitor() {
                DocIdSetBuilder.BulkAdder adder;
                @Override
                public void grow(int count) {
                    adder = result.grow(count);
                }

                @Override
                public void visit(int docID) {
                    if (docID <= maxDocVisited) {
                        return; // Already visited or skipped
                    }
                    adder.add(docID);
                }

                @Override
                public void visit(int docID, byte[] packedValue) {
                    if (docID <= maxDocVisited) {
                        return;  // Already visited or skipped
                    }
                    if (maxValueExist) {
                        // doc's value is too high
                        if (Arrays.compareUnsigned(packedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount) > 0) return;
                    }
                    if (minValueExist) {
                        // doc's value is too low
                        if (Arrays.compareUnsigned(packedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount) < 0) return;
                    }
                    adder.add(docID); // doc is competitive
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    if ((maxValueExist && Arrays.compareUnsigned(minPackedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount) > 0) ||
                            (minValueExist && Arrays.compareUnsigned(maxPackedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount) < 0)) {
                        return PointValues.Relation.CELL_OUTSIDE_QUERY;
                    }
                    if ((maxValueExist && Arrays.compareUnsigned(maxPackedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount) > 0) ||
                            (minValueExist && Arrays.compareUnsigned(minPackedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount) < 0)) {
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
            };
            final long threshold = iterator.cost() >>> 3;
            long estimatedNumberOfMatches = pointValues.estimatePointCount(visitor); // runs in O(log(numPoints))
            if (estimatedNumberOfMatches >= threshold) {
                // the new range is not selective enough to be worth materializing, it doesn't reduce number of docs at least 8x
                return;
            }
            pointValues.intersect(visitor);
            this.iterator = result.build().iterator();
        };

        public abstract void encodeBottom(byte[] packedValue);
        public abstract void encodeTop(byte[] packedValue);
    }

    public static class IterableLongComparator extends IterableNumericComparator<Long> {
        public IterableLongComparator(LongComparator in, boolean reverse) {
            super(in, reverse, Long.BYTES);
        }

        public void encodeBottom(byte[] packedValue) {
            LongPoint.encodeDimension(((LongComparator)in).bottom, packedValue, 0);
        }

        public void encodeTop(byte[] packedValue) {
            LongPoint.encodeDimension(((LongComparator)in).topValue, packedValue, 0);
        }
    }

    public static class IterableIntComparator extends IterableNumericComparator<Integer> {
        public IterableIntComparator(IntComparator in, boolean reverse) {
            super(in, reverse, Integer.BYTES);
        }
        public void encodeBottom(byte[] packedValue) {
            IntPoint.encodeDimension(((IntComparator)in).bottom, packedValue, 0);
        }
        public void encodeTop(byte[] packedValue) {
            LongPoint.encodeDimension(((IntComparator)in).topValue, packedValue, 0);
        }
    }

    public static class IterableDoubleComparator extends IterableNumericComparator<Double> {
        public IterableDoubleComparator(DoubleComparator in, boolean reverse) {
            super(in, reverse, Double.BYTES);
        }
        public void encodeBottom(byte[] packedValue) {
            DoublePoint.encodeDimension(((DoubleComparator)in).bottom, packedValue, 0);
        }
        public void encodeTop(byte[] packedValue) {
            DoublePoint.encodeDimension(((DoubleComparator)in).topValue, packedValue, 0);
        }
    }

    public static class IterableFloatComparator extends IterableNumericComparator<Float> {
        public IterableFloatComparator(FloatComparator in, boolean reverse) {
            super(in, reverse, Float.BYTES);
        }
        public void encodeBottom(byte[] packedValue) {
            FloatPoint.encodeDimension(((FloatComparator)in).bottom, packedValue, 0);
        }
        public void encodeTop(byte[] packedValue) {
            FloatPoint.encodeDimension(((FloatComparator)in).topValue, packedValue, 0);
        }
    }
}


