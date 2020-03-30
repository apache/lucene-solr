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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.FieldComparator.IterableComparator;

/**
 * Expert: a FieldComparator class for long types corresponding to
 * {@link LongDocValuesPointSortField}.
 * This comparator provides {@code iterator} over competitive documents,
 * that are stronger than the current {@code bottom} value.
 */
public class LongDocValuesPointComparator extends IterableComparator<Long> {
    private final String field;
    private final boolean reverse;
    private final long missingValue;
    private final long[] values;
    private long bottom;
    private long topValue;
    protected NumericDocValues docValues;
    private DocIdSetIterator iterator = null;
    private PointValues pointValues;
    private int maxDoc;
    private int maxDocVisited;
    private int updateCounter = 0;
    private byte[] cmaxValueAsBytes = null;
    private byte[] cminValueAsBytes = null;

    public LongDocValuesPointComparator(String field, int numHits, boolean reverse, Long missingValue) {
        this.field = field;
        this.reverse = reverse;
        this.missingValue = missingValue != null ? missingValue : 0L;
        this.values = new long[numHits];
        if (reverse == false) {
            cmaxValueAsBytes = new byte[Long.BYTES];
        } else {
            cminValueAsBytes = new byte[Long.BYTES];
        };
    }

    private long getValueForDoc(int doc) throws IOException {
        if (docValues.advanceExact(doc)) {
            return docValues.longValue();
        } else {
            return missingValue;
        }
    }

    @Override
    public int compare(int slot1, int slot2) {
        return Long.compare(values[slot1], values[slot2]);
    }

    @Override
    public void setTopValue(Long value) {
        topValue = value;
        if (reverse == false) {
            cminValueAsBytes = new byte[Long.BYTES];
        } else {
            cmaxValueAsBytes = new byte[Long.BYTES];
        };
    }

    @Override
    public Long value(int slot) {
        return Long.valueOf(values[slot]);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        docValues = DocValues.getNumeric(context.reader(), field);
        pointValues = context.reader().getPointValues(field);
        iterator = pointValues == null ? null : docValues; // if a field is not indexed with points, its iterator is null
        maxDoc = context.reader().maxDoc();
        maxDocVisited = 0;
        return this;
    }

    @Override
    public void setBottom(int slot) {
        this.bottom = values[slot];
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        return Long.compare(bottom, getValueForDoc(doc));
    }

    @Override
    public int compareTop(int doc) throws IOException {
        return Long.compare(topValue, getValueForDoc(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        maxDocVisited = doc;
        values[slot] = getValueForDoc(doc);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    public DocIdSetIterator iterator() {
        return iterator;
    }

    // update its iterator to include possibly only docs that are "stronger" than the current bottom entry
    public void updateIterator() throws IOException {
        if (pointValues == null) return;
        updateCounter++;
        if (updateCounter > 256 && (updateCounter & 0x1f) != 0x1f) { // Start sampling if we get called too much
            return;
        }
        final byte[] maxValueAsBytes = cmaxValueAsBytes;
        final byte[] minValueAsBytes = cminValueAsBytes;
        if (reverse == false) {
            LongPoint.encodeDimension(bottom, maxValueAsBytes, 0);
            if (minValueAsBytes != null) { // has top value
                LongPoint.encodeDimension(topValue, minValueAsBytes, 0);
            }
        } else {
            LongPoint.encodeDimension(bottom, minValueAsBytes, 0);
            if (maxValueAsBytes != null) { // has top value
                LongPoint.encodeDimension(topValue, maxValueAsBytes, 0);
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
                if (maxValueAsBytes != null) {
                    // doc's value is too high
                    if (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0) return;
                }
                if (minValueAsBytes != null) {
                    // doc's value is too low
                    if (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0) return;
                }
                adder.add(docID); // doc is competitive
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                if (((maxValueAsBytes != null) &&
                        Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0) ||
                        ((minValueAsBytes != null) &&
                        Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0)) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (((maxValueAsBytes != null) &&
                        Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0) ||
                        ((minValueAsBytes != null) &&
                        Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0)) {
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
    }
}
