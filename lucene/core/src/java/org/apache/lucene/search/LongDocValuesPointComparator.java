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

import static org.apache.lucene.search.FieldComparator.IteratorSupplierComparator;

public class LongDocValuesPointComparator extends IteratorSupplierComparator<Long> {
    private final String field;
    private final boolean reverse;
    private final long[] values;
    private long bottom;
    private long topValue;
    protected NumericDocValues docValues;
    private DocIdSetIterator iterator;
    private PointValues pointValues;
    private int maxDoc;
    private int maxDocVisited;

    public LongDocValuesPointComparator(String field, int numHits, boolean reverse) {
        this.field = field;
        this.reverse = reverse;
        this.values = new long[numHits];
    }

    private long getValueForDoc(int doc) throws IOException {
        if (docValues.advanceExact(doc)) {
            return docValues.longValue();
        } else {
            return 0L; // TODO: missing value
        }
    }

    @Override
    public int compare(int slot1, int slot2) {
        return Long.compare(values[slot1], values[slot2]);
    }

    @Override
    public void setTopValue(Long value) {
        topValue = value;
    }

    @Override
    public Long value(int slot) {
        return Long.valueOf(values[slot]);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        docValues = DocValues.getNumeric(context.reader(), field);
        iterator = docValues;
        pointValues = context.reader().getPointValues(field);
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

    public void updateIterator() throws IOException {
        final byte[] maxValueAsBytes = new byte[Long.BYTES];
        final byte[] minValueAsBytes = new byte[Long.BYTES];
        if (reverse == false) {
            LongPoint.encodeDimension(bottom, maxValueAsBytes, 0);
        } else {
            LongPoint.encodeDimension(bottom, minValueAsBytes, 0);
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
                if ((reverse == false) && (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0)) {
                    return; // Doc's value is too high
                }
                if ((reverse == true) && (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0)) {
                    return;  // Doc's value is too low,
                }
                adder.add(docID); // doc is competitive
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                if ((reverse == false) && (Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0)) {
                   return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if ((reverse == true) && (Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0)) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
        pointValues.intersect(visitor);
        this.iterator = result.build().iterator();
    }
}
