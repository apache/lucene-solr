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
 * This comparator is used when there is sort by _doc asc together with "after" FieldDoc.
 * The comparator provides an iterator that can quickly skip to the desired "after" document.
 */
public class FilteringAfterDocLeafComparator implements FilteringLeafFieldComparator {
    private final FieldComparator.DocComparator in;
    private DocIdSetIterator topValueIterator; // iterator that starts from topValue if possible
    private final int minDoc;
    private final int maxDoc;
    private final int docBase;
    private boolean iteratorUpdated = false;

    public FilteringAfterDocLeafComparator(FieldComparator.DocComparator in, LeafReaderContext context) {
        this.in = in;
        this.minDoc = this.in.getTopValue() + 1;
        this.maxDoc = context.reader().maxDoc();
        this.docBase = context.docBase;
        this.topValueIterator = DocIdSetIterator.all(maxDoc);
    }

    @Override
    public DocIdSetIterator competitiveIterator() {
        return new DocIdSetIterator() {
            private int doc;

            @Override
            public int nextDoc() throws IOException {
                return doc = topValueIterator.nextDoc();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public long cost() {
                return topValueIterator.cost();
            }

            @Override
            public int advance(int target) throws IOException {
                return doc = topValueIterator.advance(target);
            }
        };

    }

    @Override
    public boolean iteratorUpdated() {
        return iteratorUpdated;
    }

    @Override
    public void setQueueFull() {
    }

    @Override
    public void setHitsThresholdReached() {
        if (iteratorUpdated) return;
        if (docBase + maxDoc <= minDoc) {
            topValueIterator = DocIdSetIterator.empty(); // skip this segment
        }
        final int segmentMinDoc = Math.max(0, minDoc - docBase);
        topValueIterator = new MinDocIterator(segmentMinDoc, maxDoc);
        iteratorUpdated = true;
    }

    @Override
    public void setBottom(int slot) {
        in.setBottom(slot);
    }

    @Override
    public int compareBottom(int doc) {
        return in.compareBottom(doc);
    }

    @Override
    public int compareTop(int doc) {
        return in.compareTop(doc);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        in.copy(slot, doc);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        in.setScorer(scorer);
    }


    static class MinDocIterator extends DocIdSetIterator {
        final int segmentMinDoc;
        final int maxDoc;
        int doc = -1;

        MinDocIterator(int segmentMinDoc, int maxDoc) {
            this.segmentMinDoc = segmentMinDoc;
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            assert target > doc;
            if (doc == -1) {
                // skip directly to minDoc
                doc = Math.max(target, segmentMinDoc);
            } else {
                doc = target;
            }
            if (doc >= maxDoc) {
                doc = NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public long cost() {
            return maxDoc - segmentMinDoc;
        }
    }
}
