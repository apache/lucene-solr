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

package org.apache.lucene.search.comparators;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FilteringLeafFieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;

import java.io.IOException;

/**
 * Comparator that sorts by asc _doc
 */
public class DocComparator extends FieldComparator<Integer> {
    private final int[] docIDs;
    private int topValue;
    private boolean topValueSet;
    private boolean reverse = false; // only used to check if skipping functionality should be enabled

    /** Creates a new comparator based on document ids for {@code numHits} */
    public DocComparator(int numHits) {
        docIDs = new int[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        // No overflow risk because docIDs are non-negative
        return docIDs[slot1] - docIDs[slot2];
    }


    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
        // TODO: can we "map" our docIDs to the current
        // reader? saves having to then subtract on every
        // compare call
        return new DocLeafComparator(context);
    }

    @Override
    public void setTopValue(Integer value) {
        topValue = value;
        topValueSet = true;
    }

    @Override
    public Integer value(int slot) {
        return Integer.valueOf(docIDs[slot]);
    }

    @Override
    public void setReverse() {
        reverse = true;
    }


    /**
     * DocLeafComparator with skipping functionality.
     * When sort by _doc asc and "after" document is set,
     * the comparator provides an iterator that can quickly skip to the desired "after" document.
     */
    private class DocLeafComparator implements FilteringLeafFieldComparator {
        private final int docBase;
        private int bottom;

        private final boolean enableSkipping;
        private final int minDoc;
        private final int maxDoc;
        private DocIdSetIterator topValueIterator; // iterator that starts from topValue

        private boolean iteratorUpdated = false;

        public DocLeafComparator(LeafReaderContext context) {
            this.docBase = context.docBase;
            // skipping functionality is enabled if topValue is set and sort is asc
            this.enableSkipping = topValueSet && reverse == false ? true: false;
            if (enableSkipping) {
                this.minDoc = topValue + 1;
                this.maxDoc = context.reader().maxDoc();
                this.topValueIterator = DocIdSetIterator.all(maxDoc);
            } else {
                this.minDoc = -1;
                this.maxDoc = -1;
                this.topValueIterator = null;
            }
        }

        @Override
        public void setBottom(int slot) {
            bottom = docIDs[slot];
        }

        @Override
        public int compareBottom(int doc) {
            // No overflow risk because docIDs are non-negative
            return bottom - (docBase + doc);
        }

        @Override
        public int compareTop(int doc) {
            int docValue = docBase + doc;
            return Integer.compare(topValue, docValue);
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            docIDs[slot] = docBase + doc;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
        }

        @Override
        public DocIdSetIterator competitiveIterator() {
            if (enableSkipping == false) {
                return null;
            } else {
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
        }

        @Override
        public void setHitsThresholdReached() {
            if (enableSkipping == false) return;
            if (iteratorUpdated) return; // iterator for a segment needs to be updated only once
            if (docBase + maxDoc <= minDoc) {
                topValueIterator = DocIdSetIterator.empty(); // skip this segment
            } else {
                int segmentMinDoc = Math.max(0, minDoc - docBase);
                topValueIterator = new MinDocIterator(segmentMinDoc, maxDoc);
            }
            iteratorUpdated = true;
        }

        @Override
        public void setQueueFull() {
        }

        @Override
        public boolean iteratorUpdated() {
            return enableSkipping ? iteratorUpdated : false;
        }
    }
}
