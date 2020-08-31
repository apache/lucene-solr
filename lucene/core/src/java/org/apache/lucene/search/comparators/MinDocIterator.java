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

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * Docs iterator that starts iterating from a configurable minimum document
 */
public class MinDocIterator extends DocIdSetIterator {
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
