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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

/** A mock interval source that will only return a constant position for all documents */
public class OneTimeIntervalSource extends IntervalsSource {
  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    return new IntervalIterator() {
      int doc = -1;
      boolean flag;
      final int maxDoc = ctx.reader().maxDoc();

      @Override
      public int start() {
        return 0;
      }

      @Override
      public int end() {
        return 0;
      }

      @Override
      public int gaps() {
        return 0;
      }

      /* only returns valid position every first time called per doc */
      @Override
      public int nextInterval() throws IOException {
        if (doc != NO_MORE_DOCS) {
          if (flag) {
            flag = false;
            return start();
          } else {
            return NO_MORE_INTERVALS;
          }
        }
        throw new AssertionError("Called with docId == NO_MORE_DOCS");
      }

      @Override
      public float matchCost() {
        return 0;
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        doc++;
        if (doc >= maxDoc) {
          doc = NO_MORE_DOCS;
        }
        flag = true;
        return doc;
      }

      @Override
      public int advance(int target) throws IOException {
        doc = target;
        if (doc >= maxDoc) {
          doc = NO_MORE_DOCS;
        }
        flag = true;
        return doc;
      }

      @Override
      public long cost() {
        return 0;
      }
    };
  }

  @Override
  public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc)
      throws IOException {
    return new IntervalMatchesIterator() {
      boolean next = true;

      @Override
      public int gaps() {
        return 0;
      }

      @Override
      public int width() {
        return 1;
      }

      @Override
      public boolean next() throws IOException {
        if (next) {
          next = false;
          return true;
        }
        return false;
      }

      @Override
      public int startPosition() {
        return 0;
      }

      @Override
      public int endPosition() {
        return 0;
      }

      @Override
      public int startOffset() throws IOException {
        return 0;
      }

      @Override
      public int endOffset() throws IOException {
        return 0;
      }

      @Override
      public MatchesIterator getSubMatches() throws IOException {
        return null;
      }

      @Override
      public Query getQuery() {
        return null;
      }
    };
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {}

  @Override
  public int minExtent() {
    return 0;
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Collections.singleton(this);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    return false;
  }

  @Override
  public String toString() {
    return "";
  }
}
