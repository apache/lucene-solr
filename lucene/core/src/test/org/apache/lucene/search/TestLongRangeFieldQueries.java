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

import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;

/**
 * Random testing for LongRange Queries.
 */
public class TestLongRangeFieldQueries extends BaseRangeFieldQueryTestCase {
  private static final String FIELD_NAME = "longRangeField";

  private long nextLongInternal() {
    if (rarely()) {
      return random().nextBoolean() ? Long.MAX_VALUE : Long.MIN_VALUE;
    }
    long max = Long.MAX_VALUE / 2;
    return (max + max) * random().nextLong() - max;
  }

  @Override
  protected Range nextRange(int dimensions) throws Exception {
    long[] min = new long[dimensions];
    long[] max = new long[dimensions];

    long minV, maxV;
    for (int d=0; d<dimensions; ++d) {
      minV = nextLongInternal();
      maxV = nextLongInternal();
      min[d] = Math.min(minV, maxV);
      max[d] = Math.max(minV, maxV);
    }
    return new LongTestRange(min, max);
  }

  @Override
  protected LongRange newRangeField(Range r) {
    return new LongRange(FIELD_NAME, ((LongTestRange)r).min, ((LongTestRange)r).max);
  }

  @Override
  protected Query newIntersectsQuery(Range r) {
    return LongRange.newIntersectsQuery(FIELD_NAME, ((LongTestRange)r).min, ((LongTestRange)r).max);
  }

  @Override
  protected Query newContainsQuery(Range r) {
    return LongRange.newContainsQuery(FIELD_NAME, ((LongTestRange)r).min, ((LongTestRange)r).max);
  }

  @Override
  protected Query newWithinQuery(Range r) {
    return LongRange.newWithinQuery(FIELD_NAME, ((LongTestRange)r).min, ((LongTestRange)r).max);
  }

  @Override
  protected Query newCrossesQuery(Range r) {
    return LongRange.newCrossesQuery(FIELD_NAME, ((LongTestRange)r).min, ((LongTestRange)r).max);
  }

  /** Basic test */
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // intersects (within)
    Document document = new Document();
    document.add(new LongRange(FIELD_NAME, new long[] {-10, -10}, new long[] {9, 10}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new LongRange(FIELD_NAME, new long[] {10, -10}, new long[] {20, 10}));
    writer.addDocument(document);

    // intersects (contains, crosses)
    document = new Document();
    document.add(new LongRange(FIELD_NAME, new long[] {-20, -20}, new long[] {30, 30}));
    writer.addDocument(document);

    // intersects (within)
    document = new Document();
    document.add(new LongRange(FIELD_NAME, new long[] {-11, -11}, new long[] {1, 11}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new LongRange(FIELD_NAME, new long[] {12, 1}, new long[] {15, 29}));
    writer.addDocument(document);

    // disjoint
    document = new Document();
    document.add(new LongRange(FIELD_NAME, new long[] {-122, 1}, new long[] {-115, 29}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new LongRange(FIELD_NAME, new long[] {Long.MIN_VALUE, 1}, new long[] {-11, 29}));
    writer.addDocument(document);

    // equal (within, contains, intersects)
    document = new Document();
    document.add(new LongRange(FIELD_NAME, new long[] {-11, -15}, new long[] {15, 20}));
    writer.addDocument(document);

    // search
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(7, searcher.count(LongRange.newIntersectsQuery(FIELD_NAME,
        new long[] {-11, -15}, new long[] {15, 20})));
    assertEquals(3, searcher.count(LongRange.newWithinQuery(FIELD_NAME,
        new long[] {-11, -15}, new long[] {15, 20})));
    assertEquals(2, searcher.count(LongRange.newContainsQuery(FIELD_NAME,
        new long[] {-11, -15}, new long[] {15, 20})));
    assertEquals(4, searcher.count(LongRange.newCrossesQuery(FIELD_NAME,
        new long[] {-11, -15}, new long[] {15, 20})));

    reader.close();
    writer.close();
    dir.close();
  }

  /** LongRange test class implementation - use to validate LongRange */
  private class LongTestRange extends Range {
    long[] min;
    long[] max;

    LongTestRange(long[] min, long[] max) {
      assert min != null && max != null && min.length > 0 && max.length > 0
          : "test box: min/max cannot be null or empty";
      assert min.length == max.length : "test box: min/max length do not agree";
      this.min = min;
      this.max = max;
    }

    @Override
    protected int numDimensions() {
      return min.length;
    }

    @Override
    protected Long getMin(int dim) {
      return min[dim];
    }

    @Override
    protected void setMin(int dim, Object val) {
      long v = (Long)val;
      if (min[dim] < v) {
        max[dim] = v;
      } else {
        min[dim] = v;
      }
    }

    @Override
    protected Long getMax(int dim) {
      return max[dim];
    }

    @Override
    protected void setMax(int dim, Object val) {
      long v = (Long)val;
      if (max[dim] > v) {
        min[dim] = v;
      } else {
        max[dim] = v;
      }
    }

    @Override
    protected boolean isEqual(Range other) {
      LongTestRange o = (LongTestRange)other;
      return Arrays.equals(min, o.min) && Arrays.equals(max, o.max);
    }

    @Override
    protected boolean isDisjoint(Range o) {
      LongTestRange other = (LongTestRange)o;
      for (int d=0; d<this.min.length; ++d) {
        if (this.min[d] > other.max[d] || this.max[d] < other.min[d]) {
          // disjoint:
          return true;
        }
      }
      return false;
    }

    @Override
    protected boolean isWithin(Range o) {
      LongTestRange other = (LongTestRange)o;
      for (int d=0; d<this.min.length; ++d) {
        if ((this.min[d] >= other.min[d] && this.max[d] <= other.max[d]) == false) {
          // not within:
          return false;
        }
      }
      return true;
    }

    @Override
    protected boolean contains(Range o) {
      LongTestRange other = (LongTestRange) o;
      for (int d=0; d<this.min.length; ++d) {
        if ((this.min[d] <= other.min[d] && this.max[d] >= other.max[d]) == false) {
          // not contains:
          return false;
        }
      }
      return true;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("Box(");
      b.append(min[0]);
      b.append(" TO ");
      b.append(max[0]);
      for (int d=1; d<min.length; ++d) {
        b.append(", ");
        b.append(min[d]);
        b.append(" TO ");
        b.append(max[d]);
      }
      b.append(")");

      return b.toString();
    }
  }
}
