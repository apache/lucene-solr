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
import org.apache.lucene.document.IntRange;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;

/**
 * Random testing for IntRange Queries.
 */
public class TestIntRangeFieldQueries extends BaseRangeFieldQueryTestCase {
  private static final String FIELD_NAME = "intRangeField";

  private int nextIntInternal() {
    switch (random().nextInt(5)) {
      case 0:
        return Integer.MIN_VALUE;
      case 1:
        return Integer.MAX_VALUE;
      default:
        int bpv = random().nextInt(32);
        switch (bpv) {
          case 32:
            return random().nextInt();
          default:
            int v = TestUtil.nextInt(random(), 0, (1 << bpv) - 1);
            if (bpv > 0) {
              // negative values sometimes
              v -= 1 << (bpv - 1);
            }
            return v;
        }
    }
  }

  @Override
  protected Range nextRange(int dimensions) throws Exception {
    int[] min = new int[dimensions];
    int[] max = new int[dimensions];

    int minV, maxV;
    for (int d=0; d<dimensions; ++d) {
      minV = nextIntInternal();
      maxV = nextIntInternal();
      min[d] = Math.min(minV, maxV);
      max[d] = Math.max(minV, maxV);
    }
    return new IntTestRange(min, max);
  }

  @Override
  protected org.apache.lucene.document.IntRange newRangeField(Range r) {
    return new IntRange(FIELD_NAME, ((IntTestRange)r).min, ((IntTestRange)r).max);
  }

  @Override
  protected Query newIntersectsQuery(Range r) {
    return IntRange.newIntersectsQuery(FIELD_NAME, ((IntTestRange)r).min, ((IntTestRange)r).max);
  }

  @Override
  protected Query newContainsQuery(Range r) {
    return IntRange.newContainsQuery(FIELD_NAME, ((IntTestRange)r).min, ((IntTestRange)r).max);
  }

  @Override
  protected Query newWithinQuery(Range r) {
    return IntRange.newWithinQuery(FIELD_NAME, ((IntTestRange)r).min, ((IntTestRange)r).max);
  }

  @Override
  protected Query newCrossesQuery(Range r) {
    return IntRange.newCrossesQuery(FIELD_NAME, ((IntTestRange)r).min, ((IntTestRange)r).max);
  }

  /** Basic test */
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // intersects (within)
    Document document = new Document();
    document.add(new IntRange(FIELD_NAME, new int[] {-10, -10}, new int[] {9, 10}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new IntRange(FIELD_NAME, new int[] {10, -10}, new int[] {20, 10}));
    writer.addDocument(document);

    // intersects (contains / crosses)
    document = new Document();
    document.add(new IntRange(FIELD_NAME, new int[] {-20, -20}, new int[] {30, 30}));
    writer.addDocument(document);

    // intersects (within)
    document = new Document();
    document.add(new IntRange(FIELD_NAME, new int[] {-11, -11}, new int[] {1, 11}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new IntRange(FIELD_NAME, new int[] {12, 1}, new int[] {15, 29}));
    writer.addDocument(document);

    // disjoint
    document = new Document();
    document.add(new IntRange(FIELD_NAME, new int[] {-122, 1}, new int[] {-115, 29}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new IntRange(FIELD_NAME, new int[] {Integer.MIN_VALUE, 1}, new int[] {-11, 29}));
    writer.addDocument(document);

    // equal (within, contains, intersects)
    document = new Document();
    document.add(new IntRange(FIELD_NAME, new int[] {-11, -15}, new int[] {15, 20}));
    writer.addDocument(document);

    // search
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(7, searcher.count(IntRange.newIntersectsQuery(FIELD_NAME,
        new int[] {-11, -15}, new int[] {15, 20})));
    assertEquals(3, searcher.count(IntRange.newWithinQuery(FIELD_NAME,
        new int[] {-11, -15}, new int[] {15, 20})));
    assertEquals(2, searcher.count(IntRange.newContainsQuery(FIELD_NAME,
        new int[] {-11, -15}, new int[] {15, 20})));
    assertEquals(4, searcher.count(IntRange.newCrossesQuery(FIELD_NAME,
        new int[] {-11, -15}, new int[] {15, 20})));

    reader.close();
    writer.close();
    dir.close();
  }

  /** IntRange test class implementation - use to validate IntRange */
  protected class IntTestRange extends Range {
    int[] min;
    int[] max;

    IntTestRange(int[] min, int[] max) {
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
    protected Integer getMin(int dim) {
      return min[dim];
    }

    @Override
    protected void setMin(int dim, Object val) {
      int v = (Integer)val;
      if (min[dim] < v) {
        max[dim] = v;
      } else {
        min[dim] = v;
      }
    }

    @Override
    protected Integer getMax(int dim) {
      return max[dim];
    }

    @Override
    protected void setMax(int dim, Object val) {
      int v = (Integer)val;
      if (max[dim] > v) {
        min[dim] = v;
      } else {
        max[dim] = v;
      }
    }

    @Override
    protected boolean isEqual(Range other) {
      IntTestRange o = (IntTestRange)other;
      return Arrays.equals(min, o.min) && Arrays.equals(max, o.max);
    }

    @Override
    protected boolean isDisjoint(Range o) {
      IntTestRange other = (IntTestRange)o;
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
      IntTestRange other = (IntTestRange)o;
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
      IntTestRange other = (IntTestRange) o;
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
