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
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;

/**
 * Random testing for FloatRange Queries.
 */
public class TestFloatRangeFieldQueries extends BaseRangeFieldQueryTestCase {
  private static final String FIELD_NAME = "floatRangeField";

  private float nextFloatInternal() {
    if (rarely()) {
      return random().nextBoolean() ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
    }
    float max = Float.MAX_VALUE / 2;
    return (max + max) * random().nextFloat() - max;
  }

  @Override
  protected Range nextRange(int dimensions) throws Exception {
    float[] min = new float[dimensions];
    float[] max = new float[dimensions];

    float minV, maxV;
    for (int d=0; d<dimensions; ++d) {
      minV = nextFloatInternal();
      maxV = nextFloatInternal();
      min[d] = Math.min(minV, maxV);
      max[d] = Math.max(minV, maxV);
    }
    return new FloatTestRange(min, max);
  }

  @Override
  protected FloatRange newRangeField(Range r) {
    return new FloatRange(FIELD_NAME, ((FloatTestRange)r).min, ((FloatTestRange)r).max);
  }

  @Override
  protected Query newIntersectsQuery(Range r) {
    return FloatRange.newIntersectsQuery(FIELD_NAME, ((FloatTestRange)r).min, ((FloatTestRange)r).max);
  }

  @Override
  protected Query newContainsQuery(Range r) {
    return FloatRange.newContainsQuery(FIELD_NAME, ((FloatTestRange)r).min, ((FloatTestRange)r).max);
  }

  @Override
  protected Query newWithinQuery(Range r) {
    return FloatRange.newWithinQuery(FIELD_NAME, ((FloatTestRange)r).min, ((FloatTestRange)r).max);
  }

  @Override
  protected Query newCrossesQuery(Range r) {
    return FloatRange.newCrossesQuery(FIELD_NAME, ((FloatTestRange)r).min, ((FloatTestRange)r).max);
  }

  /** Basic test */
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // intersects (within)
    Document document = new Document();
    document.add(new FloatRange(FIELD_NAME, new float[] {-10.0f, -10.0f}, new float[] {9.1f, 10.1f}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new FloatRange(FIELD_NAME, new float[] {10.0f, -10.0f}, new float[] {20.0f, 10.0f}));
    writer.addDocument(document);

    // intersects (contains, crosses)
    document = new Document();
    document.add(new FloatRange(FIELD_NAME, new float[] {-20.0f, -20.0f}, new float[] {30.0f, 30.1f}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new FloatRange(FIELD_NAME, new float[] {-11.1f, -11.2f}, new float[] {1.23f, 11.5f}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new FloatRange(FIELD_NAME, new float[] {12.33f, 1.2f}, new float[] {15.1f, 29.9f}));
    writer.addDocument(document);

    // disjoint
    document = new Document();
    document.add(new FloatRange(FIELD_NAME, new float[] {-122.33f, 1.2f}, new float[] {-115.1f, 29.9f}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new FloatRange(FIELD_NAME, new float[] {Float.NEGATIVE_INFINITY, 1.2f}, new float[] {-11.0f, 29.9f}));
    writer.addDocument(document);

    // equal (within, contains, intersects)
    document = new Document();
    document.add(new FloatRange(FIELD_NAME, new float[] {-11f, -15f}, new float[] {15f, 20f}));
    writer.addDocument(document);

    // search
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(7, searcher.count(FloatRange.newIntersectsQuery(FIELD_NAME,
        new float[] {-11.0f, -15.0f}, new float[] {15.0f, 20.0f})));
    assertEquals(2, searcher.count(FloatRange.newWithinQuery(FIELD_NAME,
        new float[] {-11.0f, -15.0f}, new float[] {15.0f, 20.0f})));
    assertEquals(2, searcher.count(FloatRange.newContainsQuery(FIELD_NAME,
        new float[] {-11.0f, -15.0f}, new float[] {15.0f, 20.0f})));
    assertEquals(5, searcher.count(FloatRange.newCrossesQuery(FIELD_NAME,
        new float[] {-11.0f, -15.0f}, new float[] {15.0f, 20.0f})));

    reader.close();
    writer.close();
    dir.close();
  }

  /** FloatRange test class implementation - use to validate FloatRange */
  private class FloatTestRange extends Range {
    float[] min;
    float[] max;

    FloatTestRange(float[] min, float[] max) {
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
    protected Float getMin(int dim) {
      return min[dim];
    }

    @Override
    protected void setMin(int dim, Object val) {
      float v = (Float)val;
      if (min[dim] < v) {
        max[dim] = v;
      } else {
        min[dim] = v;
      }
    }

    @Override
    protected Float getMax(int dim) {
      return max[dim];
    }

    @Override
    protected void setMax(int dim, Object val) {
      float v = (Float)val;
      if (max[dim] > v) {
        min[dim] = v;
      } else {
        max[dim] = v;
      }
    }

    @Override
    protected boolean isEqual(Range other) {
      FloatTestRange o = (FloatTestRange)other;
      return Arrays.equals(min, o.min) && Arrays.equals(max, o.max);
    }

    @Override
    protected boolean isDisjoint(Range o) {
      FloatTestRange other = (FloatTestRange)o;
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
      FloatTestRange other = (FloatTestRange)o;
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
      FloatTestRange other = (FloatTestRange) o;
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
