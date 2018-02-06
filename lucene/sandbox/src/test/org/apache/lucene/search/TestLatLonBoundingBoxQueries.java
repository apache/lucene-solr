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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonBoundingBox;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/** Random testing for GeoBoundingBoxField type. */
public class TestLatLonBoundingBoxQueries extends BaseRangeFieldQueryTestCase {
  private static final String FIELD_NAME = "geoBoundingBoxField";

  @Override
  protected LatLonBoundingBox newRangeField(Range r) {
    // addRange is called instead of this method
    throw new UnsupportedOperationException("this method should never be called");
  }

  @Override
  protected void addRange(Document doc, Range r) {
    GeoBBox b = (GeoBBox)r;
    doc.add(new LatLonBoundingBox(FIELD_NAME, b.minLat, b.minLon, b.maxLat, b.maxLon));
  }

  /** Basic test for 2d boxes */
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Shared meridian test (disjoint)
    Document document = new Document();
    document.add(new LatLonBoundingBox(FIELD_NAME, -20d, -180d, 20d, -100d));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new LatLonBoundingBox(FIELD_NAME, 0d, 14.096488952636719d, 10d, 20d));
    writer.addDocument(document);

    // intersects (contains, crosses)
    document = new Document();
    document.add(new LatLonBoundingBox(FIELD_NAME, -10.282592503353953d, -1d, 1d, 14.096488952636719d));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new LatLonBoundingBox(FIELD_NAME, -1d, -11d, 1d, 1d));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new LatLonBoundingBox(FIELD_NAME, -1d, 14.096488952636719d, 5d, 30d));
    writer.addDocument(document);

    // intersects (within)
    document = new Document();
    document.add(new LatLonBoundingBox(FIELD_NAME, -5d, 0d, -1d, 14.096488952636719d));
    writer.addDocument(document);

    // search
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(5, searcher.count(LatLonBoundingBox.newIntersectsQuery(FIELD_NAME,
        -10.282592503353953d, 0.0d, 0.0d, 14.096488952636719d)));
    assertEquals(1, searcher.count(LatLonBoundingBox.newWithinQuery(FIELD_NAME,
        -10.282592503353953d, 0.0d, 0.0d, 14.096488952636719d)));
    assertEquals(1, searcher.count(LatLonBoundingBox.newContainsQuery(FIELD_NAME,
        -10.282592503353953d, 0.0d, 0.0d, 14.096488952636719d)));
    assertEquals(4, searcher.count(LatLonBoundingBox.newCrossesQuery(FIELD_NAME,
        -10.282592503353953d, 0.0d, 0.0d, 14.096488952636719d)));

    reader.close();
    writer.close();
    dir.close();
  }

  public void testToString() {
    LatLonBoundingBox field = new LatLonBoundingBox(FIELD_NAME, -20d, -180d, 20d, -100d);
    String expected = "LatLonBoundingBox <geoBoundingBoxField:[-20.000000023283064,-180.0,19.99999998137355,-100.0000000745058]>";
    assertEquals(expected, field.toString());
  }

  @Override
  protected int dimension() {
    return 2;
  }

  @Override
  protected Range nextRange(int dimensions) {
    // create a random bounding box in 2 dimensions
    return new GeoBBox(dimensions);
  }

  @Override
  protected Query newIntersectsQuery(Range r) {
    GeoBBox b = (GeoBBox)r;
    return LatLonBoundingBox.newIntersectsQuery(FIELD_NAME, b.minLat, b.minLon, b.maxLat, b.maxLon);
  }

  @Override
  protected Query newContainsQuery(Range r) {
    GeoBBox b = (GeoBBox)r;
    return LatLonBoundingBox.newContainsQuery(FIELD_NAME, b.minLat, b.minLon, b.maxLat, b.maxLon);
  }

  @Override
  protected Query newWithinQuery(Range r) {
    GeoBBox b = (GeoBBox)r;
    return LatLonBoundingBox.newWithinQuery(FIELD_NAME, b.minLat, b.minLon, b.maxLat, b.maxLon);
  }

  @Override
  protected Query newCrossesQuery(Range r) {
    GeoBBox b = (GeoBBox)r;
    return LatLonBoundingBox.newCrossesQuery(FIELD_NAME, b.minLat, b.minLon, b.maxLat, b.maxLon);
  }

  protected static class GeoBBox extends Range {
    protected double minLat, minLon, maxLat, maxLon;
    protected int dimension;

    GeoBBox(int dimension) {
      this.dimension = dimension;
      final Rectangle box = GeoTestUtil.nextBoxNotCrossingDateline();
      minLat = quantizeLat(box.minLat);
      minLon = quantizeLon(box.minLon);
      maxLat = quantizeLat(box.maxLat);
      maxLon = quantizeLon(box.maxLon);

//      minLat = quantizeLat(Math.min(box.minLat, box.maxLat));
//      minLon = quantizeLon(Math.max(box.minLat, box.maxLat));
//      maxLat = quantizeLat(box.maxLat);
//      maxLon = quantizeLon(box.maxLon);

//      if (maxLon == -180d) {
//        // index and search handle this fine, but the test validator
//        // struggles when maxLon == -180; so lets correct
//        maxLon = 180d;
//      }
    }

    protected static double quantizeLat(double lat) {
      return decodeLatitude(encodeLatitude(lat));
    }

    protected double quantizeLon(double lon) {
      return decodeLongitude(encodeLongitude(lon));
    }

    @Override
    protected int numDimensions() {
      return dimension;
    }

    @Override
    protected Double getMin(int dim) {
      if (dim == 0) {
        return minLat;
      } else if (dim == 1) {
        return minLon;
      }
      throw new IndexOutOfBoundsException("dimension " + dim + " is greater than " + dimension);
    }

    @Override
    protected void setMin(int dim, Object val) {
      if (dim == 0) {
        setMinLat((Double)val);
      } else if (dim == 1) {
        setMinLon((Double)val);
      } else {
        throw new IndexOutOfBoundsException("dimension " + dim + " is greater than " + dimension);
      }
    }

    private void setMinLat(double d) {
      if (d > maxLat) {
        minLat = maxLat;
        maxLat = d;
      } else {
        minLat = d;
      }
    }

    private void setMinLon(double d) {
      if (d > maxLon) {
        minLon = maxLon;
        maxLon = d;
      } else {
        minLon = d;
      }
    }

    private void setMaxLat(double d) {
      if (d < minLat) {
        maxLat = minLat;
        minLat = d;
      } else {
        maxLat = d;
      }
    }

    private void setMaxLon(double d) {
      if (d < minLon) {
        maxLon = minLon;
        minLon = d;
      } else {
        maxLon = d;
      }
    }

    @Override
    protected Double getMax(int dim) {
      if (dim == 0) {
        return maxLat;
      } else if (dim == 1) {
        return maxLon;
      }
      throw new IndexOutOfBoundsException("dimension " + dim + " is greater than " + dimension);
    }

    @Override
    protected void setMax(int dim, Object val) {
      if (dim == 0) {
        setMaxLat((Double)val);
      } else if (dim == 1) {
        setMaxLon((Double)val);
      } else {
        throw new IndexOutOfBoundsException("dimension " + dim + " is greater than " + dimension);
      }
    }

    @Override
    protected boolean isEqual(Range other) {
      GeoBBox o = (GeoBBox)other;
      if (this.dimension != o.dimension) return false;
      if (this.minLat != o.minLat) return false;
      if (this.minLon != o.minLon) return false;
      if (this.maxLat != o.maxLat) return false;
      if (this.maxLon != o.maxLon) return false;
      return true;
    }

    @Override
    protected boolean isDisjoint(Range other) {
      GeoBBox o = (GeoBBox)other;
      if (minLat > o.maxLat || maxLat < o.minLat) return true;
      if (minLon > o.maxLon || maxLon < o.minLon) return true;
      return false;
    }

    @Override
    protected boolean isWithin(Range other) {
      GeoBBox o = (GeoBBox)other;
      return o.contains(this);
    }

    @Override
    protected boolean contains(Range other) {
      GeoBBox o = (GeoBBox)other;
      if (minLat > o.minLat || maxLat < o.maxLat) return false;
      if (minLon > o.minLon || maxLon < o.maxLon) return false;
      return true;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("GeoBoundingBox(lat: ");
      b.append(minLat);
      b.append(" TO ");
      b.append(maxLat);
      b.append(", lon: ");
      b.append(minLon);
      b.append(" TO ");
      b.append(maxLon);
      b.append(")");

      return b.toString();
    }
  }
}
