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
package org.apache.lucene.document;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Point;

/**
 * random bounding box, line, and polygon query tests for random indexed arrays of {@code latitude,
 * longitude} points
 */
public class TestLatLonPointPointQueries extends BaseLatLonPointTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POINT;
  }

  @Override
  protected Validator getValidator() {
    return new TestLatLonPointShapeQueries.PointValidator(this.ENCODER);
  }

  @Override
  protected Field[] createIndexableFields(String name, Object o) {
    Point point = (Point) o;
    return new Field[] {new LatLonPoint(FIELD_NAME, point.getLat(), point.getLon())};
  }

  protected static class PointValidator extends Validator {
    protected PointValidator(Encoder encoder) {
      super(encoder);
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object shape) {
      Point p = (Point) shape;
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinQuery(
                query, LatLonShape.createIndexableFields("dummy", p.getLat(), p.getLon()))
            == Component2D.WithinRelation.CANDIDATE;
      }
      return testComponentQuery(
          query, LatLonShape.createIndexableFields("dummy", p.getLat(), p.getLon()));
    }
  }

  @Slow
  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(10000);
  }
}
