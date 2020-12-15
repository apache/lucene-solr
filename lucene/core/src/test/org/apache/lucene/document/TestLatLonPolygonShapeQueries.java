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
import org.apache.lucene.geo.Polygon;

/** random bounding box, line, and polygon query tests for random indexed {@link Polygon} types */
public class TestLatLonPolygonShapeQueries extends BaseLatLonShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected Field[] createIndexableFields(String field, Object polygon) {
    return LatLonShape.createIndexableFields(field, (Polygon)polygon);
  }

  @Override
  protected Validator getValidator() {
    return new PolygonValidator(this.ENCODER);
  }

  protected static class PolygonValidator extends Validator {
    protected PolygonValidator(Encoder encoder) {
      super(encoder);
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object o) {
      Polygon polygon = (Polygon) o;
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinQuery(query, LatLonShape.createIndexableFields("dummy", polygon)) == Component2D.WithinRelation.CANDIDATE;
      }
      return testComponentQuery(query, LatLonShape.createIndexableFields("dummy", polygon));
    }

    protected Component2D.WithinRelation testWithinPolygon(Component2D query, Polygon polygon) {
      return testWithinQuery(query, LatLonShape.createIndexableFields("dummy", polygon));
    }
  }

  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(25000);
  }
}
