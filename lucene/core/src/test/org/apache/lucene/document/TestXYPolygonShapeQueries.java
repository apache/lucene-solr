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
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;

/** random cartesian bounding box, line, and polygon query tests for random indexed {@link XYPolygon} types */
public class TestXYPolygonShapeQueries extends BaseXYShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected XYPolygon nextShape() {
    XYPolygon p;
    while (true) {
      // if we can't tessellate; then random polygon generator created a malformed shape
      p = (XYPolygon)getShapeType().nextShape();
      try {
        Tessellator.tessellate(p);
        return p;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
  }

  @Override
  protected Field[] createIndexableFields(String field, Object polygon) {
    return XYShape.createIndexableFields(field, (XYPolygon)polygon);
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
    public boolean testBBoxQuery(double minY, double maxY, double minX, double maxX, Object shape) {
      Component2D rectangle2D = XYGeometry.create(new XYRectangle((float) minX, (float) maxX, (float) minY, (float) maxY));
      return testComponentQuery(rectangle2D, shape);
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object o) {
      XYPolygon polygon = (XYPolygon) o;
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinQuery(query, XYShape.createIndexableFields("dummy", polygon)) == Component2D.WithinRelation.CANDIDATE;
      }
      return testComponentQuery(query, XYShape.createIndexableFields("dummy", polygon));
    }
  }

  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(25000);
  }

}
