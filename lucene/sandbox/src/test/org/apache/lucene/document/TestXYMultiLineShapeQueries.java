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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYLine;

/** random cartesian bounding box, line, and polygon query tests for random indexed arrays of cartesian {@link XYLine} types */
public class TestXYMultiLineShapeQueries extends BaseXYShapeTestCase {
  @Override
  protected ShapeType getShapeType() {
    return ShapeType.LINE;
  }

  @Override
  protected XYLine[] nextShape() {
    int n = random().nextInt(4) + 1;
    XYLine[] lines = new XYLine[n];
    for (int i =0; i < n; i++) {
      lines[i] = nextLine();
    }
    return lines;
  }

  @Override
  protected Field[] createIndexableFields(String name, Object o) {
    XYLine[] lines = (XYLine[]) o;
    List<Field> allFields = new ArrayList<>();
    for (XYLine line : lines) {
      Field[] fields = XYShape.createIndexableFields(name, line);
      for (Field field : fields) {
        allFields.add(field);
      }
    }
    return allFields.toArray(new Field[allFields.size()]);
  }

  @Override
  public Validator getValidator() {
    return new MultiLineValidator(ENCODER);
  }

  protected class MultiLineValidator extends Validator {
    TestXYLineShapeQueries.LineValidator LINEVALIDATOR;
    MultiLineValidator(Encoder encoder) {
      super(encoder);
      LINEVALIDATOR = new TestXYLineShapeQueries.LineValidator(encoder);
    }

    @Override
    public Validator setRelation(QueryRelation relation) {
      super.setRelation(relation);
      LINEVALIDATOR.queryRelation = relation;
      return this;
    }

    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      XYLine[] lines = (XYLine[])shape;
      for (XYLine l : lines) {
        boolean b = LINEVALIDATOR.testBBoxQuery(minLat, maxLat, minLon, maxLon, l);
        if (b == true && queryRelation == ShapeField.QueryRelation.INTERSECTS) {
          return true;
        } else if (b == false && queryRelation == ShapeField.QueryRelation.DISJOINT) {
          return false;
        } else if (b == false && queryRelation == ShapeField.QueryRelation.WITHIN) {
          return false;
        }
      }
      return queryRelation != ShapeField.QueryRelation.INTERSECTS;
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object shape) {
      XYLine[] lines = (XYLine[])shape;
      for (XYLine l : lines) {
        boolean b = LINEVALIDATOR.testComponentQuery(query, l);
        if (b == true && queryRelation == ShapeField.QueryRelation.INTERSECTS) {
          return true;
        } else if (b == false && queryRelation == ShapeField.QueryRelation.DISJOINT) {
          return false;
        } else if (b == false && queryRelation == ShapeField.QueryRelation.WITHIN) {
          return false;
        }
      }
      return queryRelation != ShapeField.QueryRelation.INTERSECTS;
    }
  }

  @Slow
  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(10000);
  }
}
