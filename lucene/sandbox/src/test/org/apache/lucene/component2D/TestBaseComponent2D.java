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

package org.apache.lucene.component2D;

import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.LuceneTestCase;

public abstract class TestBaseComponent2D extends LuceneTestCase {

  protected abstract Object nextShape();

  protected abstract Component2D getComponent(Object shape);

  protected abstract Component2D getComponentInside(Component2D component);

  protected abstract int nextEncodedX();

  protected abstract int nextEncodedY();

  public void testEqualsAndHashcode() {
    Object shape = nextShape();
    Component2D component1 = getComponent(shape);
    Component2D component2 = getComponent(shape);
    assertEquals(component1, component2);
    assertEquals(component1.hashCode(), component2.hashCode());
    Object otherShape =  nextShape();
    // shapes can be different but equal in the encoded space
    Component2D component3 = getComponent(otherShape);
    if (component1.equals(component3)) {
      assertEquals(component1.hashCode(), component3.hashCode());
    } else {
      assertNotEquals(component1.hashCode(), component3.hashCode());
    }
  }

  public void testRandomContains() {
    Object rectangle = nextShape();
    for (int i = 0; i < 5; i++) {
      Component2D component = getComponent(rectangle);
      Component2D insideComponent = getComponentInside(component);
      if (insideComponent == null) {
        continue;
      }
      for (int j = 0; j < 500; j++) {
        int x = nextEncodedX();
        int y = nextEncodedY();
        if (insideComponent.contains(x, y)) {
          assertTrue(component.contains(x, y));
          assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
        }
      }
    }
  }

  public void testRandomTriangles() {
    Object rectangle = nextShape();
    Component2D component = getComponent(rectangle);

    for (int i =0; i < 100; i++) {
      int ax = nextEncodedX();
      int ay = nextEncodedY();
      int bx = nextEncodedX();
      int by = nextEncodedY();
      int cx = nextEncodedX();
      int cy = nextEncodedY();

      int tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      int tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      int tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      int tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);

      PointValues.Relation r = component.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(ax, ay, bx, by, cx, cy));
        assertFalse(component.contains(ax, ay));
        assertFalse(component.contains(bx, by));
        assertFalse(component.contains(cx, cy));
      }
      else if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(ax, ay, bx, by, cx, cy));
        assertTrue(component.contains(ax, ay));
        assertTrue(component.contains(bx, by));
        assertTrue(component.contains(cx, cy));
      }
    }
  }

  public void testComponentPredicate() {
    Object shape = nextShape();
    Component2D component = getComponent(shape);
    Component2DPredicate predicate = Component2DPredicate.createComponentPredicate(component);
    for (int i =0; i < 1000; i++) {
      int x = nextEncodedX();
      int y = nextEncodedY();
      assertEquals(component.contains(x, y), predicate.test(x, y));
      if (component.contains(x, y)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      } else {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      }
    }
  }
}
