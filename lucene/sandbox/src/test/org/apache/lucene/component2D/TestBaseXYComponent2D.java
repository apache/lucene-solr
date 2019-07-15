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

import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.PointValues;

public abstract class TestBaseXYComponent2D extends TestBaseComponent2D {

  @Override
  protected Component2D getComponentInside(Component2D component) {
    for (int i =0; i < 500; i++) {
      XYRectangle rectangle = ShapeTestUtil.nextBox();
      // allowed to conservatively return false
      if (component.relate(XYEncodingUtils.encode(rectangle.minX), XYEncodingUtils.encode(rectangle.maxX),
          XYEncodingUtils.encode(rectangle.minY), XYEncodingUtils.encode(rectangle.maxY)) == PointValues.Relation.CELL_INSIDE_QUERY) {
        return XYComponent2DFactory.create(rectangle);
      }
    }
    return null;
  }

  @Override
  protected int nextEncodedX() {
    return XYEncodingUtils.encode(ShapeTestUtil.nextDouble());
  }

  @Override
  protected int nextEncodedY() {
    return XYEncodingUtils.encode(ShapeTestUtil.nextDouble());
  }

}
