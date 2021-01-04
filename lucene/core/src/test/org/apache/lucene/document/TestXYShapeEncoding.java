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

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPolygon;

/** tests XYShape encoding */
public class TestXYShapeEncoding extends BaseShapeEncodingTestCase {
  @Override
  protected int encodeX(double x) {
    return XYEncodingUtils.encode((float) x);
  }

  @Override
  protected int encodeY(double y) {
    return XYEncodingUtils.encode((float) y);
  }

  @Override
  protected double decodeX(int xEncoded) {
    return XYEncodingUtils.decode(xEncoded);
  }

  @Override
  protected double decodeY(int yEncoded) {
    return XYEncodingUtils.decode(yEncoded);
  }

  @Override
  protected double nextX() {
    return ShapeTestUtil.nextFloat(random());
  }

  @Override
  protected double nextY() {
    return ShapeTestUtil.nextFloat(random());
  }

  @Override
  protected XYPolygon nextPolygon() {
    return ShapeTestUtil.nextPolygon();
  }

  @Override
  protected Component2D createPolygon2D(Object polygon) {
    return XYGeometry.create((XYPolygon)polygon);
  }

  public void testRotationChangesOrientation() {
    double ay = -3.4028218437925203E38;
    double ax = 3.4028220466166163E38;
    double by = 3.4028218437925203E38;
    double bx = -3.4028218437925203E38;
    double cy = 3.4028230607370965E38;
    double cx = -3.4028230607370965E38;
    verifyEncoding(ay, ax, by, bx, cy, cx);
  }
}
