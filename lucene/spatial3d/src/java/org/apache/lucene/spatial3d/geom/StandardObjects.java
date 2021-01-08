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

package org.apache.lucene.spatial3d.geom;

import java.util.HashMap;
import java.util.Map;

/**
 * Lookup tables for classes that can be serialized using a code.
 *
 * @lucene.internal
 */
class StandardObjects {

  /** Registry of standard classes to corresponding code */
  static Map<Class<?>, Integer> classRegsitry = new HashMap<>();
  /** Registry of codes to corresponding classes */
  static Map<Integer, Class<?>> codeRegsitry = new HashMap<>();

  static {
    classRegsitry.put(GeoPoint.class, 0);
    classRegsitry.put(GeoRectangle.class, 1);
    classRegsitry.put(GeoStandardCircle.class, 2);
    classRegsitry.put(GeoStandardPath.class, 3);
    classRegsitry.put(GeoConvexPolygon.class, 4);
    classRegsitry.put(GeoConcavePolygon.class, 5);
    classRegsitry.put(GeoComplexPolygon.class, 6);
    classRegsitry.put(GeoCompositePolygon.class, 7);
    classRegsitry.put(GeoCompositeMembershipShape.class, 8);
    classRegsitry.put(GeoCompositeAreaShape.class, 9);
    classRegsitry.put(GeoDegeneratePoint.class, 10);
    classRegsitry.put(GeoDegenerateHorizontalLine.class, 11);
    classRegsitry.put(GeoDegenerateLatitudeZone.class, 12);
    classRegsitry.put(GeoDegenerateLongitudeSlice.class, 13);
    classRegsitry.put(GeoDegenerateVerticalLine.class, 14);
    classRegsitry.put(GeoLatitudeZone.class, 15);
    classRegsitry.put(GeoLongitudeSlice.class, 16);
    classRegsitry.put(GeoNorthLatitudeZone.class, 17);
    classRegsitry.put(GeoNorthRectangle.class, 18);
    classRegsitry.put(GeoSouthLatitudeZone.class, 19);
    classRegsitry.put(GeoSouthRectangle.class, 20);
    classRegsitry.put(GeoWideDegenerateHorizontalLine.class, 21);
    classRegsitry.put(GeoWideLongitudeSlice.class, 22);
    classRegsitry.put(GeoWideNorthRectangle.class, 23);
    classRegsitry.put(GeoWideRectangle.class, 24);
    classRegsitry.put(GeoWideSouthRectangle.class, 25);
    classRegsitry.put(GeoWorld.class, 26);
    classRegsitry.put(dXdYdZSolid.class, 27);
    classRegsitry.put(dXdYZSolid.class, 28);
    classRegsitry.put(dXYdZSolid.class, 29);
    classRegsitry.put(dXYZSolid.class, 30);
    classRegsitry.put(XdYdZSolid.class, 31);
    classRegsitry.put(XdYZSolid.class, 32);
    classRegsitry.put(XYdZSolid.class, 33);
    classRegsitry.put(StandardXYZSolid.class, 34);
    classRegsitry.put(PlanetModel.class, 35);
    classRegsitry.put(GeoDegeneratePath.class, 36);
    classRegsitry.put(GeoExactCircle.class, 37);
    classRegsitry.put(GeoS2Shape.class, 38);

    for (Map.Entry<Class<?>, Integer> entry : classRegsitry.entrySet()) {
      codeRegsitry.put(entry.getValue(), entry.getKey());
    }
  }
}
