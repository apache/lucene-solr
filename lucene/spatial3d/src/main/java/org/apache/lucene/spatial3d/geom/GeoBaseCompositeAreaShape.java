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

import java.io.InputStream;
import java.io.IOException;

/**
 * Base class to create a composite of GeoAreaShapes
 *
 * @param <T> is the type of GeoAreaShapes of the composite.
 * @lucene.internal
 */
abstract class GeoBaseCompositeAreaShape<T extends GeoAreaShape> extends GeoBaseCompositeMembershipShape<T> implements GeoAreaShape {

  /** All edgepoints inside shape */
  protected final static int ALL_INSIDE = 0;
  /** Some edgepoints inside shape */
  protected final static int SOME_INSIDE = 1;
  /** No edgepoints inside shape */
  protected final static int NONE_INSIDE = 2;

  /**
   * Constructor.
   */
  public GeoBaseCompositeAreaShape(PlanetModel planetModel) {
    super(planetModel);
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   * @param clazz is the class of the generic.
   */
  public GeoBaseCompositeAreaShape(final PlanetModel planetModel, final InputStream inputStream, final Class<T> clazz) throws IOException {
    super(planetModel, inputStream, clazz);
  }

  @Override
  public boolean intersects(GeoShape geoShape){
    for(GeoAreaShape geoAreaShape : shapes){
      if (geoAreaShape.intersects(geoShape)){
        return true;
      }
    }
    return false;
  }

  @Override
  public int getRelationship(GeoShape geoShape) {
    if (!geoShape.getPlanetModel().equals(planetModel)) {
      throw new IllegalArgumentException("Cannot relate shapes with different planet models.");
    }
    final int insideGeoAreaShape = isShapeInsideGeoAreaShape(geoShape);
    if (insideGeoAreaShape == SOME_INSIDE) {
      return GeoArea.OVERLAPS;
    }

    final int insideShape = isGeoAreaShapeInsideShape(geoShape);
    if (insideShape == SOME_INSIDE) {
      return GeoArea.OVERLAPS;
    }

    if (insideGeoAreaShape == ALL_INSIDE && insideShape==ALL_INSIDE) {
      return GeoArea.OVERLAPS;
    }

    if (intersects(geoShape)){
      return  GeoArea.OVERLAPS;
    }

    if (insideGeoAreaShape == ALL_INSIDE) {
      return GeoArea.WITHIN;
    }

    if (insideShape==ALL_INSIDE) {
      return GeoArea.CONTAINS;
    }

    return GeoArea.DISJOINT;
  }

  /** Determine the relationship between the GeoAreShape and the
   * shape's edgepoints.
   *@param geoShape is the shape.
   *@return the relationship.
   */
  protected  int isShapeInsideGeoAreaShape(final GeoShape geoShape) {
    boolean foundOutside = false;
    boolean foundInside = false;
    for (GeoPoint p : geoShape.getEdgePoints()) {
      if (isWithin(p)) {
        foundInside = true;
      } else {
        foundOutside = true;
      }
      if (foundInside && foundOutside) {
        return SOME_INSIDE;
      }
    }
    if (!foundInside && !foundOutside)
      return NONE_INSIDE;
    if (foundInside && !foundOutside)
      return ALL_INSIDE;
    if (foundOutside && !foundInside)
      return NONE_INSIDE;
    return SOME_INSIDE;
  }

  /** Determine the relationship between the GeoAreShape's edgepoints and the
   * provided shape.
   *@param geoshape is the shape.
   *@return the relationship.
   */
  protected int isGeoAreaShapeInsideShape(final GeoShape geoshape)  {
    boolean foundOutside = false;
    boolean foundInside = false;
    for (GeoPoint p : getEdgePoints()) {
      if (geoshape.isWithin(p)) {
        foundInside = true;
      } else {
        foundOutside = true;
      }
      if (foundInside && foundOutside) {
        return SOME_INSIDE;
      }
    }
    if (!foundInside && !foundOutside)
      return NONE_INSIDE;
    if (foundInside && !foundOutside)
      return ALL_INSIDE;
    if (foundOutside && !foundInside)
      return NONE_INSIDE;
    return SOME_INSIDE;
  }
}
