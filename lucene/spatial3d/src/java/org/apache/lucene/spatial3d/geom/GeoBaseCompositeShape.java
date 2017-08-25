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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * Base class to create a composite of GeoShapes.
 *
 * @param <T> is the type of GeoShapes of the composite.
 * @lucene.experimental
 */
public abstract class GeoBaseCompositeShape<T extends GeoShape> extends BasePlanetObject implements GeoShape {

  /**
   * Shape's container
   */
  protected final List<T> shapes = new ArrayList<>();

  /**
   * Constructor.
   */
  public GeoBaseCompositeShape(PlanetModel planetModel) {
    super(planetModel);
  }

  /**
   * Add a shape to the composite.
   *
   * @param shape is the shape to add.
   */
  public void addShape(final T shape) {
    if (!shape.getPlanetModel().equals(planetModel)) {
      throw new IllegalArgumentException("Cannot add a shape into a composite with different planet models.");
    }
    shapes.add(shape);
  }

  /**
   * Get the number of shapes in the composite
   *
   * @return the number of shapes
   */
  public int size() {
    return shapes.size();
  }

  /**
   * Get shape at index
   *
   * @return the shape at given index
   */
  public T getShape(int index) {
    return shapes.get(index);
  }

  /**
   * Constructor for deserialization.
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   * @param clazz is the class of the generic.
   */
  public GeoBaseCompositeShape(final PlanetModel planetModel, final InputStream inputStream, final Class<T> clazz) throws IOException {
    this(planetModel);
    final T[] array = SerializableObject.readHeterogeneousArray(planetModel, inputStream, clazz);
    for (final SerializableObject member : array) {
      addShape(clazz.cast(member));
    }
  }

  @Override
  public void write(final OutputStream outputStream) throws IOException {
    SerializableObject.writeHeterogeneousArray(outputStream, shapes);
  }

  @Override
  public boolean isWithin(final Vector point) {
    return isWithin(point.x, point.y, point.z);
  }

  @Override
  public boolean isWithin(final double x, final double y, final double z) {
    for (GeoShape shape : shapes) {
      if (shape.isWithin(x, y, z))
        return true;
    }
    return false;
  }

  @Override
  public GeoPoint[] getEdgePoints() {
    List<GeoPoint> edgePoints = new ArrayList<>();
    for (GeoShape shape : shapes) {
      edgePoints.addAll(Arrays.asList(shape.getEdgePoints()));
    }
    return edgePoints.toArray(new GeoPoint[edgePoints.size()]);
  }

  @Override
  public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
    for (GeoShape shape : shapes) {
      if (shape.intersects(p, notablePoints, bounds))
        return true;
    }
    return false;
  }

  @Override
  public void getBounds(Bounds bounds) {
    for (GeoShape shape : shapes) {
      shape.getBounds(bounds);
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode() + shapes.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoBaseCompositeShape<?>))
      return false;
    GeoBaseCompositeShape<?> other = (GeoBaseCompositeShape<?>) o;
    return super.equals(other) && shapes.equals(other.shapes);
  }
}
