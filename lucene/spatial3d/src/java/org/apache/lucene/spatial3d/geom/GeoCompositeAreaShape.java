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

import java.io.IOException;
import java.io.InputStream;

/**
 * GeoCompositeAreaShape is a set of GeoAreaShape's, treated as a unit.
 *
 * @lucene.experimental
 */
public class GeoCompositeAreaShape extends GeoBaseCompositeAreaShape<GeoAreaShape> {

  /** Constructor. */
  public GeoCompositeAreaShape(PlanetModel planetModel) {
    super(planetModel);
  }

  /**
   * Constructor for deserialization.
   *
   * @param planetModel is the planet model.
   * @param inputStream is the input stream.
   */
  public GeoCompositeAreaShape(final PlanetModel planetModel, final InputStream inputStream)
      throws IOException {
    super(planetModel, inputStream, GeoAreaShape.class);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeoCompositeAreaShape)) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public String toString() {
    return "GeoCompositeAreaShape: {" + shapes + '}';
  }
}
