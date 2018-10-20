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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.junit.Test;

/**
 * Test to check Serialization
 */
public class RandomBinaryCodecTest extends RandomGeo3dShapeGenerator {

  @Test
  @Repeat(iterations = 10)
  public void testRandomPointCodec() throws IOException{
    PlanetModel planetModel = randomPlanetModel();
    GeoPoint shape = randomGeoPoint(planetModel);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SerializableObject.writeObject(outputStream, shape);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    SerializableObject shapeCopy = SerializableObject.readObject(planetModel, inputStream);
    assertEquals(shape.toString(), shape, shapeCopy);
  }

  @Test
  @Repeat(iterations = 100)
  public void testRandomPlanetObjectCodec() throws IOException{
    PlanetModel planetModel = randomPlanetModel();
    int type = randomShapeType();
    GeoShape shape = randomGeoShape(type, planetModel);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SerializableObject.writePlanetObject(outputStream, shape);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    SerializableObject shapeCopy = SerializableObject.readPlanetObject(inputStream);
    assertEquals(shape.toString(), shape, shapeCopy);
  }

  @Test
  @Repeat(iterations = 100)
  public void testRandomShapeCodec() throws IOException{
    PlanetModel planetModel = randomPlanetModel();
    int type = randomShapeType();
    GeoShape shape = randomGeoShape(type, planetModel);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SerializableObject.writeObject(outputStream, shape);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    SerializableObject shapeCopy = SerializableObject.readObject(planetModel, inputStream);
    assertEquals(shape.toString(), shape, shapeCopy);
  }
}
