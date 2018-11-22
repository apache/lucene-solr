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

package org.apache.lucene.spatial.prefix.tree;


import com.google.common.geometry.S2CellId;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;

/**
 * Shape factory for Spatial contexts that support S2 geometry. It is an extension of
 * Spatial4j {@link ShapeFactory}.
 *
 * @lucene.experimental
 */
public interface S2ShapeFactory extends ShapeFactory{

  /**
   * Factory method for S2 cell shapes.
   *
   * @param cellId The S2 cell id
   * @return the shape representing the cell.
   */
  Shape getS2CellShape(S2CellId cellId);
}
