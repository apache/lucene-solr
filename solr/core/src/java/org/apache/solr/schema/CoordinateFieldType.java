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
package org.apache.solr.schema;

/**
 * A CoordinateFieldType is the base class for {@link org.apache.solr.schema.FieldType}s that have semantics
 * related to items in a coordinate system.
 * <br>
 * Implementations depend on a delegating work to a sub {@link org.apache.solr.schema.FieldType}, specified by
 * either the {@link #SUB_FIELD_SUFFIX} or the {@link #SUB_FIELD_TYPE} (the latter is used if both are defined.
 * <br>
 * Example:
 * <pre>&lt;fieldType name="xy" class="solr.PointType" dimension="2" subFieldType="double"/&gt;
 * </pre>
 * In theory, classes deriving from this should be able to do things like represent a point, a polygon, a line, etc.
 * <br>
 * NOTE: There can only be one sub Field Type.
 *
 */
public abstract class CoordinateFieldType extends AbstractSubTypeFieldType {
  /**
   * The dimension of the coordinate system
   */
  protected int dimension;
  /**
   * 2 dimensional by default
   */
  public static final int DEFAULT_DIMENSION = 2;
  public static final String DIMENSION = "dimension";


  public int getDimension() {
    return dimension;
  }
}
