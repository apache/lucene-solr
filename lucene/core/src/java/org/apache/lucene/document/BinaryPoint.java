package org.apache.lucene.document;

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

import org.apache.lucene.util.BytesRef;

/** A binary field that is indexed dimensionally such that finding
 *  all documents within an N-dimensional shape or range at search time is
 *  efficient.  Muliple values for the same field in one documents
 *  is allowed. */

public final class BinaryPoint extends Field {

  private static FieldType getType(byte[][] point) {
    if (point == null) {
      throw new IllegalArgumentException("point cannot be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point cannot be 0 dimensions");
    }
    int bytesPerDim = -1;
    for(int i=0;i<point.length;i++) {
      byte[] oneDim = point[i];
      if (oneDim == null) {
        throw new IllegalArgumentException("point cannot have null values");
      }
      if (oneDim.length == 0) {
        throw new IllegalArgumentException("point cannot have 0-length values");
      }
      if (bytesPerDim == -1) {
        bytesPerDim = oneDim.length;
      } else if (bytesPerDim != oneDim.length) {
        throw new IllegalArgumentException("all dimensions must have same bytes length; got " + bytesPerDim + " and " + oneDim.length);
      }
    }
    return getType(point.length, bytesPerDim);
  }

  private static FieldType getType(int numDims, int bytesPerDim) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, bytesPerDim);
    type.freeze();
    return type;
  }

  private static BytesRef pack(byte[]... point) {
    if (point == null) {
      throw new IllegalArgumentException("point cannot be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point cannot be 0 dimensions");
    }
    if (point.length == 1) {
      return new BytesRef(point[0]);
    }
    int bytesPerDim = -1;
    for(byte[] dim : point) {
      if (dim == null) {
        throw new IllegalArgumentException("point cannot have null values");
      }
      if (bytesPerDim == -1) {
        if (dim.length == 0) {
          throw new IllegalArgumentException("point cannot have 0-length values");
        }
        bytesPerDim = dim.length;
      } else if (dim.length != bytesPerDim) {
        throw new IllegalArgumentException("all dimensions must have same bytes length; got " + bytesPerDim + " and " + dim.length);
      }
    }
    byte[] packed = new byte[bytesPerDim*point.length];
    for(int i=0;i<point.length;i++) {
      System.arraycopy(point[i], 0, packed, i*bytesPerDim, bytesPerDim);
    }
    return new BytesRef(packed);
  }

  /** General purpose API: creates a new BinaryPoint, indexing the
   *  provided N-dimensional binary point.
   *
   *  @param name field name
   *  @param point byte[][] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public BinaryPoint(String name, byte[]... point) {
    super(name, pack(point), getType(point));
  }

  /** Expert API */
  public BinaryPoint(String name, byte[] packedPoint, FieldType type) {
    super(name, packedPoint, type);
    if (packedPoint.length != type.pointDimensionCount() * type.pointNumBytes()) {
      throw new IllegalArgumentException("packedPoint is length=" + packedPoint.length + " but type.pointDimensionCount()=" + type.pointDimensionCount() + " and type.pointNumBytes()=" + type.pointNumBytes());
    }
  }
}
