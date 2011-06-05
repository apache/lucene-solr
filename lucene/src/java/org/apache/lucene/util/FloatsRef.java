package org.apache.lucene.util;

/**
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

/**
 * Represents double[], as a slice (offset + length) into an existing double[].
 * 
 * @lucene.internal
 */
public final class FloatsRef implements Cloneable {
  public double[] floats;
  public int offset;
  public int length;

  public FloatsRef() {
  }

  public FloatsRef(int capacity) {
    floats = new double[capacity];
  }
  
  public void set(double value) {
    floats[offset] = value;
  }
  
  public double get() {
    return floats[offset];
  }

  public FloatsRef(double[] floats, int offset, int length) {
    this.floats = floats;
    this.offset = offset;
    this.length = length;
  }

  public FloatsRef(FloatsRef other) {
    copy(other);
  }

  @Override
  public Object clone() {
    return new FloatsRef(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 0;
    final int end = offset + length;
    for(int i = offset; i < end; i++) {
      long value = Double.doubleToLongBits(floats[i]);
      result = prime * result + (int) (value ^ (value >>> 32));
    }
    return result;
  }
  
  @Override
  public boolean equals(Object other) {
    return other instanceof FloatsRef && this.floatsEquals((FloatsRef) other);
  }

  public boolean floatsEquals(FloatsRef other) {
    if (length == other.length) {
      int otherUpto = other.offset;
      final double[] otherFloats = other.floats;
      final int end = offset + length;
      for(int upto=offset;upto<end;upto++,otherUpto++) {
        if (floats[upto] != otherFloats[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public void copy(FloatsRef other) {
    if (floats == null) {
      floats = new double[other.length];
    } else {
      floats = ArrayUtil.grow(floats, other.length);
    }
    System.arraycopy(other.floats, other.offset, floats, 0, other.length);
    length = other.length;
    offset = 0;
  }

  public void grow(int newLength) {
    if (floats.length < newLength) {
      floats = ArrayUtil.grow(floats, newLength);
    }
  }
}