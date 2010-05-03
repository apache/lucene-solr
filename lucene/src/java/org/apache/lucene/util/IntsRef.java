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

/** Represents int[], as a slice (offset + length) into an
 *  existing int[].
 *
 *  @lucene.internal */
public final class IntsRef {

  public int[] ints;
  public int offset;
  public int length;

  public IntsRef() {
  }

  public IntsRef(int capacity) {
    ints = new int[capacity];
  }

  public IntsRef(int[] ints, int offset, int length) {
    this.ints = ints;
    this.offset = offset;
    this.length = length;
  }

  public IntsRef(IntsRef other) {
    copy(other);
  }

  @Override
  public Object clone() {
    return new IntsRef(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 0;
    final int end = offset + length;
    for(int i = offset; i < end; i++) {
      result = prime * result + ints[i];
    }
    return result;
  }
  
  @Override
  public boolean equals(Object other) {
    return this.intsEquals((IntsRef) other);
  }

  public boolean intsEquals(IntsRef other) {
    if (length == other.length) {
      int otherUpto = other.offset;
      final int[] otherInts = other.ints;
      final int end = offset + length;
      for(int upto=offset;upto<end;upto++,otherUpto++) {
        if (ints[upto] != otherInts[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public void copy(IntsRef other) {
    if (ints == null) {
      ints = new int[other.length];
    } else {
      ints = ArrayUtil.grow(ints, other.length);
    }
    System.arraycopy(other.ints, other.offset, ints, 0, other.length);
    length = other.length;
    offset = 0;
  }

  public void grow(int newLength) {
    if (ints.length < newLength) {
      ints = ArrayUtil.grow(ints, newLength);
    }
  }
}
