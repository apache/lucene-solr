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
package org.apache.lucene.util.mutable;

/**
 * {@link MutableValue} implementation of type 
 * <code>double</code>.
 */
public class MutableValueDouble extends MutableValue {
  public double value;

  @Override
  public Object toObject() {
    return exists ? value : null;
  }

  @Override
  public void copy(MutableValue source) {
    MutableValueDouble s = (MutableValueDouble) source;
    value = s.value;
    exists = s.exists;
  }

  @Override
  public MutableValue duplicate() {
    MutableValueDouble v = new MutableValueDouble();
    v.value = this.value;
    v.exists = this.exists;
    return v;
  }

  @Override
  public boolean equalsSameType(Object other) {
    MutableValueDouble b = (MutableValueDouble)other;
    return value == b.value && exists == b.exists;
  }

  @Override
  public int compareSameType(Object other) {
    MutableValueDouble b = (MutableValueDouble)other;
    int c = Double.compare(value, b.value);
    if (c != 0) return c;
    if (!exists) return -1;
    if (!b.exists) return 1;
    return 0;
  }

  @Override
  public int hashCode() {
    long x = Double.doubleToLongBits(value);
    return (int)x + (int)(x>>>32);
  }
}
