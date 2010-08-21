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
package org.apache.solr.search;

public class MutableValueFloat extends MutableValue {
  public float value;

  @Override
  public Object toObject() {
    return value;
  }

  @Override
  public void copy(MutableValue source) {
    value = ((MutableValueFloat)source).value;
  }

  @Override
  public MutableValue duplicate() {
    MutableValueFloat v = new MutableValueFloat();
    v.value = this.value;
    return v;
  }

  @Override
  public boolean equalsSameType(Object other) {
    return value == ((MutableValueFloat)other).value;
  }

  @Override
  public int compareSameType(Object other) {
    return Float.compare(value, ((MutableValueFloat)other).value);  // handles NaN
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(value);
  }
}