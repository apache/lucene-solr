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

public class MutableValueLong extends MutableValue {
  public long value;

  @Override
  public Object toObject() {
    return value;
  }

  @Override
  public void copy(MutableValue source) {
    value = ((MutableValueLong)source).value;
  }

  @Override
  public MutableValue duplicate() {
    MutableValueLong v = new MutableValueLong();
    v.value = this.value;
    return v;
  }

  @Override
  public boolean equalsSameType(Object other) {
    return value == ((MutableValueLong)other).value;
  }

  @Override
  public int compareSameType(Object other) {
    long b = ((MutableValueLong)other).value;
    if (value<b) return -1;
    else if (value>b) return 1;
    else return 0;
  }


  @Override
  public int hashCode() {
    return (int)value + (int)(value>>32);
  }
}