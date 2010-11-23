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

public class MutableValueInt extends MutableValue {
  public int value;
  
  @Override
  public Object toObject() {
    return exists ? value : null;
  }

  @Override
  public void copy(MutableValue source) {
    MutableValueInt s = (MutableValueInt) source;
    value = s.value;
    exists = s.exists;
  }

  @Override
  public MutableValue duplicate() {
    MutableValueInt v = new MutableValueInt();
    v.value = this.value;
    v.exists = this.exists;
    return v;
  }

  @Override
  public boolean equalsSameType(Object other) {
    MutableValueInt b = (MutableValueInt)other;
    return value == b.value && exists == b.exists;
  }

  @Override
  public int compareSameType(Object other) {
    MutableValueInt b = (MutableValueInt)other;
    int ai = value;
    int bi = b.value;
    int c = (int)((((long)ai) - ((long)bi)) >> 32);  // any shift >= 32 should do.
    if (c!=0) return c;
    /* is there any pattern that the compiler would recognize as a single native CMP instruction? */
    /***
    if (a<b) return -1;
    else if (a>b) return 1;
    else return 0;
    ***/

    if (exists == b.exists) return 0;
    return exists ? 1 : -1;
  }


  @Override
  public int hashCode() {
    // TODO: if used in HashMap, it already mixes the value... maybe use a straight value?
    return (value>>8) + (value>>16);
  }
}
