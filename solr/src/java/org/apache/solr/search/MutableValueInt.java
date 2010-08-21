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
    return value;
  }

  @Override
  public void copy(MutableValue source) {
    value = ((MutableValueInt)source).value;
  }

  @Override
  public MutableValue duplicate() {
    MutableValueInt v = new MutableValueInt();
    v.value = this.value;
    return v;
  }

  @Override
  public boolean equalsSameType(Object other) {
    return value == ((MutableValueInt)other).value;
  }

  @Override
  public int compareSameType(Object other) {
    int a = value;
    int b = ((MutableValueInt)other).value;
    return (int)((((long)a) - ((long)b)) >> 32);  // any shift >= 32 should do.

    /* is there any pattern that the compiler would recognize as a single native CMP instruction? */
    /***
    if (a<b) return -1;
    else if (a>b) return 1;
    else return 0;
    ***/
  }


  @Override
  public int hashCode() {
    // TODO: if used in HashMap, it already mixes the value... maybe use a straight value?
    return (value>>8) + (value>>16);
  }
}
