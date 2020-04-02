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

package org.apache.solr.util.numeric;

import com.carrotsearch.hppc.procedures.IntFloatProcedure;

public class IntFloatHashMap implements IntFloatMap {
  private com.carrotsearch.hppc.IntFloatHashMap hashMap;
  private float emptyValue;

  public IntFloatHashMap(int initialSize, float emptyValue) {
    this.hashMap = new com.carrotsearch.hppc.IntFloatHashMap();
    this.emptyValue = emptyValue;
  }
  @Override
  public final void set(int key, float value) {
    this.hashMap.put(key, value);
  }

  @Override
  public final float get(int key) {
    return this.hashMap.getOrDefault(key, emptyValue);
  }

  @Override
  public void forEachValue(FloatConsumer consumer) {
    this.hashMap.forEach((IntFloatProcedure) (key, value) -> consumer.accept(value));
  }
}
