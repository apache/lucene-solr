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

public interface PrimitiveMapFactory {
  IntIntMap newIntIntMap(int initialSize, int emptyValue);
  IntFloatMap newIntFloatMap(int initialSize, float emptyValue);
  IntLongMap newIntLongMap(int initialSize, long emptyValue);

  static PrimitiveMapFactory newArrayBasedFactory() {
    return new PrimitiveMapFactory() {
      @Override
      public IntIntMap newIntIntMap(int initialSize, int emptyValue) {
        return new IntIntArrayBasedMap(initialSize, emptyValue);
      }

      @Override
      public IntFloatMap newIntFloatMap(int initialSize, float emptyValue) {
        return new IntFloatArrayBasedMap(initialSize, emptyValue);
      }

      @Override
      public IntLongMap newIntLongMap(int initialSize, long emptyValue) {
        return new IntLongArrayBasedMap(initialSize, emptyValue);
      }
    };
  }

  static PrimitiveMapFactory newHashBasedFactory() {
    return new PrimitiveMapFactory() {
      @Override
      public IntIntMap newIntIntMap(int initialSize, int emptyValue) {
        return new IntIntHashMap(4, emptyValue);
      }

      @Override
      public IntFloatMap newIntFloatMap(int initialSize, float emptyValue) {
        return new IntFloatHashMap(4, emptyValue);
      }

      @Override
      public IntLongMap newIntLongMap(int initialSize, long emptyValue) {
        return new IntLongHashMap(4, emptyValue);
      }
    };
  }
}
