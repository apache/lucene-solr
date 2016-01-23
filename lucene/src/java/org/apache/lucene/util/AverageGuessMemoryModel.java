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

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * An average, best guess, MemoryModel that should work okay on most systems.
 * 
 */
public class AverageGuessMemoryModel extends MemoryModel {
  // best guess primitive sizes
  private final Map<Class<?>,Integer> sizes = new IdentityHashMap<Class<?>,Integer>() {
    {
      put(boolean.class, Integer.valueOf(1));
      put(byte.class, Integer.valueOf(1));
      put(char.class, Integer.valueOf(2));
      put(short.class, Integer.valueOf(2));
      put(int.class, Integer.valueOf(4));
      put(float.class, Integer.valueOf(4));
      put(double.class, Integer.valueOf(8));
      put(long.class, Integer.valueOf(8));
    }
  };

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.util.MemoryModel#getArraySize()
   */
  @Override
  public int getArraySize() {
    return 16;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.util.MemoryModel#getClassSize()
   */
  @Override
  public int getClassSize() {
    return 8;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.util.MemoryModel#getPrimitiveSize(java.lang.Class)
   */
  @Override
  public int getPrimitiveSize(Class<?> clazz) {
    return sizes.get(clazz).intValue();
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.util.MemoryModel#getReferenceSize()
   */
  @Override
  public int getReferenceSize() {
    return 4;
  }

}
