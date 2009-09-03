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

package org.apache.lucene.analysis;

/**
 * Replacement for Java 1.5 Character.valueOf()
 * @deprecated Move to Character.valueOf() in 3.0
 */
class CharacterCache {

  private static final Character cache[] = new Character[128];

  static {
    for (int i = 0; i < cache.length; i++) {
      cache[i] = new Character((char) i);
    }
  }

  /**
   * Returns a Character instance representing the given char value
   * 
   * @param c
   *          a char value
   * @return a Character representation of the given char value.
   */
  public static Character valueOf(char c) {
    if (c < cache.length) {
      return cache[(int) c];
    }
    return new Character(c);
  }
}
