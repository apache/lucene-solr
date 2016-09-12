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
package org.apache.lucene.analysis.util;

import java.util.Map;

/**
 * A simple class that stores key Strings as char[]'s in a
 * hash table. Note that this is not a general purpose
 * class.  For example, it cannot remove items from the
 * map, nor does it resize its hash table to be smaller,
 * etc.  It is designed to be quick to retrieve items
 * by char[] keys without the necessity of converting
 * to a String first.
 * @deprecated This class moved to Lucene-Core module:
 *  {@link org.apache.lucene.analysis.CharArrayMap}
 */
@Deprecated
public class CharArrayMap<V> extends org.apache.lucene.analysis.CharArrayMap<V> {

  /**
   * Create map with enough capacity to hold startSize terms
   *
   * @param startSize
   *          the initial capacity
   * @param ignoreCase
   *          <code>false</code> if and only if the set should be case sensitive
   *          otherwise <code>true</code>.
   */
  public CharArrayMap(int startSize, boolean ignoreCase) {
    super(startSize, ignoreCase);
  }

  /**
   * Creates a map from the mappings in another map. 
   *
   * @param c
   *          a map whose mappings to be copied
   * @param ignoreCase
   *          <code>false</code> if and only if the set should be case sensitive
   *          otherwise <code>true</code>.
   */
  public CharArrayMap(Map<?,? extends V> c, boolean ignoreCase) {
    super(c, ignoreCase);
  }
  
}
