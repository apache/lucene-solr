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
package org.apache.solr.common.util;

import java.util.*;


/** <code>SimpleOrderedMap</code> is a {@link NamedList} where access by key is more
 * important than maintaining order when it comes to representing the
 * held data in other forms, as ResponseWriters normally do.
 * It's normally not a good idea to repeat keys or use null keys, but this
 * is not enforced.  If key uniqueness enforcement is desired, use a regular {@link Map}.
 * <p>
 * For example, a JSON response writer may choose to write a SimpleOrderedMap
 * as {"foo":10,"bar":20} and may choose to write a NamedList as
 * ["foo",10,"bar",20].  An XML response writer may choose to render both
 * the same way.
 * </p>
 * <p>
 * This class does not provide efficient lookup by key, its main purpose is
 * to hold data to be serialized.  It aims to minimize overhead and to be
 * efficient at adding new elements.
 * </p>
 */
public class SimpleOrderedMap<T> extends NamedList<T> {
  /** Creates an empty instance */
  public SimpleOrderedMap() {
    super();
  }

  public SimpleOrderedMap(int sz) {
    super(sz);
  }

  /**
   * Creates an instance backed by an explicitly specified list of
   * pairwise names/values.
   *
   * <p>
   * TODO: this method was formerly public, now that it's not we can change the impl details of 
   * this class to be based on a Map.Entry[] 
   * </p>
   *
   * @param nameValuePairs underlying List which should be used to implement a SimpleOrderedMap; modifying this List will affect the SimpleOrderedMap.
   * @lucene.internal
   */
  private SimpleOrderedMap(List<Object> nameValuePairs) {
    super(nameValuePairs);
  }
  
  public SimpleOrderedMap(Map.Entry<String, T>[] nameValuePairs) { 
    super(nameValuePairs);
  }

  @Override
  public SimpleOrderedMap<T> clone() {
    ArrayList<Object> newList = new ArrayList<>(nvPairs.size());
    newList.addAll(nvPairs);
    return new SimpleOrderedMap<>(newList);
  }
}
