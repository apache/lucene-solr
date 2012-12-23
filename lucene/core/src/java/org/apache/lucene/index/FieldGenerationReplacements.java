package org.apache.lucene.index;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

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

/**
 * Holds for a certain field in a stacked segment the documents document the
 * generation in which the last replacement (of the relevant field) took place.
 */
public class FieldGenerationReplacements implements Iterable<Entry<Integer,Long>> {
  
  TreeMap<Integer,Long> map = null;
  
  /**
   * Set the generation value for a given document.
   * 
   * @param docId
   *          Document id.
   * @param generation
   *          The requested generation.
   */
  public void set(int docId, long generation) {
    if (map == null) {
      map = new TreeMap<Integer,Long>();
    }
    assert generation > 0 && generation <= Integer.MAX_VALUE;
    map.put(docId, generation);
  }
  
  /**
   * Get the generation value for a given document.
   * 
   * @param docId
   *          Document id.
   * @return The requested generation, or -1 if the document has no generation.
   */
  public long get(int docId) {
    if (map == null) {
      return -1;
    }
    final Long val = map.get(docId);
    if (val == null) {
      return -1;
    }
    return val;
  }
  
  public void merge(FieldGenerationReplacements other) {
    if (map == null) {
       map = other.map;
    } else if (other != null) {
      map.putAll(other.map);
    }
  }

  @Override
  public Iterator<Entry<Integer,Long>> iterator() {
    return map.entrySet().iterator();
  }

  public int size() {
    return map.size();
  }
  
}
