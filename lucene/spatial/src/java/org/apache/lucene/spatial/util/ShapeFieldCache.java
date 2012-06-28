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

package org.apache.lucene.spatial.util;

import com.spatial4j.core.shape.Shape;

import java.util.ArrayList;
import java.util.List;

/**
 * @lucene.internal
 */
public class ShapeFieldCache<T extends Shape> {
  private List<T>[] cache;
  public int defaultLength;

  @SuppressWarnings({"unchecked"})
  public ShapeFieldCache( int length, int defaultLength ) {
    cache = new List[length];
    this.defaultLength= defaultLength;
  }

  public void add( int docid, T s ) {
    List<T> list = cache[docid];
    if( list == null ) {
      list = cache[docid] = new ArrayList<T>(defaultLength);
    }
    list.add( s );
  }

  public List<T> getShapes( int docid ) {
    return cache[docid];
  }
}
