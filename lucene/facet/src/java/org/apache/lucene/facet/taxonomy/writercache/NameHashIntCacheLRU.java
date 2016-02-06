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
package org.apache.lucene.facet.taxonomy.writercache;

import org.apache.lucene.facet.taxonomy.FacetLabel;

/**
 * An an LRU cache of mapping from name to int.
 * Used to cache Ordinals of category paths.
 * It uses as key, hash of the path instead of the path.
 * This way the cache takes less RAM, but correctness depends on
 * assuming no collisions. 
 * 
 * @lucene.experimental
 */
public class NameHashIntCacheLRU extends NameIntCacheLRU {

  NameHashIntCacheLRU(int maxCacheSize) {
    super(maxCacheSize);
  }

  @Override
  Object key(FacetLabel name) {
    return new Long(name.longHashCode());
  }

  @Override
  Object key(FacetLabel name, int prefixLen) {
    return new Long(name.subpath(prefixLen).longHashCode());
  }
  
}
