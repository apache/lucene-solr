package org.apache.lucene.server;

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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;

/** Holds only the top-level (browse-only) facet counts. */
public class TopFacetsCache {
  // TODO: we could serialize/compress bytes for each entry:

  // TODO: this can grow unbounded!!  Even though we only
  // cache the pure-browse facet counts, we cache all
  // requests against that ... so if you have a deep
  // taxonomy and user clicks on all the nodes in it, we are
  // caching ...:
  private final Map<FacetRequest,FacetResult> cache = new ConcurrentHashMap<FacetRequest,FacetResult>();

  public FacetResult get(FacetRequest request) {
    // System.out.println("GET: " + cache.size() + " entries");
    return cache.get(request);
  }

  public void add(FacetRequest request, FacetResult result) {
    if (!cache.containsKey(request)) {
      cache.put(request, result);
    }
  }
}
