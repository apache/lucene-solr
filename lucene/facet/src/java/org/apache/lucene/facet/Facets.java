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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.List;

/** Common base class for all facets implementations.
 *
 *  @lucene.experimental */
public abstract class Facets {

  /** Default constructor. */
  public Facets() {
  }

  /** Returns the topN child labels under the specified
   *  path.  Returns null if the specified path doesn't
   *  exist or if this dimension was never seen. */
  public abstract FacetResult getTopChildren(int topN, String dim, String... path) throws IOException;

  /** Return the count or value
   *  for a specific path.  Returns -1 if
   *  this path doesn't exist, else the count. */
  public abstract Number getSpecificValue(String dim, String... path) throws IOException;

  /** Returns topN labels for any dimension that had hits,
   *  sorted by the number of hits that dimension matched;
   *  this is used for "sparse" faceting, where many
   *  different dimensions were indexed, for example
   *  depending on the type of document. */
  public abstract List<FacetResult> getAllDims(int topN) throws IOException;
}
