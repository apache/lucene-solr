package org.apache.lucene.facet.range;

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

import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;

/** Holds the facet results for a {@link
 *  RangeFacetRequest}. */
public class RangeFacetResultNode extends FacetResultNode {
  public final Range range;

  RangeFacetResultNode(String field, Range range, int count) {
    super(-1, count);
    this.range = range;
    this.label = new CategoryPath(field, range.label);
  }
}
