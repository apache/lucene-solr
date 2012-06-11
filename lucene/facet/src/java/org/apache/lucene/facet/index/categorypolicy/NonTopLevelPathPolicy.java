package org.apache.lucene.facet.index.categorypolicy;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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
 * This class filters our the ROOT category, and it's direct descendants. For
 * more information see {@link PathPolicy}.
 * 
 * @lucene.experimental
 */
public class NonTopLevelPathPolicy implements PathPolicy {

  /**
   * The shortest path length delivered is two components (root + one child).
   */
  public final int DEFAULT_MINIMAL_SUBPATH_LENGTH = 2;

  /**
   * Filters out (returns false) CategoryPaths equal or less than
   * {@link TaxonomyReader#ROOT_ORDINAL}. true otherwise.
   */
  public boolean shouldAdd(CategoryPath categoryPath) {
    return categoryPath.length() >= DEFAULT_MINIMAL_SUBPATH_LENGTH;
  }
}
