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
 * This class filters our the ROOT category path. For more information see
 * {@link PathPolicy}.
 * 
 * @lucene.experimental
 */
public class DefaultPathPolicy implements PathPolicy {

  /**
   * Filters out (returns false) CategoryPaths equal or less than
   * {@link TaxonomyReader#ROOT_ORDINAL}. true otherwise.
   */
  public boolean shouldAdd(CategoryPath categoryPath) {
    return categoryPath.length() > 0;
  }
}
