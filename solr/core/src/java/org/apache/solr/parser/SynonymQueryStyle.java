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

package org.apache.solr.parser;

/**
 *  Query strategy when analyzed query terms overlap the same position (ie synonyms)
 *  consider if pants and khakis are query time synonyms
 *
 *  {@link #AS_SAME_TERM}
 *  {@link #PICK_BEST}
 *  {@link #AS_DISTINCT_TERMS}
 */
public enum SynonymQueryStyle {
  /** (default) synonym terms share doc freq
   *  so if "pants" has df 500, and "khakis" a df of 50, uses 500 df when scoring both terms
   *  appropriate for exact synonyms
   *  see {@link org.apache.lucene.search.SynonymQuery}
   * */
  AS_SAME_TERM,

  /** highest scoring term match chosen (ie dismax)
   *  so if "pants" has df 500, and "khakis" a df of 50, khakis matches are scored higher
   *  appropriate when more specific synonyms should score higher
   * */
  PICK_BEST,

  /** each synonym scored indepedently, then added together (ie boolean query)
   *  so if "pants" has df 500, and "khakis" a df of 50, khakis matches are scored higher but
   *  summed with any "pants" matches
   *  appropriate when more specific synonyms should score higher, but we don't want to ignore
   *  less specific synonyms
   * */
  AS_DISTINCT_TERMS
}
