/**
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

package org.apache.solr.search;

import org.apache.lucene.search.DefaultSimilarity;

import java.util.HashMap;

/**
 */
// don't make it public for now... easier to change later.

// This class is currently unused.
class SolrSimilarity extends DefaultSimilarity {
  private final HashMap<String,Float> lengthNormConfig = new HashMap<String,Float>();

  public float lengthNorm(String fieldName, int numTerms) {
    // Float f = lengthNormConfig.
    // if (lengthNormDisabled.)
    return super.lengthNorm(fieldName, numTerms);
  }
}
