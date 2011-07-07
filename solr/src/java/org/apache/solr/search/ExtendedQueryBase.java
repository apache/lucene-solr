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

import org.apache.lucene.search.Query;

public class ExtendedQueryBase extends Query implements ExtendedQuery {
  private int cost;
  private boolean cache = true;
  private boolean cacheSep;

  @Override
  public void setCache(boolean cache) {
    this.cache = cache;
  }

  @Override
  public boolean getCache() {
    return cache;
  }

  @Override
  public void setCacheSep(boolean cacheSep) {
    this.cacheSep = cacheSep;
  }

  @Override
  public boolean getCacheSep() {
    return cacheSep;
  }

  @Override
  public void setCost(int cost) {
    this.cost = cost;
  }

  public int getCost() {
    return cost;
  }

  public String getOptions() {
    StringBuilder sb = new StringBuilder();
    if (!cache) {
      sb.append("{!cache=false");
      sb.append(" cost=");
      sb.append(cost);
      sb.append("}");
    } else if (cacheSep) {
      sb.append("{!cache=sep");
      sb.append("}");
    }
    return sb.toString();
  }

  @Override
  public String toString(String field) {
    return getOptions();
  }
}
