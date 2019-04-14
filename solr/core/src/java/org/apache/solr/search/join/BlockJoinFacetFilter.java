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
package org.apache.solr.search.join;

import java.util.Objects;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.PostFilter;

class BlockJoinFacetFilter extends Query implements PostFilter {

  public static final int COST = 120;
  private DelegatingCollector blockJoinFacetCollector;

  public BlockJoinFacetFilter(DelegatingCollector blockJoinFacetCollector) {
    super();
    this.blockJoinFacetCollector = blockJoinFacetCollector;
  }

  @Override
  public String toString(String field) {
    return null;
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    return blockJoinFacetCollector;
  }

  @Override
  public boolean getCache() {
    return false;
  }

  @Override
  public void setCache(boolean cache) {

  }

  @Override
  public int getCost() {
    return COST;
  }

  @Override
  public void setCost(int cost) {

  }

  @Override
  public boolean getCacheSep() {
    return false;
  }

  @Override
  public void setCacheSep(boolean cacheSep) {

  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(BlockJoinFacetFilter other) {
    return Objects.equals(blockJoinFacetCollector, other.blockJoinFacetCollector);
  }

  @Override
  public int hashCode() {
    return classHash() * 31 + blockJoinFacetCollector.hashCode();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
