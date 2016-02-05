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
package org.apache.solr.schema;

import java.util.Map;

import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;

/**
 * @see RecursivePrefixTreeStrategy
 * @lucene.experimental
 */
public class SpatialRecursivePrefixTreeFieldType extends AbstractSpatialPrefixTreeFieldType<RecursivePrefixTreeStrategy> {

  /** @see RecursivePrefixTreeStrategy#setPrefixGridScanLevel(int) */
  public static final String PREFIX_GRID_SCAN_LEVEL = "prefixGridScanLevel";

  private Integer prefixGridScanLevel;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    String v = args.remove(PREFIX_GRID_SCAN_LEVEL);
    if (v != null)
      prefixGridScanLevel = Integer.valueOf(v);
  }

  @Override
  protected RecursivePrefixTreeStrategy newPrefixTreeStrategy(String fieldName) {
    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, fieldName);
    if (prefixGridScanLevel != null)
      strategy.setPrefixGridScanLevel(prefixGridScanLevel);
    if (grid instanceof PackedQuadPrefixTree) {
      // This grid has a (usually) better prune leafy branch implementation
      ((PackedQuadPrefixTree) grid).setPruneLeafyBranches(true);
      strategy.setPruneLeafyBranches(false);
    }
    return strategy;
  }
}

