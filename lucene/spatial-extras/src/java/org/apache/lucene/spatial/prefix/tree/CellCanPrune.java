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

package org.apache.lucene.spatial.prefix.tree;

import org.locationtech.spatial4j.shape.Shape;

/**
 *  Grid cells that share nothing with other cells when calling {@link #getNextLevelCells(Shape)}
 *  might implement this interface. Children cells for this cell will be eligible for pruning via
 *  {@link org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy#setPruneLeafyBranches(boolean)}.
 *
 * @lucene.experimental
 */
public interface CellCanPrune extends Cell{

  /**
   * Returns the number of children for this cell.
   *
   * @return the number of children.
   */
  int getSubCellsSize();
}
