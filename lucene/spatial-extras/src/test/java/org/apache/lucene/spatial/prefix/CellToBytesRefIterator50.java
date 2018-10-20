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
package org.apache.lucene.spatial.prefix;

import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.util.BytesRef;

/** For testing Lucene <= 5.0. Index redundant prefixes for leaf cells. Fixed in LUCENE-4942. */
class CellToBytesRefIterator50 extends CellToBytesRefIterator {

  Cell repeatCell;

  @Override
  public BytesRef next() {
    if (repeatCell != null) {
      bytesRef = repeatCell.getTokenBytesWithLeaf(bytesRef);
      repeatCell = null;
      return bytesRef;
    }
    if (!cellIter.hasNext()) {
      return null;
    }
    Cell cell = cellIter.next();
    bytesRef = cell.getTokenBytesNoLeaf(bytesRef);
    if (cell.isLeaf()) {
      repeatCell = cell;
    }
    return bytesRef;
  }
}
