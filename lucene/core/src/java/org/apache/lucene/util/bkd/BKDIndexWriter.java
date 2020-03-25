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

package org.apache.lucene.util.bkd;

import java.io.IOException;

/**
 * Serializes a KD tree in a index.
 *
 * @lucene.experimental */
public interface BKDIndexWriter {

  /** writes a leaf block in the index */
  void writeLeafBlock(BKDConfig config, BKDLeafBlock leafBlock,
                      int[] commonPrefixes, int sortedDim, int leafCardinality) throws IOException;


  /** writes inner nodes in the index */
  void writeIndex(BKDConfig config, int countPerLeaf, long[] leafBlockFPs, byte[] splitPackedValues,
                  byte[] minPackedValue, byte[] maxPackedValue, long pointCount, int numberDocs) throws IOException;

  /** return the current position of the index */
  long getFilePointer();
}
