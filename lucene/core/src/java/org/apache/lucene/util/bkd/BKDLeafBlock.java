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

import org.apache.lucene.util.BytesRef;

/**
 * It represents a leaf block on thr BKD tree.
 *
 * @lucene.experimental */
public interface BKDLeafBlock {

  /** number of points on the leaf */
  int count();

  /** the point values of this leaf at a given position packed
   * on a BytesRef */
  BytesRef packedValue(int position);

  /** the docId of this block at a given position */
  int docId(int position);
}
