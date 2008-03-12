package org.apache.lucene.index;

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

/* Used by DocumentsWriter to track data for term vectors.
 * One of these exists per unique term seen in each field in
 * the document. */
class PostingVector {
  Posting p;                                      // Corresponding Posting instance for this term
  int lastOffset;                                 // Last offset we saw
  int offsetStart;                                // Address of first slice for offsets
  int offsetUpto;                                 // Next write address for offsets
  int posStart;                                   // Address of first slice for positions
  int posUpto;                                    // Next write address for positions
}
