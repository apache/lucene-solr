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

/* Used by DocumentsWriter to track postings for a single
 * term.  One of these exists per unique term seen since the
 * last flush. */
final class Posting {
  int textStart;                                  // Address into char[] blocks where our text is stored
  int docFreq;                                    // # times this term occurs in the current doc
  int freqStart;                                  // Address of first byte[] slice for freq
  int freqUpto;                                   // Next write address for freq
  int proxStart;                                  // Address of first byte[] slice
  int proxUpto;                                   // Next write address for prox
  int lastDocID;                                  // Last docID where this term occurred
  int lastDocCode;                                // Code for prior doc
  int lastPosition;                               // Last position where this term occurred
  PostingVector vector;                           // Corresponding PostingVector instance
}
