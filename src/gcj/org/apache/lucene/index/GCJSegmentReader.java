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

import java.io.IOException;

import org.apache.lucene.store.GCJIndexInput;

class GCJSegmentReader extends SegmentReader {

  /** Try to use an optimized native implementation of TermDocs.  The optimized
   * implementation can only be used when the segment's directory is a
   * GCJDirectory and it is not in compound format.  */
  public final TermDocs termDocs() throws IOException {
    if (freqStream instanceof GCJIndexInput) {    // it's a GCJIndexInput
      return new GCJTermDocs(this);               // so can use GCJTermDocs
    } else {
      return super.termDocs();
    }
  }
}
