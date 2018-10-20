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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.IntsRef;

/** Provides per-document ordinals. */

public abstract class OrdinalsReader {

  /** Returns ordinals for documents in one segment. */
  public static abstract class OrdinalsSegmentReader {
    /** Get the ordinals for this document.  ordinals.offset
     *  must always be 0! */
    public abstract void get(int doc, IntsRef ordinals) throws IOException;

    /** Default constructor. */
    public OrdinalsSegmentReader() {
    }
  }

  /** Default constructor. */
  public OrdinalsReader() {
  }

  /** Set current atomic reader. */
  public abstract OrdinalsSegmentReader getReader(LeafReaderContext context) throws IOException;

  /** Returns the indexed field name this {@code
   *  OrdinalsReader} is reading from. */
  public abstract String getIndexFieldName();
}
