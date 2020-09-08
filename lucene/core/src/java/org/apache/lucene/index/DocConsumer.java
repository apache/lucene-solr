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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;

abstract class DocConsumer implements Accountable {
  abstract void processDocument(int docId, Iterable<? extends IndexableField> document) throws IOException;
  abstract Sorter.DocMap flush(final SegmentWriteState state) throws IOException;
  abstract void abort() throws IOException;

  /**
   * Returns a {@link DocIdSetIterator} for the given field or null if the field doesn't have
   * doc values.
   */
  abstract DocIdSetIterator getHasDocValues(String field);

}
