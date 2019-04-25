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
package org.apache.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SimpleCollector;

abstract class DocValuesTermsCollector<DV> extends SimpleCollector {
  
  @FunctionalInterface
  static interface Function<R> {
    R apply(LeafReader t) throws IOException;
  }
  
  protected DV docValues;
  private final Function<DV> docValuesCall;
  
  public DocValuesTermsCollector(Function<DV> docValuesCall) {
    this.docValuesCall = docValuesCall;
  }

  @Override
  protected final void doSetNextReader(LeafReaderContext context) throws IOException {
    docValues = docValuesCall.apply(context.reader());
  }
  
  static Function<BinaryDocValues> binaryDocValues(String field) {
    return (ctx) -> DocValues.getBinary(ctx, field);
  }

  static Function<SortedSetDocValues> sortedSetDocValues(String field) {
    return (ctx) -> DocValues.getSortedSet(ctx, field);
  }
}
