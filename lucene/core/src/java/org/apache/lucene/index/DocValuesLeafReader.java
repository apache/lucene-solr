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

import org.apache.lucene.util.Bits;

abstract class DocValuesLeafReader extends LeafReader {
  @Override
  public final CacheHelper getCoreCacheHelper() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final Terms terms(String field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final NumericDocValues getNormValues(String field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final Bits getLiveDocs() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final PointValues getPointValues(String field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void checkIntegrity() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final LeafMetaData getMetaData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final Fields getTermVectors(int docID) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final int numDocs() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final int maxDoc() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void document(int docID, StoredFieldVisitor visitor) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected final void doClose() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public final CacheHelper getReaderCacheHelper() {
    throw new UnsupportedOperationException();
  }
}
