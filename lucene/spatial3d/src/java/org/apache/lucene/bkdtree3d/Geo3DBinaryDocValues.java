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
package org.apache.lucene.bkdtree3d;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

/* @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
class Geo3DBinaryDocValues extends BinaryDocValues {
  final BKD3DTreeReader bkdTreeReader;
  final BinaryDocValues delegate;
  final double planetMax;

  public Geo3DBinaryDocValues(BKD3DTreeReader bkdTreeReader, BinaryDocValues delegate, double planetMax) {
    this.bkdTreeReader = bkdTreeReader;
    this.delegate = delegate;
    this.planetMax = planetMax;
  }

  public BKD3DTreeReader getBKD3DTreeReader() {
    return bkdTreeReader;
  }

  @Override
  public BytesRef get(int docID) {
    return delegate.get(docID);
  }
}
