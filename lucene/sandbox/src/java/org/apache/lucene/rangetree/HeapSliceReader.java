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
package org.apache.lucene.rangetree;

final class HeapSliceReader implements SliceReader {
  private int curRead;
  final long[] values;
  final long[] ords;
  final int[] docIDs;
  final int end;

  HeapSliceReader(long[] values, long[] ords, int[] docIDs, int start, int end) {
    this.values = values;
    this.ords = ords;
    this.docIDs = docIDs;
    curRead = start-1;
    this.end = end;
  }

  @Override
  public boolean next() {
    curRead++;
    return curRead < end;
  }

  @Override
  public long value() {
    return values[curRead];
  }

  @Override
  public int docID() {
    return docIDs[curRead];
  }

  @Override
  public long ord() {
    return ords[curRead];
  }

  @Override
  public void close() {
  }
}

