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
package org.apache.lucene.bkdtree;

final class HeapLatLonWriter implements LatLonWriter {
  final int[] latEncs;
  final int[] lonEncs;
  final int[] docIDs;
  final long[] ords;
  private int nextWrite;
  private boolean closed;

  public HeapLatLonWriter(int count) {
    latEncs = new int[count];
    lonEncs = new int[count];
    docIDs = new int[count];
    ords = new long[count];
  }

  @Override
  public void append(int latEnc, int lonEnc, long ord, int docID) {
    latEncs[nextWrite] = latEnc;
    lonEncs[nextWrite] = lonEnc;
    ords[nextWrite] = ord;
    docIDs[nextWrite] = docID;
    nextWrite++;
  }

  @Override
  public LatLonReader getReader(long start) {
    assert closed;
    return new HeapLatLonReader(latEncs, lonEncs, ords, docIDs, (int) start, latEncs.length);
  }

  @Override
  public void close() {
    closed = true;
    if (nextWrite != latEncs.length) {
      throw new IllegalStateException("only wrote " + nextWrite + " values, but expected " + latEncs.length);
    }
  }

  @Override
  public void destroy() {
  }

  @Override
  public String toString() {
    return "HeapLatLonWriter(count=" + latEncs.length + ")";
  }
}
