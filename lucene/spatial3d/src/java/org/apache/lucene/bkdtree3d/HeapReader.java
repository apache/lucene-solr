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


final class HeapReader implements Reader {
  private int curRead;
  final int[] xs;
  final int[] ys;
  final int[] zs;
  final long[] ords;
  final int[] docIDs;
  final int end;

  HeapReader(int[] xs, int[] ys, int[] zs, long[] ords, int[] docIDs, int start, int end) {
    this.xs = xs;
    this.ys = ys;
    this.zs = zs;
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
  public int x() {
    return xs[curRead];
  }

  @Override
  public int y() {
    return ys[curRead];
  }

  @Override
  public int z() {
    return zs[curRead];
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
