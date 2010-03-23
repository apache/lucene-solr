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


class ParallelPostingsArray {
  final static int BYTES_PER_POSTING = 3 * DocumentsWriter.INT_NUM_BYTE;

  final int[] textStarts;
  final int[] intStarts;
  final int[] byteStarts;
  
  public ParallelPostingsArray(final int size) {
    textStarts = new int[size];
    intStarts = new int[size];
    byteStarts = new int[size];
  }
  
  ParallelPostingsArray resize(int newSize) {
    ParallelPostingsArray newArray = new ParallelPostingsArray(newSize);
    copy(this, newArray);
    return newArray;
  }
  
  void copy(ParallelPostingsArray fromArray, ParallelPostingsArray toArray) {
    System.arraycopy(fromArray.textStarts, 0, toArray.textStarts, 0, fromArray.textStarts.length);
    System.arraycopy(fromArray.intStarts, 0, toArray.intStarts, 0, fromArray.intStarts.length);
    System.arraycopy(fromArray.byteStarts, 0, toArray.byteStarts, 0, fromArray.byteStarts.length);
  }
}
