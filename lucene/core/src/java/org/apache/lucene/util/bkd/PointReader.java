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
package org.apache.lucene.util.bkd;


import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.util.BytesRef;

/** One pass iterator through all points previously written with a
 *  {@link PointWriter}, abstracting away whether points a read
 *  from (offline) disk or simple arrays in heap.
 *
 * @lucene.internal */
public abstract class PointReader implements Closeable {

  /** Returns false once iteration is done, else true. */
  public abstract boolean next() throws IOException;

  /** Returns the packed byte[] value */
  public abstract BytesRef packedValue();

  /** DocID for this point */
  public abstract int docID();

  /** Build histogram of the document at the provided byte position */
  public abstract void buildHistogram(int bytePosition, int[] histogram) throws IOException;

}

