package org.apache.lucene.codecs.compressing;

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

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;

/**
 * An uncompressor.
 */
abstract class Uncompressor implements Cloneable {

  /**
   * Uncompress bytes. This method is free to resize <code>bytes</code> in case
   * it is too small to hold all the uncompressed data.
   */
  public abstract void uncompress(DataInput in, BytesRef bytes) throws IOException;

  /**
   * Method to use if you are only interested into <code>length</code>
   * uncompressed bytes starting at offset <code>offset</code>. Some compression
   * codecs might have optimizations for this special case.
   */
  public void uncompress(DataInput in, int offset, int length, BytesRef bytes) throws IOException {
    uncompress(in, bytes);
    if (bytes.length < offset + length) {
      throw new IndexOutOfBoundsException((offset + length) + " > " + bytes.length);
    }
    bytes.offset += offset;
    bytes.length = length;
  }

  public abstract void copyCompressedData(DataInput in, DataOutput out) throws IOException;

  @Override
  public abstract Uncompressor clone();

}
