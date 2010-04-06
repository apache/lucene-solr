package org.apache.lucene.index.codecs.intblock;

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

/** Naive int block API that writes vInts.  This is
 *  expected to give poor performance; it's really only for
 *  testing the pluggability.  One should typically use pfor instead. */

import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Don't use this class!!  It naively encodes ints one vInt
 * at a time.  Use it only for testing.
 *
 * @lucene.experimental
 */
public class SimpleIntBlockIndexInput extends FixedIntBlockIndexInput {

  public SimpleIntBlockIndexInput(Directory dir, String fileName, int readBufferSize) throws IOException {
    IndexInput in = dir.openInput(fileName, readBufferSize);
    CodecUtil.checkHeader(in, SimpleIntBlockIndexOutput.CODEC, SimpleIntBlockIndexOutput.VERSION_START);
    init(in);
  }

  private static class BlockReader implements FixedIntBlockIndexInput.BlockReader {

    private final IndexInput in;
    private final int[] buffer;

    public BlockReader(IndexInput in, int[] buffer) {
      this.in = in;
      this.buffer = buffer;
    }

    public void readBlock() throws IOException {
      // silly impl
      for(int i=0;i<buffer.length;i++) {
        buffer[i] = in.readVInt();
      }
    }
  }

  @Override
  protected BlockReader getBlockReader(IndexInput in, int[] buffer) {
    return new BlockReader(in, buffer);
  }
}

