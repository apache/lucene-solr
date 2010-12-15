package org.apache.lucene.index.codecs.pfordelta;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexInput;
import org.apache.lucene.util.pfor.PFor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class PForDeltaIndexInput extends FixedIntBlockIndexInput {

  public PForDeltaIndexInput(Directory dir, String fileName, int readBufferSize) throws IOException {
    super(dir.openInput(fileName, readBufferSize));
  }

  private static class BlockReader implements FixedIntBlockIndexInput.BlockReader {
    private final IndexInput in;
    private final int[] buffer;
    private final PFor decompressor;
    private final byte[] input;
    private final IntBuffer intInput;

    public BlockReader(IndexInput in, int[] buffer) {
      this.in = in;
      this.buffer = buffer;

      decompressor = new PFor();
      // nocommit -- can't hardwire 1024; it's a function of blockSize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
      input = byteBuffer.array();
      intInput = byteBuffer.asIntBuffer();
      decompressor.setCompressedBuffer(intInput);
      decompressor.setUnCompressedData(buffer, 0, buffer.length);
    }

    public void seek(long pos) throws IOException {
      //
    }

    public void readBlock() throws IOException {
      int numBytes = in.readInt();
      //System.out.println("nb=" + numBytes);
      // nocommit -- how to avoid this copy?
      in.readBytes(input, 0, numBytes);
      intInput.rewind();
      decompressor.decompress();
    }
  }

  protected BlockReader getBlockReader(IndexInput in, int[] buffer) {
    return new BlockReader(in, buffer);
  }
}

