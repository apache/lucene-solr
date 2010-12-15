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
import org.apache.lucene.util.pfor.ForDecompress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class FORIndexInput extends FixedIntBlockIndexInput {

  public FORIndexInput(Directory dir, String fileName, int readBufferSize) throws IOException {
    super(dir.openInput(fileName, readBufferSize));
  }

  private static class BlockReader implements FixedIntBlockIndexInput.BlockReader {
    private final IndexInput in;
    private final int[] buffer;
    private final ForDecompress decompressor;
    private final byte[] input;
    private final IntBuffer intInput;

    public BlockReader(IndexInput in, int[] buffer) {
      this.in = in;
      this.buffer = buffer;

      decompressor = new ForDecompress();
      // nocommit -- can't hardwire 1024; it's a function of blockSize
      ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
      input = byteBuffer.array();
      intInput = byteBuffer.asIntBuffer();
      decompressor.setCompressedBuffer(intInput);
      decompressor.setUnCompressedData(buffer, 0, buffer.length);
    }

    public void seek(long pos) throws IOException {
      //System.out.println("for: seek pos=" + pos);
    }

    public void readBlock() throws IOException {
      // nocommit -- we don't need this numBytes header --
      // it's a waste.  we need something like the zip
      // interface -- the decompressor asks for more bytes
      // if it needs it
      //System.out.println("for: read @ fp=" + in.getFilePointer());
      int numBytes = in.readInt();
      // nocommit -- how to avoid this copy?  plus, the copy
      // inside BII.  if mmapdir how can we directly access
      // underlying ram w/ no copy?
      in.readBytes(input, 0, numBytes);
      intInput.rewind();
      decompressor.decompress();
      //System.out.println("  FOR.readBlock");
    }
  }

  protected FixedIntBlockIndexInput.BlockReader getBlockReader(IndexInput in, int[] buffer) {
    return new BlockReader(in, buffer);
  }
}
