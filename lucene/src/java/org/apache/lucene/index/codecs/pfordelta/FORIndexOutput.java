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

/** Naive int block API that writes vInts.  This is
 *  expected to give poor performance; it's really only for
 *  testing the pluggability.  One should typically use pfor instead. */

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.pfor.ForCompress;
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Don't use this class!!  It naively encodes ints one vInt
 *  at a time.  Use it only for testing.  */
public class FORIndexOutput extends FixedIntBlockIndexOutput {

  public final static String CODEC = "P_FOR_DELTA";
  public final static int VERSION_START = 0;
  public final static int VERSION_CURRENT = VERSION_START;
  private final ForCompress compressor;
  private final byte[] output;

  // nocommit
  private final String fileName;

  public FORIndexOutput(Directory dir, String fileName, int blockSize) throws IOException {
    super(dir.createOutput(fileName), blockSize);

    // nocommit
    this.fileName = fileName;
    compressor = new ForCompress();
    // nocommit -- can't hardwire 1024; it's a function of blockSize
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    output = byteBuffer.array();
    compressor.setCompressedBuffer(byteBuffer.asIntBuffer());
  }

  @Override
  protected void flushBlock() throws IOException {
    // make sure output is always aligned to int
    assert (out.getFilePointer() & 3) == 0;
    compressor.setUnCompressedData(buffer, 0, buffer.length);
    final int numFrameBits = compressor.frameBitsForCompression();
    compressor.compress();
    final int numBytes = compressor.compressedSize() * 4;
    assert numBytes <= 1024;
    out.writeBytes(output, numBytes);
  }
}
