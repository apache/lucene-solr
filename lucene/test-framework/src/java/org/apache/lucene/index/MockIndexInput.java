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

import org.apache.lucene.store.BufferedIndexInput;

// TODO: what is this used for? just testing BufferedIndexInput?
// if so it should be pkg-private. otherwise its a dup of ByteArrayIndexInput?
/**
 * IndexInput backed by a byte[] for testing.
 */
public class MockIndexInput extends BufferedIndexInput {
    private byte[] buffer;
    private int pointer = 0;
    private long length;

    public MockIndexInput(byte[] bytes) {
        super("MockIndexInput", BufferedIndexInput.BUFFER_SIZE);
        buffer = bytes;
        length = bytes.length;
    }

    @Override
    protected void readInternal(byte[] dest, int destOffset, int len) {
        int remainder = len;
        int start = pointer;
        while (remainder != 0) {
//          int bufferNumber = start / buffer.length;
          int bufferOffset = start % buffer.length;
          int bytesInBuffer = buffer.length - bufferOffset;
          int bytesToCopy = bytesInBuffer >= remainder ? remainder : bytesInBuffer;
          System.arraycopy(buffer, bufferOffset, dest, destOffset, bytesToCopy);
          destOffset += bytesToCopy;
          start += bytesToCopy;
          remainder -= bytesToCopy;
        }
        pointer += len;
    }

    @Override
    public void close() {
        // ignore
    }

    @Override
    protected void seekInternal(long pos) {
        pointer = (int) pos;
    }

    @Override
    public long length() {
      return length;
    }

}
