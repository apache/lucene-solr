package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.store.InputStream;

import java.io.IOException;

public class MockInputStream extends InputStream {
    private byte[] buffer;
    private int pointer = 0;

    public MockInputStream(byte[] bytes) {
        buffer = bytes;
        length = bytes.length;
    }

    protected void readInternal(byte[] dest, int destOffset, int len)
            throws IOException {
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

    public void close() throws IOException {
        // ignore
    }

    protected void seekInternal(long pos) throws IOException {
        pointer = (int) pos;
    }
}
