package org.apache.lucene.index;

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
