package org.apache.lucene.store;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * NIO version of FSDirectory.  Uses FileChannel.read(ByteBuffer dst, long position) method
 * which allows multiple threads to read from the file without synchronizing.  FSDirectory
 * synchronizes in the FSIndexInput.readInternal method which can cause pileups when there
 * are many threads accessing the Directory concurrently.  
 *
 * This class only uses FileChannel when reading; writing
 * with an IndexOutput is inherited from FSDirectory.
 * 
 * Note: NIOFSDirectory is not recommended on Windows because of a bug
 * in how FileChannel.read is implemented in Sun's JRE.
 * Inside of the implementation the position is apparently
 * synchronized.  See here for details:

 * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6265734 
 * 
 * @see FSDirectory
 */

public class NIOFSDirectory extends FSDirectory {

  // Inherit javadoc
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    ensureOpen();
    return new NIOFSIndexInput(new File(getFile(), name), bufferSize);
  }

  private static class NIOFSIndexInput extends FSDirectory.FSIndexInput {

    private ByteBuffer byteBuf; // wraps the buffer for NIO

    private byte[] otherBuffer;
    private ByteBuffer otherByteBuf;

    final FileChannel channel;

    public NIOFSIndexInput(File path, int bufferSize) throws IOException {
      super(path, bufferSize);
      channel = file.getChannel();
    }

    protected void newBuffer(byte[] newBuffer) {
      super.newBuffer(newBuffer);
      byteBuf = ByteBuffer.wrap(newBuffer);
    }

    public void close() throws IOException {
      if (!isClone && file.isOpen) {
        // Close the channel & file
        try {
          channel.close();
        } finally {
          file.close();
        }
      }
    }

    protected void readInternal(byte[] b, int offset, int len) throws IOException {

      final ByteBuffer bb;

      // Determine the ByteBuffer we should use
      if (b == buffer && 0 == offset) {
        // Use our own pre-wrapped byteBuf:
        assert byteBuf != null;
        byteBuf.clear();
        byteBuf.limit(len);
        bb = byteBuf;
      } else {
        if (offset == 0) {
          if (otherBuffer != b) {
            // Now wrap this other buffer; with compound
            // file, we are repeatedly called with its
            // buffer, so we wrap it once and then re-use it
            // on subsequent calls
            otherBuffer = b;
            otherByteBuf = ByteBuffer.wrap(b);
          } else
            otherByteBuf.clear();
          otherByteBuf.limit(len);
          bb = otherByteBuf;
        } else
          // Always wrap when offset != 0
          bb = ByteBuffer.wrap(b, offset, len);
      }

      long pos = getFilePointer();
      while (bb.hasRemaining()) {
        int i = channel.read(bb, pos);
        if (i == -1)
          throw new IOException("read past EOF");
        pos += i;
      }
    }
  }
}
