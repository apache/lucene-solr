package org.apache.lucene.store;

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

import java.io.IOException;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/** File-based {@link Directory} implementation that uses mmap for input.
 *
 * <p>To use this, invoke Java with the System property
 * org.apache.lucene.FSDirectory.class set to
 * org.apache.lucene.store.MMapDirectory.  This will cause {@link
 * FSDirectory#getDirectory(File,boolean)} to return instances of this class.
 *
 * @author Doug Cutting
 */
public class MMapDirectory extends FSDirectory {

  private class MMapIndexInput extends IndexInput {

    private ByteBuffer buffer;
    private RandomAccessFile file;
    private long length;
    private boolean isClone;

    public MMapIndexInput(String path) throws IOException {
      this.file = new RandomAccessFile(path, "r");
      this.length = file.length();
      this.buffer = file.getChannel().map(MapMode.READ_ONLY, 0, length);
    }

    public byte readByte() throws IOException {
      return buffer.get();
    }

    public void readBytes(byte[] b, int offset, int len)
      throws IOException {
      buffer.get(b, offset, len);
    }

    public long getFilePointer() {
      return buffer.position();
    }

    public void seek(long pos) throws IOException {
      buffer.position((int)pos);
    }

    public long length() {
      return length;
    }

    public Object clone() {
      MMapIndexInput clone = (MMapIndexInput)super.clone();
      clone.isClone = true;
      clone.buffer = buffer.duplicate();
      return clone;
    }

    public void close() throws IOException {
      if (!isClone)
        file.close();
    }
  }

  private MMapDirectory() {}                      // no public ctor

  public IndexInput openInput(String name) throws IOException {
    return new MMapIndexInput(new File(getFile(), name).getPath());
  }
}

