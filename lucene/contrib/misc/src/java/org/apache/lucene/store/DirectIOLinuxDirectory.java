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
import java.io.FileInputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.lucene.index.IOContext;
import org.apache.lucene.store.Directory; // javadoc
import org.apache.lucene.store.NativeFSLockFactory; // javadoc

/**
 * An {@link Directory} implementation that uses the
 * Linux-specific O_DIRECT flag to bypass all OS level
 * caching.  To use this you must compile
 * NativePosixUtil.cpp (exposes Linux-specific APIs through
 * JNI) for your platform.
 *
 * <p><b>WARNING</b>: this code is very new and quite easily
 * could contain horrible bugs.  For example, here's one
 * known issue: if you use seek in IndexOutput, and then
 * write more than one buffer's worth of bytes, then the
 * file will be wrong.  Lucene does not do this (only writes
 * small number of bytes after seek).

 * @lucene.experimental
 */
public class DirectIOLinuxDirectory extends FSDirectory {

  private final static long ALIGN = 512;
  private final static long ALIGN_NOT_MASK = ~(ALIGN-1);

  private final int forcedBufferSize;

  /** Create a new NIOFSDirectory for the named location.
   * 
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @param forcedBufferSize if this is 0, just use Lucene's
   *    default buffer size; else, force this buffer size.
   *    For best performance, force the buffer size to
   *    something fairly large (eg 1 MB), but note that this
   *    will eat up the JRE's direct buffer storage space
   * @throws IOException
   */
  public DirectIOLinuxDirectory(File path, LockFactory lockFactory, int forcedBufferSize) throws IOException {
    super(path, lockFactory);
    this.forcedBufferSize = forcedBufferSize;
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    //nocommit - use buffer based on IOContext
    return new DirectIOLinuxIndexInput(new File(getDirectory(), name), forcedBufferSize == 0 ? BufferedIndexInput.BUFFER_SIZE : forcedBufferSize);
  }

  @Override
  public IndexOutput createOutput(String name,IOContext context) throws IOException {
    ensureOpen();
    ensureCanWrite(name);
    //nocommit - use buffer based on IOContext
    return new DirectIOLinuxIndexOutput(new File(getDirectory(), name), forcedBufferSize == 0 ? BufferedIndexOutput.BUFFER_SIZE : forcedBufferSize);
  }

  private final static class DirectIOLinuxIndexOutput extends IndexOutput {
    private final ByteBuffer buffer;
    private final FileOutputStream fos;
    private final FileChannel channel;
    private final int bufferSize;

    //private final File path;

    private int bufferPos;
    private long filePos;
    private long fileLength;
    private boolean isOpen;

    public DirectIOLinuxIndexOutput(File path, int bufferSize) throws IOException {
      //this.path = path;
      FileDescriptor fd = NativePosixUtil.open_direct(path.toString(), false);
      fos = new FileOutputStream(fd);
      //fos = new FileOutputStream(path);
      channel = fos.getChannel();
      buffer = ByteBuffer.allocateDirect(bufferSize);
      this.bufferSize = bufferSize;
      isOpen = true;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      assert bufferPos == buffer.position(): "bufferPos=" + bufferPos + " vs buffer.position()=" + buffer.position();
      buffer.put(b);
      if (++bufferPos == bufferSize) {
        dump();
      }
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
      int toWrite = len;
      while(true) {
        final int left = bufferSize - bufferPos;
        if (left <= toWrite) {
          buffer.put(src, offset, left);
          toWrite -= left;
          offset += left;
          bufferPos = bufferSize;
          dump();
        } else {
          buffer.put(src, offset, toWrite);
          bufferPos += toWrite;
          break;
        }
      }
    }

    //@Override
    //public void setLength() throws IOException {
    //   TODO -- how to impl this?  neither FOS nor
    //   FileChannel provides an API?
    //}

    @Override
    public void flush() throws IOException {
      // TODO -- I don't think this method is necessary?
    }

    private void dump() throws IOException {
      buffer.flip();
      final long limit = filePos + buffer.limit();
      if (limit > fileLength) {
        // this dump extends the file
        fileLength = limit;
      } else {
        // we had seek'd back & wrote some changes
      }

      // must always round to next block
      buffer.limit((int) ((buffer.limit() + ALIGN - 1) & ALIGN_NOT_MASK));

      assert (buffer.limit() & ALIGN_NOT_MASK) == buffer.limit() : "limit=" + buffer.limit() + " vs " + (buffer.limit() & ALIGN_NOT_MASK);
      assert (filePos & ALIGN_NOT_MASK) == filePos;
      //System.out.println(Thread.currentThread().getName() + ": dump to " + filePos + " limit=" + buffer.limit() + " fos=" + fos);
      channel.write(buffer, filePos);
      filePos += bufferPos;
      bufferPos = 0;
      buffer.clear();
      //System.out.println("dump: done");

      // TODO: the case where we'd seek'd back, wrote an
      // entire buffer, we must here read the next buffer;
      // likely Lucene won't trip on this since we only
      // write smallish amounts on seeking back
    }

    @Override
    public long getFilePointer() {
      return filePos + bufferPos;
    }

    // TODO: seek is fragile at best; it can only properly
    // handle seek & then change bytes that fit entirely
    // within one buffer
    @Override
    public void seek(long pos) throws IOException {
      if (pos != getFilePointer()) {
        dump();
        final long alignedPos = pos & ALIGN_NOT_MASK;
        filePos = alignedPos;
        int n = (int) NativePosixUtil.pread(fos.getFD(), filePos, buffer);
        if (n < bufferSize) {
          buffer.limit(n);
        }
        //System.out.println("seek refill=" + n);
        final int delta = (int) (pos - alignedPos);
        buffer.position(delta);
        bufferPos = delta;
      }
    }

    @Override
    public long length() throws IOException {
      return fileLength;
    }

    @Override
    public void close() throws IOException {
      if (isOpen) {
        isOpen = false;
        try {
          dump();
        } finally {
          try {
            //System.out.println("direct close set len=" + fileLength + " vs " + channel.size() + " path=" + path);
            channel.truncate(fileLength);
            //System.out.println("  now: " + channel.size());
          } finally {
            try {
              channel.close();
            } finally {
              fos.close();
              //System.out.println("  final len=" + path.length());
            }
          }
        }
      }
    }
  }

  private final static class DirectIOLinuxIndexInput extends IndexInput {
    private final ByteBuffer buffer;
    private final FileInputStream fis;
    private final FileChannel channel;
    private final int bufferSize;

    private boolean isOpen;
    private boolean isClone;
    private long filePos;
    private int bufferPos;

    public DirectIOLinuxIndexInput(File path, int bufferSize) throws IOException {
      FileDescriptor fd = NativePosixUtil.open_direct(path.toString(), true);
      fis = new FileInputStream(fd);
      channel = fis.getChannel();
      this.bufferSize = bufferSize;
      buffer = ByteBuffer.allocateDirect(bufferSize);
      isOpen = true;
      isClone = false;
      filePos = -bufferSize;
      bufferPos = bufferSize;
      //System.out.println("D open " + path + " this=" + this);
    }

    // for clone
    public DirectIOLinuxIndexInput(DirectIOLinuxIndexInput other) throws IOException {
      this.fis = null;
      channel = other.channel;
      this.bufferSize = other.bufferSize;
      buffer = ByteBuffer.allocateDirect(bufferSize);
      filePos = -bufferSize;
      bufferPos = bufferSize;
      isOpen = true;
      isClone = true;
      //System.out.println("D clone this=" + this);
      seek(other.getFilePointer());
    }

    @Override
    public void close() throws IOException {
      if (isOpen && !isClone) {
        try {
          channel.close();
        } finally {
          if (!isClone) {
            fis.close();
          }
        }
      }
    }

    @Override
    public long getFilePointer() {
      return filePos + bufferPos;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos != getFilePointer()) {
        final long alignedPos = pos & ALIGN_NOT_MASK;
        //System.out.println("seek pos=" + pos + " aligned=" + alignedPos + " bufferSize=" + bufferSize + " this=" + this);
        filePos = alignedPos-bufferSize;
        refill();
        
        final int delta = (int) (pos - alignedPos);
        buffer.position(delta);
        bufferPos = delta;
      }
    }

    @Override
    public long length() {
      try {
        return channel.size();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public byte readByte() throws IOException {
      // NOTE: we don't guard against EOF here... ie the
      // "final" buffer will typically be filled to less
      // than bufferSize
      if (bufferPos == bufferSize) {
        refill();
      }
      assert bufferPos == buffer.position() : "bufferPos=" + bufferPos + " vs buffer.position()=" + buffer.position();
      bufferPos++;
      return buffer.get();
    }

    private void refill() throws IOException {
      buffer.clear();
      filePos += bufferSize;
      bufferPos = 0;
      assert (filePos & ALIGN_NOT_MASK) == filePos : "filePos=" + filePos + " anded=" + (filePos & ALIGN_NOT_MASK);
      //System.out.println("X refill filePos=" + filePos);
      int n = channel.read(buffer, filePos);
      if (n < 0) {
        throw new IOException("eof");
      }
      buffer.rewind();
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      int toRead = len;
      //System.out.println("\nX readBytes len=" + len + " fp=" + getFilePointer() + " size=" + length() + " this=" + this);
      while(true) {
        final int left = bufferSize - bufferPos;
        if (left < toRead) {
          //System.out.println("  copy " + left);
          buffer.get(dst, offset, left);
          toRead -= left;
          offset += left;
          refill();
        } else {
          //System.out.println("  copy " + toRead);
          buffer.get(dst, offset, toRead);
          bufferPos += toRead;
          //System.out.println("  readBytes done");
          break;
        }
      }
    }

    @Override
    public Object clone() {
      try {
        return new DirectIOLinuxIndexInput(this);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}
