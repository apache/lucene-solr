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
 */
public class MMapDirectory extends FSDirectory {

  private static class MMapIndexInput extends IndexInput {

    private ByteBuffer buffer;
    private final long length;

    private MMapIndexInput(RandomAccessFile raf) throws IOException {
        this.length = raf.length();
        this.buffer = raf.getChannel().map(MapMode.READ_ONLY, 0, length);
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
      clone.buffer = buffer.duplicate();
      return clone;
    }

    public void close() throws IOException {}
  }

  private static class MultiMMapIndexInput extends IndexInput {
  
    private ByteBuffer[] buffers;
    private int[] bufSizes; // keep here, ByteBuffer.size() method is optional
  
    private final long length;
  
    private int curBufIndex;
    private final int maxBufSize;
  
    private ByteBuffer curBuf; // redundant for speed: buffers[curBufIndex]
    private int curAvail; // redundant for speed: (bufSizes[curBufIndex] - curBuf.position())
  
    
    public MultiMMapIndexInput(RandomAccessFile raf, int maxBufSize)
      throws IOException {
      this.length = raf.length();
      this.maxBufSize = maxBufSize;
      
      if (maxBufSize <= 0)
        throw new IllegalArgumentException("Non positive maxBufSize: "
                                           + maxBufSize);
      
      if ((length / maxBufSize) > Integer.MAX_VALUE)
        throw new IllegalArgumentException
          ("RandomAccessFile too big for maximum buffer size: "
           + raf.toString());
      
      int nrBuffers = (int) (length / maxBufSize);
      if ((nrBuffers * maxBufSize) < length) nrBuffers++;
      
      this.buffers = new ByteBuffer[nrBuffers];
      this.bufSizes = new int[nrBuffers];
      
      long bufferStart = 0;
      FileChannel rafc = raf.getChannel();
      for (int bufNr = 0; bufNr < nrBuffers; bufNr++) { 
        int bufSize = (length > (bufferStart + maxBufSize))
          ? maxBufSize
          : (int) (length - bufferStart);
        this.buffers[bufNr] = rafc.map(MapMode.READ_ONLY,bufferStart,bufSize);
        this.bufSizes[bufNr] = bufSize;
        bufferStart += bufSize;
      }
      seek(0L);
    }
  
    public byte readByte() throws IOException {
      // Performance might be improved by reading ahead into an array of
      // eg. 128 bytes and readByte() from there.
      if (curAvail == 0) {
        curBufIndex++;
        curBuf = buffers[curBufIndex]; // index out of bounds when too many bytes requested
        curBuf.position(0);
        curAvail = bufSizes[curBufIndex];
      }
      curAvail--;
      return curBuf.get();
    }
  
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      while (len > curAvail) {
        curBuf.get(b, offset, curAvail);
        len -= curAvail;
        offset += curAvail;
        curBufIndex++;
        curBuf = buffers[curBufIndex]; // index out of bounds when too many bytes requested
        curBuf.position(0);
        curAvail = bufSizes[curBufIndex];
      }
      curBuf.get(b, offset, len);
      curAvail -= len;
    }
  
    public long getFilePointer() {
      return (curBufIndex * (long) maxBufSize) + curBuf.position();
    }
  
    public void seek(long pos) throws IOException {
      curBufIndex = (int) (pos / maxBufSize);
      curBuf = buffers[curBufIndex];
      int bufOffset = (int) (pos - (curBufIndex * maxBufSize));
      curBuf.position(bufOffset);
      curAvail = bufSizes[curBufIndex] - bufOffset;
    }
  
    public long length() {
      return length;
    }
  
    public Object clone() {
      MultiMMapIndexInput clone = (MultiMMapIndexInput)super.clone();
      clone.buffers = new ByteBuffer[buffers.length];
      // No need to clone bufSizes.
      // Since most clones will use only one buffer, duplicate() could also be
      // done lazy in clones, eg. when adapting curBuf.
      for (int bufNr = 0; bufNr < buffers.length; bufNr++) {
        clone.buffers[bufNr] = buffers[bufNr].duplicate();
      }
      try {
        clone.seek(getFilePointer());
      } catch(IOException ioe) {
        RuntimeException newException = new RuntimeException(ioe);
        newException.initCause(ioe);
        throw newException;
      };
      return clone;
    }
  
    public void close() throws IOException {}
  }
  
  private final int MAX_BBUF = Integer.MAX_VALUE;

  public IndexInput openInput(String name) throws IOException {
    File f =  new File(getFile(), name);
    RandomAccessFile raf = new RandomAccessFile(f, "r");
    try {
      return (raf.length() <= MAX_BBUF)
             ? (IndexInput) new MMapIndexInput(raf)
             : (IndexInput) new MultiMMapIndexInput(raf, MAX_BBUF);
    } finally {
      raf.close();
    }
  }
}
