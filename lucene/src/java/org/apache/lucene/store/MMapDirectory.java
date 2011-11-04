package org.apache.lucene.store;

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
 
import java.io.IOException;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.nio.channels.ClosedChannelException; // javadoc
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import java.lang.reflect.Method;

import org.apache.lucene.util.Constants;

/** File-based {@link Directory} implementation that uses
 *  mmap for reading, and {@link
 *  FSDirectory.FSIndexOutput} for writing.
 *
 * <p><b>NOTE</b>: memory mapping uses up a portion of the
 * virtual memory address space in your process equal to the
 * size of the file being mapped.  Before using this class,
 * be sure your have plenty of virtual address space, e.g. by
 * using a 64 bit JRE, or a 32 bit JRE with indexes that are
 * guaranteed to fit within the address space.
 * On 32 bit platforms also consult {@link #setMaxChunkSize}
 * if you have problems with mmap failing because of fragmented
 * address space. If you get an OutOfMemoryException, it is recommended
 * to reduce the chunk size, until it works.
 *
 * <p>Due to <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038">
 * this bug</a> in Sun's JRE, MMapDirectory's {@link IndexInput#close}
 * is unable to close the underlying OS file handle.  Only when GC
 * finally collects the underlying objects, which could be quite
 * some time later, will the file handle be closed.
 *
 * <p>This will consume additional transient disk usage: on Windows,
 * attempts to delete or overwrite the files will result in an
 * exception; on other platforms, which typically have a &quot;delete on
 * last close&quot; semantics, while such operations will succeed, the bytes
 * are still consuming space on disk.  For many applications this
 * limitation is not a problem (e.g. if you have plenty of disk space,
 * and you don't rely on overwriting files on Windows) but it's still
 * an important limitation to be aware of.
 *
 * <p>This class supplies the workaround mentioned in the bug report
 * (see {@link #setUseUnmap}), which may fail on
 * non-Sun JVMs. It forcefully unmaps the buffer on close by using
 * an undocumented internal cleanup functionality.
 * {@link #UNMAP_SUPPORTED} is <code>true</code>, if the workaround
 * can be enabled (with no guarantees).
 * <p>
 * <b>NOTE:</b> Accessing this class either directly or
 * indirectly from a thread while it's interrupted can close the
 * underlying channel immediately if at the same time the thread is
 * blocked on IO. The channel will remain closed and subsequent access
 * to {@link MMapDirectory} will throw a {@link ClosedChannelException}. 
 * </p>
 */
public class MMapDirectory extends FSDirectory {
  private boolean useUnmapHack = UNMAP_SUPPORTED;
  public static final int DEFAULT_MAX_BUFF = Constants.JRE_IS_64BIT ? (1 << 30) : (1 << 28);
  private int chunkSizePower;

  /** Create a new MMapDirectory for the named location.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException
   */
  public MMapDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
    setMaxChunkSize(DEFAULT_MAX_BUFF);
  }

  /** Create a new MMapDirectory for the named location and {@link NativeFSLockFactory}.
   *
   * @param path the path of the directory
   * @throws IOException
   */
  public MMapDirectory(File path) throws IOException {
    super(path, null);
    setMaxChunkSize(DEFAULT_MAX_BUFF);
  }

  /**
   * <code>true</code>, if this platform supports unmapping mmapped files.
   */
  public static final boolean UNMAP_SUPPORTED;
  static {
    boolean v;
    try {
      Class.forName("sun.misc.Cleaner");
      Class.forName("java.nio.DirectByteBuffer")
        .getMethod("cleaner");
      v = true;
    } catch (Exception e) {
      v = false;
    }
    UNMAP_SUPPORTED = v;
  }
  
  /**
   * This method enables the workaround for unmapping the buffers
   * from address space after closing {@link IndexInput}, that is
   * mentioned in the bug report. This hack may fail on non-Sun JVMs.
   * It forcefully unmaps the buffer on close by using
   * an undocumented internal cleanup functionality.
   * <p><b>NOTE:</b> Enabling this is completely unsupported
   * by Java and may lead to JVM crashes if <code>IndexInput</code>
   * is closed while another thread is still accessing it (SIGSEGV).
   * @throws IllegalArgumentException if {@link #UNMAP_SUPPORTED}
   * is <code>false</code> and the workaround cannot be enabled.
   */
  public void setUseUnmap(final boolean useUnmapHack) {
    if (useUnmapHack && !UNMAP_SUPPORTED)
      throw new IllegalArgumentException("Unmap hack not supported on this platform!");
    this.useUnmapHack=useUnmapHack;
  }
  
  /**
   * Returns <code>true</code>, if the unmap workaround is enabled.
   * @see #setUseUnmap
   */
  public boolean getUseUnmap() {
    return useUnmapHack;
  }
  
  /**
   * Try to unmap the buffer, this method silently fails if no support
   * for that in the JVM. On Windows, this leads to the fact,
   * that mmapped files cannot be modified or deleted.
   */
  final void cleanMapping(final ByteBuffer buffer) throws IOException {
    if (useUnmapHack) {
      try {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
          public Object run() throws Exception {
            final Method getCleanerMethod = buffer.getClass()
              .getMethod("cleaner");
            getCleanerMethod.setAccessible(true);
            final Object cleaner = getCleanerMethod.invoke(buffer);
            if (cleaner != null) {
              cleaner.getClass().getMethod("clean")
                .invoke(cleaner);
            }
            return null;
          }
        });
      } catch (PrivilegedActionException e) {
        final IOException ioe = new IOException("unable to unmap the mapped buffer");
        ioe.initCause(e.getCause());
        throw ioe;
      }
    }
  }
  
  /**
   * Sets the maximum chunk size (default is {@link Integer#MAX_VALUE} for
   * 64 bit JVMs and 256 MiBytes for 32 bit JVMs) used for memory mapping.
   * Especially on 32 bit platform, the address space can be very fragmented,
   * so large index files cannot be mapped.
   * Using a lower chunk size makes the directory implementation a little
   * bit slower (as the correct chunk may be resolved on lots of seeks)
   * but the chance is higher that mmap does not fail. On 64 bit
   * Java platforms, this parameter should always be {@code 1 << 30},
   * as the address space is big enough.
   * <b>Please note:</b> This method always rounds down the chunk size
   * to a power of 2.
   */
  public final void setMaxChunkSize(final int maxChunkSize) {
    if (maxChunkSize <= 0)
      throw new IllegalArgumentException("Maximum chunk size for mmap must be >0");
    //System.out.println("Requested chunk size: "+maxChunkSize);
    this.chunkSizePower = 31 - Integer.numberOfLeadingZeros(maxChunkSize);
    assert this.chunkSizePower >= 0 && this.chunkSizePower <= 30;
    //System.out.println("Got chunk size: "+getMaxChunkSize());
  }
  
  /**
   * Returns the current mmap chunk size.
   * @see #setMaxChunkSize
   */
  public final int getMaxChunkSize() {
    return 1 << chunkSizePower;
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    ensureOpen();
    File f = new File(getDirectory(), name);
    RandomAccessFile raf = new RandomAccessFile(f, "r");
    try {
      return new MMapIndexInput("MMapIndexInput(path=\"" + f + "\")", raf, chunkSizePower);
    } finally {
      raf.close();
    }
  }

  // Because Java's ByteBuffer uses an int to address the
  // values, it's necessary to access a file >
  // Integer.MAX_VALUE in size using multiple byte buffers.
  private final class MMapIndexInput extends IndexInput {
  
    private ByteBuffer[] buffers;
  
    private final long length, chunkSizeMask, chunkSize;
    private final int chunkSizePower;
  
    private int curBufIndex;
  
    private ByteBuffer curBuf; // redundant for speed: buffers[curBufIndex]
  
    private boolean isClone = false;
    
    MMapIndexInput(String resourceDescription, RandomAccessFile raf, int chunkSizePower) throws IOException {
      super(resourceDescription);
      this.length = raf.length();
      this.chunkSizePower = chunkSizePower;
      this.chunkSize = 1L << chunkSizePower;
      this.chunkSizeMask = chunkSize - 1L;
      
      if (chunkSizePower < 0 || chunkSizePower > 30)
        throw new IllegalArgumentException("Invalid chunkSizePower used for ByteBuffer size: " + chunkSizePower);
      
      if ((length >>> chunkSizePower) >= Integer.MAX_VALUE)
        throw new IllegalArgumentException("RandomAccessFile too big for chunk size: " + raf.toString());
      
      // we always allocate one more buffer, the last one may be a 0 byte one
      final int nrBuffers = (int) (length >>> chunkSizePower) + 1;
      
      //System.out.println("length="+length+", chunkSizePower=" + chunkSizePower + ", chunkSizeMask=" + chunkSizeMask + ", nrBuffers=" + nrBuffers);
      
      this.buffers = new ByteBuffer[nrBuffers];
      
      long bufferStart = 0L;
      FileChannel rafc = raf.getChannel();
      for (int bufNr = 0; bufNr < nrBuffers; bufNr++) { 
        int bufSize = (int) ( (length > (bufferStart + chunkSize))
          ? chunkSize
          : (length - bufferStart)
        );
        this.buffers[bufNr] = rafc.map(MapMode.READ_ONLY, bufferStart, bufSize);
        bufferStart += bufSize;
      }
      seek(0L);
    }
  
    @Override
    public byte readByte() throws IOException {
      try {
        return curBuf.get();
      } catch (BufferUnderflowException e) {
        do {
          curBufIndex++;
          if (curBufIndex >= buffers.length) {
            throw new IOException("read past EOF: " + this);
          }
          curBuf = buffers[curBufIndex];
          curBuf.position(0);
        } while (!curBuf.hasRemaining());
        return curBuf.get();
      }
    }
  
    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      try {
        curBuf.get(b, offset, len);
      } catch (BufferUnderflowException e) {
        int curAvail = curBuf.remaining();
        while (len > curAvail) {
          curBuf.get(b, offset, curAvail);
          len -= curAvail;
          offset += curAvail;
          curBufIndex++;
          if (curBufIndex >= buffers.length) {
            throw new IOException("read past EOF: " + this);
          }
          curBuf = buffers[curBufIndex];
          curBuf.position(0);
          curAvail = curBuf.remaining();
        }
        curBuf.get(b, offset, len);
      }
    }

    @Override
    public int readInt() throws IOException {
      try {
        return curBuf.getInt();
      } catch (BufferUnderflowException e) {
        return super.readInt();
      }
    }

    @Override
    public long readLong() throws IOException {
      try {
        return curBuf.getLong();
      } catch (BufferUnderflowException e) {
        return super.readLong();
      }
    }
    
    @Override
    public long getFilePointer() {
      return (((long) curBufIndex) << chunkSizePower) + curBuf.position();
    }
  
    @Override
    public void seek(long pos) throws IOException {
      // we use >> here to preserve negative, so we will catch AIOOBE:
      final int bi = (int) (pos >> chunkSizePower);
      try {
        final ByteBuffer b = buffers[bi];
        b.position((int) (pos & chunkSizeMask));
        // write values, on exception all is unchanged
        this.curBufIndex = bi;
        this.curBuf = b;
      } catch (ArrayIndexOutOfBoundsException aioobe) {
        if (pos < 0L) {
          throw new IllegalArgumentException("Seeking to negative position: " + this);
        }
        throw new IOException("seek past EOF");
      } catch (IllegalArgumentException iae) {
        if (pos < 0L) {
          throw new IllegalArgumentException("Seeking to negative position: " + this);
        }
        throw new IOException("seek past EOF: " + this);
      }
    }
  
    @Override
    public long length() {
      return length;
    }
  
    @Override
    public Object clone() {
      if (buffers == null) {
        throw new AlreadyClosedException("MMapIndexInput already closed: " + this);
      }
      final MMapIndexInput clone = (MMapIndexInput)super.clone();
      clone.isClone = true;
      clone.buffers = new ByteBuffer[buffers.length];
      // Since most clones will use only one buffer, duplicate() could also be
      // done lazy in clones, e.g. when adapting curBuf.
      for (int bufNr = 0; bufNr < buffers.length; bufNr++) {
        clone.buffers[bufNr] = buffers[bufNr].duplicate();
      }
      try {
        clone.seek(getFilePointer());
      } catch(IOException ioe) {
        throw new RuntimeException("Should never happen: " + this, ioe);
      }
      return clone;
    }
  
    @Override
    public void close() throws IOException {
      try {
        if (isClone || buffers == null) return;
        for (int bufNr = 0; bufNr < buffers.length; bufNr++) {
          // unmap the buffer (if enabled) and at least unset it for GC
          try {
            cleanMapping(buffers[bufNr]);
          } finally {
            buffers[bufNr] = null;
          }
        }
      } finally {
        buffers = null;
      }
    }
  }
}
