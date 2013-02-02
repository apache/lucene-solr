package org.apache.lucene.store;

/*
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
import java.nio.channels.ClosedChannelException; // javadoc @link
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
 * On 32 bit platforms also consult {@link #MMapDirectory(File, LockFactory, int)}
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
  /** 
   * Default max chunk size.
   * @see #MMapDirectory(File, LockFactory, int)
   */
  public static final int DEFAULT_MAX_BUFF = Constants.JRE_IS_64BIT ? (1 << 30) : (1 << 28);
  final int chunkSizePower;

  /** Create a new MMapDirectory for the named location.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException if there is a low-level I/O error
   */
  public MMapDirectory(File path, LockFactory lockFactory) throws IOException {
    this(path, lockFactory, DEFAULT_MAX_BUFF);
  }

  /** Create a new MMapDirectory for the named location and {@link NativeFSLockFactory}.
   *
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public MMapDirectory(File path) throws IOException {
    this(path, null);
  }
  
  /**
   * Create a new MMapDirectory for the named location, specifying the 
   * maximum chunk size used for memory mapping.
   * 
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @param maxChunkSize maximum chunk size (default is 1 GiBytes for
   * 64 bit JVMs and 256 MiBytes for 32 bit JVMs) used for memory mapping.
   * <p>
   * Especially on 32 bit platform, the address space can be very fragmented,
   * so large index files cannot be mapped. Using a lower chunk size makes 
   * the directory implementation a little bit slower (as the correct chunk 
   * may be resolved on lots of seeks) but the chance is higher that mmap 
   * does not fail. On 64 bit Java platforms, this parameter should always 
   * be {@code 1 << 30}, as the address space is big enough.
   * <p>
   * <b>Please note:</b> The chunk size is always rounded down to a power of 2.
   * @throws IOException if there is a low-level I/O error
   */
  public MMapDirectory(File path, LockFactory lockFactory, int maxChunkSize) throws IOException {
    super(path, lockFactory);
    if (maxChunkSize <= 0) {
      throw new IllegalArgumentException("Maximum chunk size for mmap must be >0");
    }
    this.chunkSizePower = 31 - Integer.numberOfLeadingZeros(maxChunkSize);
    assert this.chunkSizePower >= 0 && this.chunkSizePower <= 30;
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
   * Returns the current mmap chunk size.
   * @see #MMapDirectory(File, LockFactory, int)
   */
  public final int getMaxChunkSize() {
    return 1 << chunkSizePower;
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    File f = new File(getDirectory(), name);
    RandomAccessFile raf = new RandomAccessFile(f, "r");
    try {
      return new MMapIndexInput("MMapIndexInput(path=\"" + f + "\")", raf);
    } finally {
      raf.close();
    }
  }
  
  @Override
  public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
    final MMapIndexInput full = (MMapIndexInput) openInput(name, context);
    return new IndexInputSlicer() {
      @Override
      public IndexInput openSlice(String sliceDescription, long offset, long length) throws IOException {
        ensureOpen();
        return full.slice(sliceDescription, offset, length);
      }
      
      @Override
      public IndexInput openFullSlice() throws IOException {
        ensureOpen();
        return full.clone();
      }

      @Override
      public void close() throws IOException {
        full.close();
      }
    };
  }

  private final class MMapIndexInput extends ByteBufferIndexInput {
    private final boolean useUnmapHack;
    
    MMapIndexInput(String resourceDescription, RandomAccessFile raf) throws IOException {
      super(resourceDescription, map(raf, 0, raf.length()), raf.length(), chunkSizePower, getUseUnmap());
      this.useUnmapHack = getUseUnmap();
    }
    
    /**
     * Try to unmap the buffer, this method silently fails if no support
     * for that in the JVM. On Windows, this leads to the fact,
     * that mmapped files cannot be modified or deleted.
     */
    @Override
    protected void freeBuffer(final ByteBuffer buffer) throws IOException {
      if (useUnmapHack) {
        try {
          AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
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
  }
  
  /** Maps a file into a set of buffers */
  ByteBuffer[] map(RandomAccessFile raf, long offset, long length) throws IOException {
    if ((length >>> chunkSizePower) >= Integer.MAX_VALUE)
      throw new IllegalArgumentException("RandomAccessFile too big for chunk size: " + raf.toString());
    
    final long chunkSize = 1L << chunkSizePower;
    
    // we always allocate one more buffer, the last one may be a 0 byte one
    final int nrBuffers = (int) (length >>> chunkSizePower) + 1;
    
    ByteBuffer buffers[] = new ByteBuffer[nrBuffers];
    
    long bufferStart = 0L;
    FileChannel rafc = raf.getChannel();
    for (int bufNr = 0; bufNr < nrBuffers; bufNr++) { 
      int bufSize = (int) ( (length > (bufferStart + chunkSize))
          ? chunkSize
              : (length - bufferStart)
          );
      buffers[bufNr] = rafc.map(MapMode.READ_ONLY, offset + bufferStart, bufSize);
      bufferStart += bufSize;
    }
    
    return buffers;
  }
}
