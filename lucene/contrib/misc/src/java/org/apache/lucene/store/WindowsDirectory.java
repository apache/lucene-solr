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
import java.io.EOFException;

import org.apache.lucene.store.Directory; // javadoc
import org.apache.lucene.store.NativeFSLockFactory; // javadoc

/**
 * Native {@link Directory} implementation for Microsoft Windows.
 * <p>
 * Steps:
 * <ol> 
 *   <li>Compile the source code to create WindowsDirectory.dll:
 *       <blockquote>
 * c:\mingw\bin\g++ -Wall -D_JNI_IMPLEMENTATION_ -Wl,--kill-at 
 * -I"%JAVA_HOME%\include" -I"%JAVA_HOME%\include\win32" -static-libgcc 
 * -static-libstdc++ -shared WindowsDirectory.cpp -o WindowsDirectory.dll
 *       </blockquote> 
 *       For 64-bit JREs, use mingw64, with the -m64 option. 
 *   <li>Put WindowsDirectory.dll into some directory in your windows PATH
 *   <li>Open indexes with WindowsDirectory and use it.
 * </p>
 * @lucene.experimental
 */
public class WindowsDirectory extends FSDirectory {
  private static final int DEFAULT_BUFFERSIZE = 4096; /* default pgsize on ia32/amd64 */
  
  static {
    System.loadLibrary("WindowsDirectory");
  }
  
  /** Create a new WindowsDirectory for the named location.
   * 
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException
   */
  public WindowsDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
  }

  /** Create a new WindowsDirectory for the named location and {@link NativeFSLockFactory}.
   *
   * @param path the path of the directory
   * @throws IOException
   */
  public WindowsDirectory(File path) throws IOException {
    super(path, null);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    return new WindowsIndexInput(new File(getDirectory(), name), Math.max(BufferedIndexInput.bufferSize(context), DEFAULT_BUFFERSIZE));
  }
  
  protected static class WindowsIndexInput extends BufferedIndexInput {
    private final long fd;
    private final long length;
    boolean isClone;
    boolean isOpen;
    
    public WindowsIndexInput(File file, int bufferSize) throws IOException {
      super("WindowsIndexInput(path=\"" + file.getPath() + "\")", bufferSize);
      fd = WindowsDirectory.open(file.getPath());
      length = WindowsDirectory.length(fd);
      isOpen = true;
    }
    
    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      int bytesRead;
      try {
        bytesRead = WindowsDirectory.read(fd, b, offset, length, getFilePointer());
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + this, ioe);
      }

      if (bytesRead != length) {
        throw new EOFException("read past EOF: " + this);
      }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
    }

    @Override
    public synchronized void close() throws IOException {
      // NOTE: we synchronize and track "isOpen" because Lucene sometimes closes IIs twice!
      if (!isClone && isOpen) {
        WindowsDirectory.close(fd);
        isOpen = false;
      }
    }

    @Override
    public long length() {
      return length;
    }
    
    @Override
    public WindowsIndexInput clone() {
      WindowsIndexInput clone = (WindowsIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
  }
  
  /** Opens a handle to a file. */
  private static native long open(String filename) throws IOException;
  
  /** Reads data from a file at pos into bytes */
  private static native int read(long fd, byte bytes[], int offset, int length, long pos) throws IOException;
  
  /** Closes a handle to a file */
  private static native void close(long fd) throws IOException;
  
  /** Returns the length of a file */
  private static native long length(long fd) throws IOException;
}
