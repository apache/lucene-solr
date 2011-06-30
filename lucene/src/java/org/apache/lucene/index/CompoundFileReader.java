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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import java.util.Collection;
import java.util.HashMap;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 * @lucene.experimental
 */
public class CompoundFileReader extends Directory {
  
  private static final class FileEntry {
    long offset;
    long length;
  }
  
  // Base info
  private Directory directory;
  private String fileName;
  
  private IndexInput stream;
  private HashMap<String,FileEntry> entries = new HashMap<String,FileEntry>();

  public CompoundFileReader(Directory dir, String name, IOContext context) throws IOException {
    assert !(dir instanceof CompoundFileReader) : "compound file inside of compound file: " + name;
    directory = dir;
    fileName = name;
    
    boolean success = false;
    
    try {
      stream = dir.openInput(name, context);
      
      // read the first VInt. If it is negative, it's the version number
      // otherwise it's the count (pre-3.1 indexes)
      int firstInt = stream.readVInt();
      
      final int count;
      final boolean stripSegmentName;
      if (firstInt < CompoundFileWriter.FORMAT_PRE_VERSION) {
        if (firstInt < CompoundFileWriter.FORMAT_CURRENT) {
          throw new CorruptIndexException("Incompatible format version: "
              + firstInt + " expected " + CompoundFileWriter.FORMAT_CURRENT);
        }
        // It's a post-3.1 index, read the count.
        count = stream.readVInt();
        stripSegmentName = false;
      } else {
        count = firstInt;
        stripSegmentName = true;
      }
      
      // read the directory and init files
      FileEntry entry = null;
      for (int i=0; i<count; i++) {
        long offset = stream.readLong();
        String id = stream.readString();
        
        if (stripSegmentName) {
          // Fix the id to not include the segment names. This is relevant for
          // pre-3.1 indexes.
          id = IndexFileNames.stripSegmentName(id);
        }
        
        if (entry != null) {
          // set length of the previous entry
          entry.length = offset - entry.offset;
        }
        
        entry = new FileEntry();
        entry.offset = offset;
        entries.put(id, entry);
      }
      
      // set the length of the final entry
      if (entry != null) {
        entry.length = stream.length() - entry.offset;
      }
      
      success = true;
      
    } finally {
      if (!success && (stream != null)) {
        try {
          stream.close();
        } catch (IOException e) { }
      }
    }
  }
  
  public Directory getDirectory() {
    return directory;
  }
  
  public String getName() {
    return fileName;
  }
  
  @Override
  public synchronized void close() throws IOException {
    if (stream == null)
      throw new IOException("Already closed");
    
    entries.clear();
    stream.close();
    stream = null;
  }
  
  @Override
  public synchronized IndexInput openInput(String id, IOContext context) throws IOException {
    if (stream == null)
      throw new IOException("Stream closed");
    
    id = IndexFileNames.stripSegmentName(id);
    final FileEntry entry = entries.get(id);
    if (entry == null)
      throw new IOException("No sub-file with id " + id + " found (files: " + entries.keySet() + ")");
    // nocommit set read buffer size based on IOContext
    return new CSIndexInput(stream, entry.offset, entry.length, BufferedIndexInput.BUFFER_SIZE);
  }
  
  /** Returns an array of strings, one for each file in the directory. */
  @Override
  public String[] listAll() {
    String[] res = entries.keySet().toArray(new String[entries.size()]);
    // Add the segment name
    String seg = fileName.substring(0, fileName.indexOf('.'));
    for (int i = 0; i < res.length; i++) {
      res[i] = seg + res[i];
    }
    return res;
  }
  
  /** Returns true iff a file with the given name exists. */
  @Override
  public boolean fileExists(String name) {
    return entries.containsKey(IndexFileNames.stripSegmentName(name));
  }
  
  /** Returns the time the compound file was last modified. */
  @Override
  public long fileModified(String name) throws IOException {
    return directory.fileModified(fileName);
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  @Override
  public void deleteFile(String name) {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  public void renameFile(String from, String to) {
    throw new UnsupportedOperationException();
  }
  
  /** Returns the length of a file in the directory.
   * @throws IOException if the file does not exist */
  @Override
  public long fileLength(String name) throws IOException {
    FileEntry e = entries.get(IndexFileNames.stripSegmentName(name));
    if (e == null)
      throw new FileNotFoundException(name);
    return e.length;
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  @Override
  public IndexOutput createOutput(String name, IOContext context) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void sync(Collection<String> names) throws IOException {
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  @Override
  public Lock makeLock(String name) {
    throw new UnsupportedOperationException();
  }
  
  /** Implementation of an IndexInput that reads from a portion of the
   *  compound file. The visibility is left as "package" *only* because
   *  this helps with testing since JUnit test cases in a different class
   *  can then access package fields of this class.
   */
  static final class CSIndexInput extends BufferedIndexInput {
    IndexInput base;
    long fileOffset;
    long length;
    
    CSIndexInput(final IndexInput base, final long fileOffset, final long length) {
      this(base, fileOffset, length, BufferedIndexInput.BUFFER_SIZE);
    }
    
    CSIndexInput(final IndexInput base, final long fileOffset, final long length, int readBufferSize) {
      super(readBufferSize);
      this.base = (IndexInput)base.clone();
      this.fileOffset = fileOffset;
      this.length = length;
    }
    
    @Override
    public Object clone() {
      CSIndexInput clone = (CSIndexInput)super.clone();
      clone.base = (IndexInput)base.clone();
      clone.fileOffset = fileOffset;
      clone.length = length;
      return clone;
    }
    
    /** Expert: implements buffer refill.  Reads bytes from the current
     *  position in the input.
     * @param b the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param len the number of bytes to read
     */
    @Override
    protected void readInternal(byte[] b, int offset, int len) throws IOException {
      long start = getFilePointer();
      if(start + len > length)
        throw new IOException("read past EOF");
      base.seek(fileOffset + start);
      base.readBytes(b, offset, len, false);
    }
    
    /** Expert: implements seek.  Sets current position in this file, where
     *  the next {@link #readInternal(byte[],int,int)} will occur.
     * @see #readInternal(byte[],int,int)
     */
    @Override
    protected void seekInternal(long pos) {}
    
    /** Closes the stream to further operations. */
    @Override
    public void close() throws IOException {
      base.close();
    }
    
    @Override
    public long length() {
      return length;
    }
    
    @Override
    public void copyBytes(IndexOutput out, long numBytes) throws IOException {
      // Copy first whatever is in the buffer
      numBytes -= flushBuffer(out, numBytes);
      
      // If there are more bytes left to copy, delegate the copy task to the
      // base IndexInput, in case it can do an optimized copy.
      if (numBytes > 0) {
        long start = getFilePointer();
        if (start + numBytes > length) {
          throw new IOException("read past EOF");
        }
        base.seek(fileOffset + start);
        base.copyBytes(out, numBytes);
      }
    }
  }
}
