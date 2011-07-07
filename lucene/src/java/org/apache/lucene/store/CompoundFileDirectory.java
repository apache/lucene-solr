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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.IOUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 * @lucene.experimental
 */
public abstract class CompoundFileDirectory extends Directory {
  
  /** Offset/Length for a slice inside of a compound file */
  public static final class FileEntry {
    long offset;
    long length;
  }
  
  private final Directory directory;
  private final String fileName;
  private final int readBufferSize;  
  private Map<String,FileEntry> entries;
  private boolean openForWrite;
  private static final Map<String,FileEntry> SENTINEL = Collections.emptyMap();
  private CompoundFileWriter writer;
  
  /**
   * Create a new CompoundFileDirectory.
   * <p>
   * NOTE: subclasses must call {@link #initForRead(Map)} before the directory can be used.
   */
  public CompoundFileDirectory(Directory directory, String fileName, int readBufferSize) throws IOException {
    assert !(directory instanceof CompoundFileDirectory) : "compound file inside of compound file: " + fileName;
    this.directory = directory;
    this.fileName = fileName;
    this.readBufferSize = readBufferSize;
    this.isOpen = false;
  }
  
  /** Initialize with a map of filename->slices */
  protected final void initForRead(Map<String,FileEntry> entries) {
    this.entries = entries;
    this.isOpen = true;
    this.openForWrite = false;
  }
  
  protected final void initForWrite() {
    this.entries = SENTINEL;
    this.openForWrite = true;
    this.isOpen = true;
    writer = new CompoundFileWriter(directory, fileName);
  }
  
  /** Helper method that reads CFS entries from an input stream */
  public static final Map<String,FileEntry> readEntries(IndexInput stream, Directory dir, String name) throws IOException {
    // read the first VInt. If it is negative, it's the version number
    // otherwise it's the count (pre-3.1 indexes)
    final int firstInt = stream.readVInt();
    if (firstInt == CompoundFileWriter.FORMAT_CURRENT) {
      IndexInput input = null;
      try {
        input = dir.openInput(IndexFileNames.segmentFileName(IndexFileNames.stripExtension(name), "",
            IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
        final int readInt = input.readInt(); // unused right now
        assert readInt == CompoundFileWriter.ENTRY_FORMAT_CURRENT;
        final int numEntries = input.readVInt();
        final Map<String, FileEntry> mapping = new HashMap<String, CompoundFileDirectory.FileEntry>(
            numEntries);
        for (int i = 0; i < numEntries; i++) {
          final FileEntry fileEntry = new FileEntry();
          mapping.put(input.readString(), fileEntry);
          fileEntry.offset = input.readLong();
          fileEntry.length = input.readLong();
        }
        return mapping;
      } finally {
        IOUtils.closeSafely(true, input);
      }
    }
    
    // TODO remove once 3.x is not supported anymore
    return readLegacyEntries(stream, firstInt);
  }

  private static Map<String, FileEntry> readLegacyEntries(IndexInput stream,
      int firstInt) throws CorruptIndexException, IOException {
    final Map<String,FileEntry> entries = new HashMap<String,FileEntry>();
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
    long streamLength = stream.length();
    FileEntry entry = null;
    for (int i=0; i<count; i++) {
      long offset = stream.readLong();
      if (offset < 0 || offset > streamLength) {
        throw new CorruptIndexException("Invalid CFS entry offset: " + offset);
      }
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
      entry.length = streamLength - entry.offset;
    }
    
    return entries;
  }
  
  public Directory getDirectory() {
    return directory;
  }
  
  public String getName() {
    return fileName;
  }
  
  @Override
  public synchronized void close() throws IOException {
    ensureOpen();
    entries = null;
    isOpen = false;
    if (writer != null) {
      assert openForWrite;
      writer.close();
    }
  }
  
  @Override
  public synchronized IndexInput openInput(String id) throws IOException {
    // Default to readBufferSize passed in when we were opened
    return openInput(id, readBufferSize);
  }
  
  @Override
  public synchronized IndexInput openInput(String id, int readBufferSize) throws IOException {
    ensureOpen();
    assert !openForWrite;
    id = IndexFileNames.stripSegmentName(id);
    final FileEntry entry = entries.get(id);
    if (entry == null)
      throw new IOException("No sub-file with id " + id + " found (files: " + entries.keySet() + ")");
    
    return openInputSlice(id, entry.offset, entry.length, readBufferSize);
  }
  
  /** Return an IndexInput that represents a "slice" or portion of the CFS file. */
  public abstract IndexInput openInputSlice(String id, long offset, long length, int readBufferSize) throws IOException;
  
  /** Returns an array of strings, one for each file in the directory. */
  @Override
  public String[] listAll() {
    ensureOpen();
    String[] res;
    if (writer != null) {
      res = writer.listAll(); 
    } else {
      res = entries.keySet().toArray(new String[entries.size()]);
      // Add the segment name
      String seg = fileName.substring(0, fileName.indexOf('.'));
      for (int i = 0; i < res.length; i++) {
        res[i] = seg + res[i];
      }
    }
    return res;
  }
  
  /** Returns true iff a file with the given name exists. */
  @Override
  public boolean fileExists(String name) {
    ensureOpen();
    if (this.writer != null) {
      return writer.fileExists(name);
    }
    return entries.containsKey(IndexFileNames.stripSegmentName(name));
  }
  
  
  /** Returns the time the compound file was last modified. */
  @Override
  public long fileModified(String name) throws IOException {
    ensureOpen();
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
    ensureOpen();
    if (this.writer != null) {
      return writer.fileLenght(name);
    }
    FileEntry e = entries.get(IndexFileNames.stripSegmentName(name));
    if (e == null)
      throw new FileNotFoundException(name);
    return e.length;
  }
  
  @Override
  public IndexOutput createOutput(String name) throws IOException {
    ensureOpen();
    return writer.createOutput(name);
  }
  
  @Override
  public void sync(Collection<String> names) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  @Override
  public Lock makeLock(String name) {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException */
  @Override
  public final CompoundFileDirectory openCompoundInput(String name, int bufferSize) throws IOException {
    // NOTE: final to make nested compounding impossible.
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
  * @throws UnsupportedOperationException */
  @Override
  public CompoundFileDirectory createCompoundOutput(String name)
      throws IOException {
    // NOTE: final to make nested compounding impossible.
    throw new UnsupportedOperationException();
  }
  
}
