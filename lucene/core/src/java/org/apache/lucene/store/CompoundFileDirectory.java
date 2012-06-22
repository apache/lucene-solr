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

import org.apache.lucene.codecs.Codec; // javadocs
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.LiveDocsFormat; // javadocs
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.DataOutput; // javadocs
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
 * <p>
 * All files belonging to a segment have the same name with varying extensions.
 * The extensions correspond to the different file formats used by the {@link Codec}. 
 * When using the Compound File format these files are collapsed into a 
 * single <tt>.cfs</tt> file (except for the {@link LiveDocsFormat}, with a 
 * corresponding <tt>.cfe</tt> file indexing its sub-files.
 * <p>
 * Files:
 * <ul>
 *    <li><tt>.cfs</tt>: An optional "virtual" file consisting of all the other 
 *    index files for systems that frequently run out of file handles.
 *    <li><tt>.cfe</tt>: The "virtual" compound file's entry table holding all 
 *    entries in the corresponding .cfs file.
 * </ul>
 * <p>Description:</p>
 * <ul>
 *   <li>Compound (.cfs) --&gt; Header, FileData <sup>FileCount</sup></li>
 *   <li>Compound Entry Table (.cfe) --&gt; Header, FileCount, &lt;FileName,
 *       DataOffset, DataLength&gt; <sup>FileCount</sup></li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>FileCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>DataOffset,DataLength --&gt; {@link DataOutput#writeLong UInt64}</li>
 *   <li>FileName --&gt; {@link DataOutput#writeString String}</li>
 *   <li>FileData --&gt; raw file data</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>FileCount indicates how many files are contained in this compound file. 
 *       The entry table that follows has that many entries. 
 *   <li>Each directory entry contains a long pointer to the start of this file's data
 *       section, the files length, and a String with that file's name.
 * </ul>
 * 
 * @lucene.experimental
 */
public final class CompoundFileDirectory extends Directory {
  
  /** Offset/Length for a slice inside of a compound file */
  public static final class FileEntry {
    long offset;
    long length;
  }
  
  private final Directory directory;
  private final String fileName;
  protected final int readBufferSize;  
  private final Map<String,FileEntry> entries;
  private final boolean openForWrite;
  private static final Map<String,FileEntry> SENTINEL = Collections.emptyMap();
  private final CompoundFileWriter writer;
  private final IndexInputSlicer handle;
  
  /**
   * Create a new CompoundFileDirectory.
   */
  public CompoundFileDirectory(Directory directory, String fileName, IOContext context, boolean openForWrite) throws IOException {
    this.directory = directory;
    this.fileName = fileName;
    this.readBufferSize = BufferedIndexInput.bufferSize(context);
    this.isOpen = false;
    this.openForWrite = openForWrite;
    if (!openForWrite) {
      boolean success = false;
      handle = directory.createSlicer(fileName, context);
      try {
        this.entries = readEntries(handle, directory, fileName);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(handle);
        }
      }
      this.isOpen = true;
      writer = null;
    } else {
      assert !(directory instanceof CompoundFileDirectory) : "compound file inside of compound file: " + fileName;
      this.entries = SENTINEL;
      this.isOpen = true;
      writer = new CompoundFileWriter(directory, fileName);
      handle = null;
    }
  }

  /** Helper method that reads CFS entries from an input stream */
  private static final Map<String, FileEntry> readEntries(
      IndexInputSlicer handle, Directory dir, String name) throws IOException {
    final IndexInput stream = handle.openFullSlice();
    final Map<String, FileEntry> mapping;
    boolean success = false;
    try {
      final int firstInt = stream.readInt();
      // NOTE: as long as we want to throw indexformattooold (vs corruptindexexception), we need
      // to read the magic ourselves. See SegmentInfos which also has this.
      if (firstInt == CodecUtil.CODEC_MAGIC) {
        CodecUtil.checkHeaderNoMagic(stream, CompoundFileWriter.DATA_CODEC, 
            CompoundFileWriter.VERSION_START, CompoundFileWriter.VERSION_START);
        IndexInput input = null;
        try {
          final String entriesFileName = IndexFileNames.segmentFileName(
                                                IndexFileNames.stripExtension(name), "",
                                                IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION);
          input = dir.openInput(entriesFileName, IOContext.READONCE);
          CodecUtil.checkHeader(input, CompoundFileWriter.ENTRY_CODEC, CompoundFileWriter.VERSION_START, CompoundFileWriter.VERSION_START);
          final int numEntries = input.readVInt();
          mapping = new HashMap<String, CompoundFileDirectory.FileEntry>(
              numEntries);
          for (int i = 0; i < numEntries; i++) {
            final FileEntry fileEntry = new FileEntry();
            final String id = input.readString();
            assert !mapping.containsKey(id): "id=" + id + " was written multiple times in the CFS";
            mapping.put(id, fileEntry);
            fileEntry.offset = input.readLong();
            fileEntry.length = input.readLong();
          }
          success = true;
          return mapping;
        } finally {
          IOUtils.close(input);
        }
      } else {
        throw new IndexFormatTooOldException(stream, firstInt,
            CodecUtil.CODEC_MAGIC, CodecUtil.CODEC_MAGIC);
      }
    } finally {
      if (success) {
        IOUtils.close(stream);
      } else {
        IOUtils.closeWhileHandlingException(stream);
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
    if (!isOpen) {
      // allow double close - usually to be consistent with other closeables
      return; // already closed
     }
    isOpen = false;
    if (writer != null) {
      assert openForWrite;
      writer.close();
    } else {
      IOUtils.close(handle);
    }
  }
  
  @Override
  public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    assert !openForWrite;
    final String id = IndexFileNames.stripSegmentName(name);
    final FileEntry entry = entries.get(id);
    if (entry == null) {
      throw new FileNotFoundException("No sub-file with id " + id + " found (fileName=" + name + " files: " + entries.keySet() + ")");
    }
    return handle.openSlice(name, entry.offset, entry.length);
  }
  
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
      String seg = IndexFileNames.parseSegmentName(fileName);
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
      return writer.fileLength(name);
    }
    FileEntry e = entries.get(IndexFileNames.stripSegmentName(name));
    if (e == null)
      throw new FileNotFoundException(name);
    return e.length;
  }
  
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    return writer.createOutput(name, context);
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

  @Override
  public IndexInputSlicer createSlicer(final String name, IOContext context)
      throws IOException {
    ensureOpen();
    assert !openForWrite;
    final String id = IndexFileNames.stripSegmentName(name);
    final FileEntry entry = entries.get(id);
    if (entry == null) {
      throw new FileNotFoundException("No sub-file with id " + id + " found (fileName=" + name + " files: " + entries.keySet() + ")");
    }
    return new IndexInputSlicer() {
      @Override
      public void close() throws IOException {
      }
      
      @Override
      public IndexInput openSlice(String sliceDescription, long offset, long length) throws IOException {
        return handle.openSlice(sliceDescription, entry.offset + offset, length);
      }

      @Override
      public IndexInput openFullSlice() throws IOException {
        return openSlice("full-slice", 0, entry.length);
      }
    };
  }

  @Override
  public String toString() {
    return "CompoundFileDirectory(file=\"" + fileName + "\" in dir=" + directory + ")";
  }
}
