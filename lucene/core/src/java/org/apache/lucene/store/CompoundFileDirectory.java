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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
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

  private static final byte CODEC_MAGIC_BYTE1 = (byte) (CodecUtil.CODEC_MAGIC >>> 24);
  private static final byte CODEC_MAGIC_BYTE2 = (byte) (CodecUtil.CODEC_MAGIC >>> 16);
  private static final byte CODEC_MAGIC_BYTE3 = (byte) (CodecUtil.CODEC_MAGIC >>> 8);
  private static final byte CODEC_MAGIC_BYTE4 = (byte) CodecUtil.CODEC_MAGIC;

  /** Helper method that reads CFS entries from an input stream */
  private static final Map<String, FileEntry> readEntries(
      IndexInputSlicer handle, Directory dir, String name) throws IOException {
    IOException priorE = null;
    IndexInput stream = null, entriesStream = null;
    // read the first VInt. If it is negative, it's the version number
    // otherwise it's the count (pre-3.1 indexes)
    try {
      final Map<String,FileEntry> mapping;
      stream = handle.openFullSlice();
      final int firstInt = stream.readVInt();
      // impossible for 3.0 to have 63 files in a .cfs, CFS writer was not visible
      // and separate norms/etc are outside of cfs.
      if (firstInt == CODEC_MAGIC_BYTE1) {
        byte secondByte = stream.readByte();
        byte thirdByte = stream.readByte();
        byte fourthByte = stream.readByte();
        if (secondByte != CODEC_MAGIC_BYTE2 || 
            thirdByte != CODEC_MAGIC_BYTE3 || 
            fourthByte != CODEC_MAGIC_BYTE4) {
          throw new CorruptIndexException("Illegal/impossible header for CFS file: " 
                                         + secondByte + "," + thirdByte + "," + fourthByte);
        }
        CodecUtil.checkHeaderNoMagic(stream, CompoundFileWriter.DATA_CODEC, 
            CompoundFileWriter.VERSION_START, CompoundFileWriter.VERSION_START);
        final String entriesFileName = IndexFileNames.segmentFileName(
                                              IndexFileNames.stripExtension(name), "",
                                              IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION);
        entriesStream = dir.openInput(entriesFileName, IOContext.READONCE);
        CodecUtil.checkHeader(entriesStream, CompoundFileWriter.ENTRY_CODEC, CompoundFileWriter.VERSION_START, CompoundFileWriter.VERSION_START);
        final int numEntries = entriesStream.readVInt();
        mapping = new HashMap<String,FileEntry>(numEntries);
        for (int i = 0; i < numEntries; i++) {
          final FileEntry fileEntry = new FileEntry();
          final String id = entriesStream.readString();
          FileEntry previous = mapping.put(id, fileEntry);
          if (previous != null) {
            throw new CorruptIndexException("Duplicate cfs entry id=" + id + " in CFS: " + entriesStream);
          }
          fileEntry.offset = entriesStream.readLong();
          fileEntry.length = entriesStream.readLong();
        }
      } else {
        // TODO remove once 3.x is not supported anymore
        mapping = readLegacyEntries(stream, firstInt);
      }
      return mapping;
    } catch (IOException ioe) {
      priorE = ioe;
    } finally {
      IOUtils.closeWhileHandlingException(priorE, stream, entriesStream);
    }
    // this is needed until Java 7's real try-with-resources:
    throw new AssertionError("impossible to get here");
  }

  private static Map<String, FileEntry> readLegacyEntries(IndexInput stream,
      int firstInt) throws CorruptIndexException, IOException {
    final Map<String,FileEntry> entries = new HashMap<String,FileEntry>();
    final int count;
    final boolean stripSegmentName;
    if (firstInt < CompoundFileWriter.FORMAT_PRE_VERSION) {
      if (firstInt < CompoundFileWriter.FORMAT_NO_SEGMENT_PREFIX) {
        throw new CorruptIndexException("Incompatible format version: "
            + firstInt + " expected >= " + CompoundFileWriter.FORMAT_NO_SEGMENT_PREFIX + " (resource: " + stream + ")");
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
        throw new CorruptIndexException("Invalid CFS entry offset: " + offset + " (resource: " + stream + ")");
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

      FileEntry previous = entries.put(id, entry);
      if (previous != null) {
        throw new CorruptIndexException("Duplicate cfs entry id=" + id + " in CFS: " + stream);
      }
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
   * @throws UnsupportedOperationException always: not supported by CFS */
  @Override
  public void deleteFile(String name) {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException always: not supported by CFS */
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
  public void sync(Collection<String> names) {
    throw new UnsupportedOperationException();
  }
  
  /** Not implemented
   * @throws UnsupportedOperationException always: not supported by CFS */
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
      public void close() {
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
