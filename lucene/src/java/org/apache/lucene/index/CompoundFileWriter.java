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

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.lucene.index.codecs.MergeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Combines multiple files into a single compound file.
 * The file format:<br>
 * <ul>
 *     <li>VInt fileCount</li>
 *     <li>{Directory}
 *         fileCount entries with the following structure:</li>
 *         <ul>
 *             <li>long dataOffset</li>
 *             <li>String fileName</li>
 *         </ul>
 *     <li>{File Data}
 *         fileCount entries with the raw data of the corresponding file</li>
 * </ul>
 *
 * The fileCount integer indicates how many files are contained in this compound
 * file. The {directory} that follows has that many entries. Each directory entry
 * contains a long pointer to the start of this file's data section, and a String
 * with that file's name.
 * 
 * @lucene.internal
 */
public final class CompoundFileWriter {

    private static final class FileEntry {
        /** source file */
        String file;

        /** temporary holder for the start of directory entry for this file */
        long directoryOffset;

        /** temporary holder for the start of this file's data section */
        long dataOffset;
        
        /** the directory which contains the file. */
        Directory dir;
    }

    // Before versioning started.
    static final int FORMAT_PRE_VERSION = 0;
    
    // Segment name is not written in the file names.
    static final int FORMAT_NO_SEGMENT_PREFIX = -1;

    // NOTE: if you introduce a new format, make it 1 lower
    // than the current one, and always change this if you
    // switch to a new format!
    static final int FORMAT_CURRENT = FORMAT_NO_SEGMENT_PREFIX;

    private final Directory directory;
    private final String fileName;
    private final IOContext context;
    private final HashSet<String> ids;
    private final LinkedList<FileEntry> entries;
    private boolean merged = false;
    private final MergeState.CheckAbort checkAbort;

    /** Create the compound stream in the specified file. The file name is the
     *  entire name (no extensions are added).
     *  @throws NullPointerException if <code>dir</code> or <code>name</code> is null
     */
    public CompoundFileWriter(Directory dir, String name, IOContext context) {
      this(dir, name, context, null);
    }

    CompoundFileWriter(Directory dir, String name, IOContext context, MergeState.CheckAbort checkAbort) {
        if (dir == null)
            throw new NullPointerException("directory cannot be null");
        if (name == null)
            throw new NullPointerException("name cannot be null");
        this.checkAbort = checkAbort;
        directory = dir;
        fileName = name;
        ids = new HashSet<String>();
        entries = new LinkedList<FileEntry>();
        this.context = context;
    }

    /** Returns the directory of the compound file. */
    public Directory getDirectory() {
        return directory;
    }

    /** Returns the name of the compound file. */
    public String getName() {
        return fileName;
    }

    /** Add a source stream. <code>file</code> is the string by which the 
     *  sub-stream will be known in the compound stream.
     * 
     *  @throws IllegalStateException if this writer is closed
     *  @throws NullPointerException if <code>file</code> is null
     *  @throws IllegalArgumentException if a file with the same name
     *   has been added already
     */
    public void addFile(String file) {
      addFile(file, directory);
    }

    /**
     * Same as {@link #addFile(String)}, only for files that are found in an
     * external {@link Directory}.
     */
    public void addFile(String file, Directory dir) {
        if (merged)
            throw new IllegalStateException(
                "Can't add extensions after merge has been called");

        if (file == null)
            throw new NullPointerException(
                "file cannot be null");

        if (! ids.add(file))
            throw new IllegalArgumentException(
                "File " + file + " already added");

        FileEntry entry = new FileEntry();
        entry.file = file;
        entry.dir = dir;
        entries.add(entry);
    }

    /** Merge files with the extensions added up to now.
     *  All files with these extensions are combined sequentially into the
     *  compound stream.
     *  @throws IllegalStateException if close() had been called before or
     *   if no file has been added to this object
     */
    public void close() throws IOException {
        if (merged)
            throw new IllegalStateException("Merge already performed");

        if (entries.isEmpty())
            throw new IllegalStateException("No entries to merge have been defined");

        merged = true;

        // open the compound stream
        IndexOutput os = directory.createOutput(fileName, context);
        IOException priorException = null;
        try {
            // Write the Version info - must be a VInt because CFR reads a VInt
            // in older versions!
            os.writeVInt(FORMAT_CURRENT);
            
            // Write the number of entries
            os.writeVInt(entries.size());

            // Write the directory with all offsets at 0.
            // Remember the positions of directory entries so that we can
            // adjust the offsets later
            long totalSize = 0;
            for (FileEntry fe : entries) {
                fe.directoryOffset = os.getFilePointer();
                os.writeLong(0);    // for now
                os.writeString(IndexFileNames.stripSegmentName(fe.file));
                totalSize += fe.dir.fileLength(fe.file);
            }

            // Pre-allocate size of file as optimization --
            // this can potentially help IO performance as
            // we write the file and also later during
            // searching.  It also uncovers a disk-full
            // situation earlier and hopefully without
            // actually filling disk to 100%:
            final long finalLength = totalSize+os.getFilePointer();
            os.setLength(finalLength);

            // Open the files and copy their data into the stream.
            // Remember the locations of each file's data section.
            for (FileEntry fe : entries) {
                fe.dataOffset = os.getFilePointer();
                copyFile(fe, os);
            }

            // Write the data offsets into the directory of the compound stream
            for (FileEntry fe : entries) {
                os.seek(fe.directoryOffset);
                os.writeLong(fe.dataOffset);
            }

            assert finalLength == os.length();

            // Close the output stream. Set the os to null before trying to
            // close so that if an exception occurs during the close, the
            // finally clause below will not attempt to close the stream
            // the second time.
            IndexOutput tmp = os;
            os = null;
            tmp.close();
        } catch (IOException e) {
          priorException = e;
        } finally {
          IOUtils.closeSafely(priorException, os);
        }
    }

  /**
   * Copy the contents of the file with specified extension into the provided
   * output stream.
   */
  private void copyFile(FileEntry source, IndexOutput os) throws IOException {
    IndexInput is = source.dir.openInput(source.file, context);
    try {
      long startPtr = os.getFilePointer();
      long length = is.length();
      os.copyBytes(is, length);

      if (checkAbort != null) {
        checkAbort.work(length);
      }

      // Verify that the output length diff is equal to original file
      long endPtr = os.getFilePointer();
      long diff = endPtr - startPtr;
      if (diff != length)
        throw new IOException("Difference in the output file offsets " + diff
            + " does not match the original file length " + length);

    } finally {
      is.close();
    }
  }
}
