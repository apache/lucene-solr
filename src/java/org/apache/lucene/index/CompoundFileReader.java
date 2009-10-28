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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import java.util.HashMap;
import java.io.IOException;


/**
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 */
class CompoundFileReader extends Directory {

    private int readBufferSize;

    private static final class FileEntry {
        long offset;
        long length;
    }


    // Base info
    private Directory directory;
    private String fileName;

    private IndexInput stream;
    private HashMap<String,FileEntry> entries = new HashMap<String,FileEntry>();


  public CompoundFileReader(Directory dir, String name) throws IOException {
    this(dir, name, BufferedIndexInput.BUFFER_SIZE);
  }

  public CompoundFileReader(Directory dir, String name, int readBufferSize)
    throws IOException
    {
        directory = dir;
        fileName = name;
        this.readBufferSize = readBufferSize;

        boolean success = false;

        try {
            stream = dir.openInput(name, readBufferSize);

            // read the directory and init files
            int count = stream.readVInt();
            FileEntry entry = null;
            for (int i=0; i<count; i++) {
                long offset = stream.readLong();
                String id = stream.readString();

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
            if (! success && (stream != null)) {
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
    public synchronized IndexInput openInput(String id)
    throws IOException
    {
      // Default to readBufferSize passed in when we were opened
      return openInput(id, readBufferSize);
    }

    @Override
    public synchronized IndexInput openInput(String id, int readBufferSize)
    throws IOException
    {
        if (stream == null)
            throw new IOException("Stream closed");

        FileEntry entry = entries.get(id);
        if (entry == null)
            throw new IOException("No sub-file with id " + id + " found");

        return new CSIndexInput(stream, entry.offset, entry.length, readBufferSize);
    }

    /** Returns an array of strings, one for each file in the directory. */
    @Override
    public String[] listAll() {
        String res[] = new String[entries.size()];
        return entries.keySet().toArray(res);
    }

    /** Returns true iff a file with the given name exists. */
    @Override
    public boolean fileExists(String name) {
        return entries.containsKey(name);
    }

    /** Returns the time the compound file was last modified. */
    @Override
    public long fileModified(String name) throws IOException {
        return directory.fileModified(fileName);
    }

    /** Set the modified time of the compound file to now. */
    @Override
    public void touchFile(String name) throws IOException {
        directory.touchFile(fileName);
    }

    /** Not implemented
     * @throws UnsupportedOperationException */
    @Override
    public void deleteFile(String name)
    {
        throw new UnsupportedOperationException();
    }

    /** Not implemented
     * @throws UnsupportedOperationException */
    public void renameFile(String from, String to)
    {
        throw new UnsupportedOperationException();
    }

    /** Returns the length of a file in the directory.
     * @throws IOException if the file does not exist */
    @Override
    public long fileLength(String name)
    throws IOException
    {
        FileEntry e = entries.get(name);
        if (e == null)
            throw new IOException("File " + name + " does not exist");
        return e.length;
    }

    /** Not implemented
     * @throws UnsupportedOperationException */
    @Override
    public IndexOutput createOutput(String name)
    {
        throw new UnsupportedOperationException();
    }

    /** Not implemented
     * @throws UnsupportedOperationException */
    @Override
    public Lock makeLock(String name)
    {
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

        CSIndexInput(final IndexInput base, final long fileOffset, final long length)
        {
            this(base, fileOffset, length, BufferedIndexInput.BUFFER_SIZE);
        }

        CSIndexInput(final IndexInput base, final long fileOffset, final long length, int readBufferSize)
        {
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
        protected void readInternal(byte[] b, int offset, int len)
        throws IOException
        {
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


    }
    
}
