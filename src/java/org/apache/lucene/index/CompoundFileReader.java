package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;


/**
 * Class for accessing a compound stream.
 * This class implements a directory, but is limited to only read operations.
 * Directory methods that would normally modify data throw an exception.
 *
 * @author Dmitry Serebrennikov
 * @version $Id$
 */
public class CompoundFileReader extends Directory {

    private static final class FileEntry {
        long offset;
        long length;
    }


    // Base info
    private Directory directory;
    private String fileName;

    // Reference count
    private boolean open;

    private InputStream stream;
    private HashMap entries = new HashMap();


    public CompoundFileReader(Directory dir, String name)
    throws IOException
    {
        directory = dir;
        fileName = name;

        boolean success = false;

        try {
            stream = dir.openFile(name);

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
            if (! success) {
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

    public synchronized void close() throws IOException {
        if (stream == null)
            throw new IOException("Already closed");

        entries.clear();
        stream.close();
        stream = null;
    }

    public synchronized InputStream openFile(String id)
    throws IOException
    {
        if (stream == null)
            throw new IOException("Stream closed");

        FileEntry entry = (FileEntry) entries.get(id);
        if (entry == null)
            throw new IOException("No sub-file with id " + id + " found");

        return new CSInputStream(stream, entry.offset, entry.length);
    }

    /** Returns an array of strings, one for each file in the directory. */
    public String[] list() {
        String res[] = new String[entries.size()];
        return (String[]) entries.keySet().toArray(res);
    }

    /** Returns true iff a file with the given name exists. */
    public boolean fileExists(String name) {
        return entries.containsKey(name);
    }

    /** Returns the time the named file was last modified. */
    public long fileModified(String name) throws IOException {
        return directory.fileModified(fileName);
    }

    /** Set the modified time of an existing file to now. */
    public void touchFile(String name) throws IOException {
        directory.touchFile(fileName);
    }

    /** Removes an existing file in the directory. */
    public void deleteFile(String name)
    {
        throw new UnsupportedOperationException();
    }

    /** Renames an existing file in the directory.
    If a file already exists with the new name, then it is replaced.
    This replacement should be atomic. */
    public void renameFile(String from, String to)
    {
        throw new UnsupportedOperationException();
    }

    /** Returns the length of a file in the directory. */
    public long fileLength(String name)
    throws IOException
    {
        FileEntry e = (FileEntry) entries.get(name);
        if (e == null)
            throw new IOException("File " + name + " does not exist");
        return e.length;
    }

    /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
    public OutputStream createFile(String name)
    {
        throw new UnsupportedOperationException();
    }

    /** Construct a {@link Lock}.
     * @param name the name of the lock file
     */
    public Lock makeLock(String name)
    {
        throw new UnsupportedOperationException();
    }

    /** Implementation of an InputStream that reads from a portion of the
     *  compound file. The visibility is left as "package" *only* because
     *  this helps with testing since JUnit test cases in a different class
     *  can then access package fields of this class.
     */
    static final class CSInputStream extends InputStream {

        InputStream base;
        long fileOffset;

        CSInputStream(final InputStream base,
                      final long fileOffset,
                      final long length)
        throws IOException
        {
            this.base = (InputStream) base.clone();
            this.fileOffset = fileOffset;
            this.length = length;   // variable in the superclass
            seekInternal(0);        // position to the adjusted 0th byte
        }

        /** Expert: implements buffer refill.  Reads bytes from the current
         *  position in the input.
         * @param b the array to read bytes into
         * @param offset the offset in the array to start storing bytes
         * @param length the number of bytes to read
         */
        protected void readInternal(byte[] b, int offset, int len)
        throws IOException
        {
            base.readBytes(b, offset, len);
        }

        /** Expert: implements seek.  Sets current position in this file, where
         *  the next {@link #readInternal(byte[],int,int)} will occur.
         * @see #readInternal(byte[],int,int)
         */
        protected void seekInternal(long pos) throws IOException
        {
            if (pos > 0 && pos >= length)
                throw new IOException("Seek past the end of file");

            if (pos < 0)
                throw new IOException("Seek to a negative offset");

            base.seek(fileOffset + pos);
        }

        /** Closes the stream to futher operations. */
        public void close() throws IOException
        {
            base.close();
        }

        /** Returns a clone of this stream.
         *
         * <p>Clones of a stream access the same data, and are positioned at the same
         * point as the stream they were cloned from.
         *
         * <p>Expert: Subclasses must ensure that clones may be positioned at
         * different points in the input from each other and from the stream they
         * were cloned from.
         */
        public Object clone() {
            CSInputStream other = (CSInputStream) super.clone();
            other.base = (InputStream) base.clone();
            return other;
        }
    }
}
