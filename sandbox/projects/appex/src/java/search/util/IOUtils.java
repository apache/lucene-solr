package search.util;

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
 *    "Apache Turbine" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Turbine", nor may "Apache" appear in their name, without
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

//import com.ice.tar.TarArchive; -dunno where this lives -ACO
import org.apache.log4j.Category;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Utility IO-related methods.
 *
 * @author <a href="mailto:kelvin@relevanz.com">Kelvin Tan</a>
 */
public final class IOUtils
{
    /**
     * Log4j category.
     */
    static Category cat = Category.getInstance(IOUtils.class.getName());

    /**
     * Writes data from the inputstream to the outputstream.
     *
     * @param in InputStream to read from.
     * @param out OutputStream to write to.
     * @throws IOException I/O error.
     */
    public static void transferData(InputStream in, OutputStream out)
            throws IOException
    {
        byte[] data = new byte[10000];
        int len;
        while ((len = in.read(data)) != -1)
        {
            out.write(data, 0, len);
        }
    }

    /** To copy the file. (from Java Examples in a Nutshell)
     * @param from_name source file with full path.
     * @param to_name target file with full path.
     */
    public static void copyFile(String from_name, String to_name)
            throws IOException
    {
        File from_file = new File(from_name);
        File to_file = new File(to_name);
        copyFile(from_file, to_file);
    }

    /** To copy the file. (from Java Examples in a Nutshell)
     * @param from_file source file with full path.
     * @param to_file target file with full path.
     */
    public static void copyFile(File from_file, File to_file)
            throws IOException
    {
        if (!from_file.exists())
            throw new IOException("FileCopy: no such source file: " + from_file.getName());
        if (!from_file.isFile())
            throw new IOException("FileCopy: can't copy directory: " + from_file.getName());
        if (!from_file.canRead())
            throw new IOException("FileCopy: source file is unreadable: " + from_file.getName());

        // If we've gotten this far, then everything is okay.
        // So we copy the file, a buffer of bytes at a time.
        FileInputStream from = null;  // Stream to read from source
        FileOutputStream to = null;   // Stream to write to destination
        try
        {
            from = new FileInputStream(from_file);  // Create input stream
            to = new FileOutputStream(to_file);     // Create output stream
            byte[] buffer = new byte[8192];         // A buffer to hold file contents
            int bytes_read;                         // How many bytes in buffer
            // Read a chunk of bytes into the buffer, then write them out,
            // looping until we reach the end of the file (when read() returns -1).
            // Note the combination of assignment and comparison in this while
            // loop.  This is a common I/O programming idiom.
            while ((bytes_read = from.read(buffer)) != -1) // Read bytes until EOF
                to.write(buffer, 0, bytes_read);            //   write bytes
        }

                // Always close the streams, even if exceptions were thrown
        finally
        {
            if (from != null)
                try
                {
                    from.close();
                }
                catch (IOException e)
                {
                    ;
                }
            if (to != null)
                try
                {
                    to.close();
                }
                catch (IOException e)
                {
                    ;
                }
        }
    }

    /**
     * Moves files from one directory to another. The source directory is not moved.
     * <br><br>
     * Implementation note: Only files are moved. Directories within the source
     * are not moved as well.
     * @param File Source directory.
     * @param File Destination directory.
     */
    public static void moveFiles(File source, File destination)
            throws IOException
    {
        InputStream in = null;
        OutputStream out = null;
        if (!source.isDirectory())
            throw new IOException("Expected a directory but "
                                  + source.toString() + " is a file!");
        if (!destination.isDirectory())
            throw new IOException("Expected a directory but "
                                  + destination.toString() + " is a file!");
        File[] farray = source.listFiles();
        try
        {
            for (int i = 0; i < farray.length; i++)
            {
                if (farray[i].isFile())
                {
                    File target = new File(destination, farray[i].getName());
                    if (!target.exists() || target.canWrite())
                    {
                        in = new FileInputStream(farray[i]);
                        out = new FileOutputStream(target);
                        transferData(in, out);
                        in.close();
                        out.close();
                        if (target.exists())
                            farray[i].delete();
                    }
                }
            }
            in = null;
            out = null;
        }
        finally
        {
            if (in != null)
                in.close();
            if (out != null)
                out.close();
        }
    }

    /**
     * Recursively deletes a directory.
     * @param File Directory to delete.
     */
    public static void deleteDirectory(File directory)
    {
        File[] fArray = directory.listFiles();
        for (int i = 0; i < fArray.length; i++)
        {
            if (fArray[i].isDirectory())
            {
                deleteDirectory(fArray[i]);
            }
            fArray[i].delete();
        }
        directory.delete();
    }

    /**
     * Writes an input stream to a temporary file which is set
     * to delete when the VM exits.
     * @param Inputstream to read data from
     * @param Temporary file to write to
     */
    public static void writeToTempFile(InputStream in, String tempfile)
            throws IOException
    {
        OutputStream out = null;
        try
        {
            File f = new File(tempfile);
            f.deleteOnExit();
            char lastChar = tempfile.charAt(tempfile.length() - 1);
            // make no assumptions that java.io.File detects directories
            // in a cross-platform manner
            if (f.isDirectory() || lastChar == '\\' || lastChar == '/')
                f.mkdirs();
            else
            {
                // ensure that all necessary directories are created
                File parent = f.getParentFile();
                parent.deleteOnExit();
                parent.mkdirs();
                out = new FileOutputStream(tempfile);
                transferData(in, out);
            }
        }
        finally
        {
            if (out != null)
                out.close();
        }
    }

    /**
     * Writes an file to a ZipOutputStream.
     * @param File to read data from
     * @param Path of the ZipEntry
     * @param ZipOutputStream to write to
     */
    public static void addToZipOutputStream(String file,
                                            String zipPath,
                                            ZipOutputStream out)
            throws FileNotFoundException, IOException
    {
        File f = new File(file);
        byte[] buffer = new byte[8192];  // Create a buffer for copying
        int bytes_read;
        FileInputStream in = null;
        try
        {
            in = new FileInputStream(f); // Stream to read file
            ZipEntry entry = new ZipEntry(zipPath);      // Make a ZipEntry
            out.putNextEntry(entry);                     // Store entry in zipfile
            while ((bytes_read = in.read(buffer)) != -1) // Copy bytes to zipfile
                out.write(buffer, 0, bytes_read);
        }
        finally
        {
            if (in != null)
                in.close(); // Close input stream
        }
    }

    /**
     * Extracts a tar file to a directory.
     * @param Tar file to read data from
     * @param Directory to write to
     */
    public static void extractTar(File tarFile, File destDir)
            throws IOException
    {
/*
        FileInputStream fis = null;
        try
        {
            fis = new FileInputStream(tarFile);
            TarArchive ta = new TarArchive(fis);
            ta.extractContents(destDir);
            ta.closeArchive();
        }
        finally
        {
            if (fis != null)
                fis.close();
        }
*/
	throw new RuntimeException("This method has been officially broken "+
                                   "by andy who couldn't find TarArchive");
    }

    /**
     * Extracts a GZip file to a file.
     * @param GZip file to read data from
     * @param File to write to
     */
    public static void extractGZip(File f, File destFile) throws IOException
    {
        FileOutputStream out = null;
        FileInputStream fis = null;
        GZIPInputStream gzin = null;
        try
        {
            out = new FileOutputStream(destFile);
            fis = new FileInputStream(f);
            gzin = new GZIPInputStream(fis);
            byte[] data = new byte[10000];
            int len;
            while ((len = gzin.read(data)) != -1)
            {
                out.write(data, 0, len);
            }
            out.flush();
        }
        finally
        {
            if (gzin != null)
                gzin.close();
            if (out != null)
                out.close();
            if (fis != null)
                fis.close();
        }
    }

    /**
     * reads all bytes from the given stream
     * @param is the stream to read from
     */
    public static final byte[] loadBytes(InputStream is) throws IOException
    {
        // read in the entry data
        int count = 0;
        byte[] buffer = new byte[0];
        byte[] chunk = new byte[4096];
        while ((count = is.read(chunk)) >= 0)
        {
            byte[] t = new byte[buffer.length + count];
            System.arraycopy(buffer, 0, t, 0, buffer.length);
            System.arraycopy(chunk, 0, t, buffer.length, count);
            buffer = t;
        }
        return buffer;
    }

    /** Returns the file extension of a file.
     * @param filename Filename to obtain the file extension.
     * @return File extension (without the ".").
     */
    public static String getFileExtension(String filename)
    {
        return filename.substring(filename.lastIndexOf(".") + 1); // + 1 to remove the "."
    }

    /** Returns the file extension of a file.
     * @param f File object to obtain the file extension.
     * @return File extension (without the ".").
     */
    public static String getFileExtension(File f)
    {
        return getFileExtension(f.getName());
    }
}
