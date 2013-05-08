package org.apache.lucene.analysis.kr.utils;

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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.kr.morph.MorphException;

/**
 * file utility class
 */
public class FileUtil {
  
  /**
   * Given a file name for a file that is located somewhere in the application
   * classpath, return a File object representing the file.
   *
   * @param filename The name of the file (relative to the classpath) that is
   *  to be retrieved.
   * @return A file object representing the requested filename
   * @throws Exception Thrown if the classloader can not be found or if
   *  the file can not be found in the classpath.
   */
  public static File getClassLoaderFile(String filename) throws MorphException  {
    // note that this method is used when initializing logging, so it must
    // not attempt to log anything.
    File file = null;
    ClassLoader loader = FileUtil.class.getClassLoader();
    URL url = loader.getResource(filename);
    if (url == null) {
      url = ClassLoader.getSystemResource(filename);
      if (url == null) {
        throw new MorphException("Unable to find " + filename);
      }
      file = toFile(url);
    } else {
      file = toFile(url);
    }
    if (file==null||!file.exists()) {
      return null;
    }
    return file;
  }
  
  /**
   * Reads the contents of a file line by line to a List of Strings.
   * The file is always closed.
   *
   * @param file  the file to read, must not be <code>null</code>
   * @param encoding  the encoding to use, <code>null</code> means platform default
   * @return the list of Strings representing each line in the file, never <code>null</code>
   * @throws IOException in case of an I/O error
   * @throws java.io.UnsupportedEncodingException if the encoding is not supported by the VM
   * @since Commons IO 1.1
   */
  public static List readLines(File file, String encoding) throws IOException {
    InputStream in = null;
    try {
      in = openInputStream(file);
      return readLines(in, encoding);
    } finally {
      closeQuietly(in);
    }
  }
    
  /**
   * Reads the contents of a file line by line to a List of Strings.
   * The file is always closed.
   *
   * @param fName  the file to read, must not be <code>null</code>
   * @param encoding  the encoding to use, <code>null</code> means platform default
   * @return the list of Strings representing each line in the file, never <code>null</code>
   * @throws MorphException 
   * @throws IOException 
   * @throws Exception 
   * @throws java.io.UnsupportedEncodingException if the encoding is not supported by the VM
   * @since Commons IO 1.1
   */
  public static List readLines(String fName, String encoding) throws MorphException, IOException  {
    InputStream in = null;        
    try {

      File file = getClassLoaderFile(fName);
      if(file!=null&&file.exists()) {
        in = openInputStream(file);
      } else {
        in = new ByteArrayInputStream(readByteFromCurrentJar(fName));
      }
        
      return readLines(in, encoding);
    } finally {
      closeQuietly(in);
    }
  }
    
  //-----------------------------------------------------------------------
  /**
   * Opens a {@link FileInputStream} for the specified file, providing better
   * error messages than simply calling <code>new FileInputStream(file)</code>.
   * <p>
   * At the end of the method either the stream will be successfully opened,
   * or an exception will have been thrown.
   * <p>
   * An exception is thrown if the file does not exist.
   * An exception is thrown if the file object exists but is a directory.
   * An exception is thrown if the file exists but cannot be read.
   * 
   * @param file  the file to open for input, must not be <code>null</code>
   * @return a new {@link FileInputStream} for the specified file
   * @throws FileNotFoundException if the file does not exist
   * @throws IOException if the file object is a directory
   * @throws IOException if the file cannot be read
   * @since Commons IO 1.3
   */
  public static FileInputStream openInputStream(File file) throws IOException {
    if (file.exists()) {
      if (file.isDirectory()) {
        throw new IOException("File '" + file + "' exists but is a directory");
      }
      if (file.canRead() == false) {
        throw new IOException("File '" + file + "' cannot be read");
      }
    } else {
      throw new FileNotFoundException("File '" + file + "' does not exist");
    }
    return new FileInputStream(file);
  }
    
  // readLines
  //-----------------------------------------------------------------------
  /**
   * Get the contents of an <code>InputStream</code> as a list of Strings,
   * one entry per line, using the default character encoding of the platform.
   * <p>
   * This method buffers the input internally, so there is no need to use a
   * <code>BufferedInputStream</code>.
   *
   * @param input  the <code>InputStream</code> to read from, not null
   * @return the list of Strings, never null
   * @throws NullPointerException if the input is null
   * @throws IOException if an I/O error occurs
   * @since Commons IO 1.1
   */
  public static List readLines(InputStream input) throws IOException {
    InputStreamReader reader = new InputStreamReader(input);
    return readLines(reader);
  }

  /**
   * Get the contents of an <code>InputStream</code> as a list of Strings,
   * one entry per line, using the specified character encoding.
   * <p>
   * Character encoding names can be found at
   * <a href="http://www.iana.org/assignments/character-sets">IANA</a>.
   * <p>
   * This method buffers the input internally, so there is no need to use a
   * <code>BufferedInputStream</code>.
   *
   * @param input  the <code>InputStream</code> to read from, not null
   * @param encoding  the encoding to use, null means platform default
   * @return the list of Strings, never null
   * @throws NullPointerException if the input is null
   * @throws IOException if an I/O error occurs
   * @since Commons IO 1.1
   */
  public static List readLines(InputStream input, String encoding) throws IOException {
    if (encoding == null) {
      return readLines(input);
    } else {
      InputStreamReader reader = new InputStreamReader(input, encoding);
      return readLines(reader);
    }
  }

  /**
   * Get the contents of a <code>Reader</code> as a list of Strings,
   * one entry per line.
   * <p>
   * This method buffers the input internally, so there is no need to use a
   * <code>BufferedReader</code>.
   *
   * @param input  the <code>Reader</code> to read from, not null
   * @return the list of Strings, never null
   * @throws NullPointerException if the input is null
   * @throws IOException if an I/O error occurs
   * @since Commons IO 1.1
   */
  public static List readLines(Reader input) throws IOException {
    BufferedReader reader = new BufferedReader(input);
    List list = new ArrayList();
    String line = reader.readLine();
    while (line != null) {
      if ( ! (line.startsWith("!") || line.startsWith("\uFEFF!"))) { // Skip comment lines starting with '!'
        list.add(line);
      }
      line = reader.readLine();
    }
    return list;
  }
    
  /**
   * Unconditionally close an <code>InputStream</code>.
   * <p>
   * Equivalent to {@link InputStream#close()}, except any exceptions will be ignored.
   * This is typically used in finally blocks.
   *
   * @param input  the InputStream to close, may be null or already closed
   */
  public static void closeQuietly(InputStream input) {
    try {
      if (input != null) {
        input.close();
      }
    } catch (IOException ioe) {
      // ignore
    }
  }
    
    

  //-----------------------------------------------------------------------
  /**
   * Convert from a <code>URL</code> to a <code>File</code>.
   * <p>
   * From version 1.1 this method will decode the URL.
   * Syntax such as <code>file:///my%20docs/file.txt</code> will be
   * correctly decoded to <code>/my docs/file.txt</code>.
   *
   * @param url  the file URL to convert, <code>null</code> returns <code>null</code>
   * @return the equivalent <code>File</code> object, or <code>null</code>
   *  if the URL's protocol is not <code>file</code>
   * @throws IllegalArgumentException if the file is incorrectly encoded
   */
  public static File toFile(URL url) {
    if (url == null || !url.getProtocol().equals("file")) {
      return null;
    } else {
      String filename = url.getFile().replace('/', File.separatorChar);
      int pos =0;
      while ((pos = filename.indexOf('%', pos)) >= 0) {
        if (pos + 2 < filename.length()) {
          String hexStr = filename.substring(pos + 1, pos + 3);
          char ch = (char) Integer.parseInt(hexStr, 16);
          filename = filename.substring(0, pos) + ch + filename.substring(pos + 3);
        }
      }
      return new File(filename);
    }
  }

  public static byte[] readByteFromCurrentJar(String resource) throws MorphException {

    String  jarPath = FileUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();

    JarResources jar = new JarResources(jarPath);
    try {  
      return jar.getResource(resource);
    } catch (Exception e) {
      throw new MorphException(e.getMessage(),e);
    }
  }
}
