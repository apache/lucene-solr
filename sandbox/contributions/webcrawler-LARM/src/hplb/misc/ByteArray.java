/*
 * $Id$
 * 
 * Copyright 1997 Hewlett-Packard Company
 * 
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

package hplb.misc;

import java.io.*;
import java.net.*;

/**
 * This class is a container for algorithms working on byte arrays - some
 * of the algorithms are analogous to those in java.lang.String.
 * @author      Anders Kristensen
 */
public class ByteArray {

  /** Returns copy of characters in s as a new byte array. */
  public static final byte[] getBytes(String s) {
    int len = s.length();
    byte b[] = new byte[len];
    s.getBytes(0, len, b, 0);
    return b;
  }

  /** Returns contents of file as byte array. */
  public static byte[] loadFromFile(String filename) throws IOException {
    return loadFromFile(new File(filename));
  }

  /** Returns contents of file <i>file</i> as byte array. */
  public static byte[] loadFromFile(File file) throws IOException {
    int n, nread = 0, len = (int) file.length();
    FileInputStream fin = new FileInputStream(file);
    byte[] content = new byte[len];

    while (nread < len) {
      if ((n = fin.read(content, nread, len - nread)) == -1)
        throw new IOException("Error loading Compound from file");
      nread += n;
    }

    return content;
  }

  /**
   * Reads n bytes from the specified input stream. It will return
   * fewer bytes if fewer bytes are available on the stream.
   * Hence the application should check the resulting arrays length.
   */
  public static byte[] readn(InputStream in, int n) throws IOException {
    byte[] buf = new byte[n];
    int ntotal = 0;
    int nread;

    while (ntotal < n) {
      nread = in.read(buf, ntotal, n - ntotal);
      if (nread < 0) {
        // we got less than expected - return what we got
        byte[] newbuf = new byte[ntotal];
        System.arraycopy(buf, 0, newbuf, 0, ntotal);
        return newbuf;
      }
      ntotal += nread;
    }
    return buf;
  }

  /**
   * Return contents of a WWW resource identified by a URL.
   * @param url the resource to retrieve
   * @return    the resource contents as a byte array
   */
  public static byte[] getContent(URL url) throws IOException {
    URLConnection conn = url.openConnection();
    InputStream in = conn.getInputStream();
    int length;

    /*
     * N.B. URLConnection.getContentLength() is buggy for "http" resources
     * (at least in JDK1.0.2) and won't work for "file" URLs either.
     */
    length = length = conn.getContentLength();
    if (length == -1)
      length = conn.getHeaderFieldInt("Content-Length", -1);
    if (length == -1)
      return readAll(in);
    return readn(in, length);
  }

  /**
   * Read all input from an InputStream and return as a byte array.
   * This method will not return before the end of the stream is reached.
   * @return    contents of the stream
   */
  public static byte[] readAll(InputStream in) throws IOException {
    byte[] buf = new byte[1024];
    int nread, ntotal = 0;

    while ((nread = in.read(buf, ntotal, buf.length - ntotal)) > -1) {
      ntotal += nread;
      if (ntotal == buf.length) {
        // extend buffer
        byte[] newbuf = new byte[buf.length * 2];
        System.arraycopy(buf, 0, newbuf, 0, buf.length);
        buf = newbuf;
      }
    }
    if (ntotal < buf.length) {
      // we cannot have excess space
      byte[] newbuf = new byte[ntotal];
      System.arraycopy(buf, 0, newbuf, 0, ntotal);
      buf = newbuf;
    }
    return buf;
  }

  /**
   * Copies data from the specified input stream to the output stream
   * until end of file is met.
   * @return    the total number of bytes written to the output stream
   */
  public static int cpybytes(InputStream in, OutputStream out)
    throws IOException
  {
    byte[] buf = new byte[1024];
    int n, ntotal = 0;
    while ((n = in.read(buf)) > -1) {
      out.write(buf, 0, n);
      ntotal += n;
    }
    return ntotal;
  }

  /**
   * Copies data from the specified input stream to the output stream
   * until <em>n</em> bytes has been copied or end of file is met.
   * @return    the total number of bytes written to the output stream
   */
  public static int cpybytes(InputStream in, OutputStream out, int n)
    throws IOException
  {
    int sz = n < 1024 ? n : 1024;
    byte[] buf = new byte[sz];
    int chunk, nread, ntotal = 0;

    chunk = sz;

    while (ntotal < n && (nread = in.read(buf, 0, chunk)) > -1) {
      out.write(buf, 0, nread);
      ntotal += nread;
      chunk = (n - ntotal < sz) ? n - ntotal : sz;
    }
    return ntotal;
  }

  /**
   * Returns the index within this String of the first occurrence of the
   * specified character or -1 if the character is not found.
   * @params buf        the buffer to search
   * @params ch         the character to search for
   */
  public static final int indexOf(byte[] buf,
                                  int ch) {
    return indexOf(buf, ch, 0, buf.length);
  }

  /**
   * Returns the index within this String of the first occurrence of the
   * specified character, starting the search at fromIndex. This method
   * returns -1 if the character is not found.
   * @params buf        the buffer to search
   * @params ch         the character to search for
   * @params fromIndex  the index to start the search from 
   * @params toIndex    the highest possible index returned plus 1
   */
  public static final int indexOf(byte[] buf,
                                  int ch,
                                  int fromIndex,
                                  int toIndex) {
    int i;

    for (i = fromIndex; i < toIndex && buf[i] != ch; i++)
      ;  // do nothing

    if (i < toIndex)
      return i;
    else
      return -1;
  }

  /**
   * Returns the index of the first occurrence of s in the specified
   * buffer or -1 if this is not found.
   */
  public static final int indexOf(byte[] buf, String s) {
    return indexOf(buf, s, 0);
  }

  /**
   * Returns the index of the first occurrence of s in the specified
   * buffer. The search starts from fromIndex. This method returns -1
   * if the index is not found.
   */
  public static final int indexOf(byte[] buf, String s, int fromIndex) {
    int i;                  // index into buf
    int j;                  // index into s
    int max_i = buf.length;
    int max_j = s.length();

    for (i = fromIndex; i + max_j <= max_i; i++) {
      for (j = 0; j < max_j; j++) {
        if (buf[j + i] != s.charAt(j))
          break;
      }
      if (j == max_j) return i;
    }
    return -1;
  }

/*
  // for testing indexOf(byte[], String, int)
  public static void main(String[] args) {
    byte[] buf = getBytes(args[0]);
    System.out.println("IndexOf(arg0, arg1, 0) = " + indexOf(buf, args[1], 3));
  }
*/

  public static final boolean isSpace(int ch) {
    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') return true;
    else return false;
  }

  public static final int skipSpaces(byte[] buf, int fromIndex, int toIndex) {
    int i;
    for (i = fromIndex; i < toIndex && isSpace(buf[i]); i++)
      ;
    return i;
  }
  /**
   * Find byte pattern ptrn in buffer buf.
   * @return    index of first occurrence of ptrn in buf, -1 if no occurence
   */
  public static final int findBytes(byte buf[],
                                    int off,
                                    int len,
                                    byte ptrn[]) {
    // Note: This code is completely incomprehensible without a drawing...

    int buf_len = off + len;
    int ptrn_len = ptrn.length;
    int i;                       // index into buf
    int j;                       // index into ptrn;
    byte b = ptrn[0];            // next byte of interest

    for (i = off; i < buf_len; ) {
      j = 0;
      while (i < buf_len && j < ptrn_len && buf[i] == ptrn[j]) {
        i++;
        j++;
      }
      if (i == buf_len || j == ptrn_len)
        return i - j;
      else {
        // We have to go back a bit as there may be an overlapping
        // match starting a bit later in buf...
        i = i - j + 1;
      }
    }
    return -1;
  }

/*
  // for testing findBytes(byte[], int, int, byte[]) 
  public static void main(String args[]) {
    if (args.length < 4) {
      System.err.println("Usage: s1 off len s2");
      System.exit(1);
    }
    byte b1[] = new byte[args[0].length()];
    byte b2[] = new byte[args[3].length()];
    args[0].getBytes(0, args[0].length(), b1, 0);
    args[3].getBytes(0, args[3].length(), b2, 0);
    int off = Integer.parseInt(args[1]);
    int len = Integer.parseInt(args[2]);
    System.out.println("Index = " + findBytes(b1, off, len, b2));
  }
*/
}
