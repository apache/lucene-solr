package org.apache.lucene;

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

import com.lucene.store.Directory;
import com.lucene.store.InputStream;
import com.lucene.store.OutputStream;
import com.lucene.store.FSDirectory;
import com.lucene.store.RAMDirectory;

import java.util.Date;
import java.util.Random;

class StoreTest {
  public static void main(String[] args) {
    try {
      test(1000, true);
    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
  }

  public static void test(int count, boolean ram)
       throws Exception {
    Random gen = new Random(1251971);
    int i;
    
    Date veryStart = new Date();
    Date start = new Date();

    Directory store;
    if (ram)
      store = new RAMDirectory();
    else
      store = new FSDirectory("test.store", true);

    final int LENGTH_MASK = 0xFFF;

    for (i = 0; i < count; i++) {
      String name = i + ".dat";
      int length = gen.nextInt() & LENGTH_MASK;
      byte b = (byte)(gen.nextInt() & 0x7F);
      //System.out.println("filling " + name + " with " + length + " of " + b);

      OutputStream file = store.createFile(name);

      for (int j = 0; j < length; j++)
	file.writeByte(b);
      
      file.close();
    }

    store.close();

    Date end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" total milliseconds to create");

    gen = new Random(1251971);
    start = new Date();

    if (!ram)
      store = new FSDirectory("test.store", false);

    for (i = 0; i < count; i++) {
      String name = i + ".dat";
      int length = gen.nextInt() & LENGTH_MASK;
      byte b = (byte)(gen.nextInt() & 0x7F);
      //System.out.println("reading " + name + " with " + length + " of " + b);

      InputStream file = store.openFile(name);

      if (file.length() != length)
	throw new Exception("length incorrect");

      for (int j = 0; j < length; j++)
	if (file.readByte() != b)
	  throw new Exception("contents incorrect");

      file.close();
    }

    end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" total milliseconds to read");

    gen = new Random(1251971);
    start = new Date();

    for (i = 0; i < count; i++) {
      String name = i + ".dat";
      //System.out.println("deleting " + name);
      store.deleteFile(name);
    }

    end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" total milliseconds to delete");

    System.out.print(end.getTime() - veryStart.getTime());
    System.out.println(" total milliseconds");

    store.close();
  }
}
