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
import org.apache.lucene.store.FSDirectory;

import java.util.Date;
import java.util.Random;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;

class TermInfosTest {
  public static void main(String[] args) {
    try {
      test();
    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
  }

  public static void test()
       throws Exception {
    
    File file = new File("words.txt");
    System.out.println(" reading word file containing " +
		       file.length() + " bytes");

    Date start = new Date();

    Vector keys = new Vector();
    FileInputStream ws = new FileInputStream(file);
    BufferedReader wr = new BufferedReader(new InputStreamReader(ws));

    for (String key = wr.readLine(); key!=null; key = wr.readLine())
      keys.addElement(new Term("word", key));
    wr.close();

    Date end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" milliseconds to read " + keys.size() + " words");

    start = new Date();

    Random gen = new Random(1251971);
    long fp = (gen.nextInt() & 0xF) + 1;
    long pp = (gen.nextInt() & 0xF) + 1;
    int[] docFreqs = new int[keys.size()];
    long[] freqPointers = new long[keys.size()];
    long[] proxPointers = new long[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      docFreqs[i] = (gen.nextInt() & 0xF) + 1;
      freqPointers[i] = fp;
      proxPointers[i] = pp;
      fp += (gen.nextInt() & 0xF) + 1;;
      pp += (gen.nextInt() & 0xF) + 1;;
    }

    end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" milliseconds to generate values");

    start = new Date();

    Directory store = new FSDirectory("test.store", true);
    FieldInfos fis = new FieldInfos();

    TermInfosWriter writer = new TermInfosWriter(store, "words", fis);
    fis.add("word", false);

    for (int i = 0; i < keys.size(); i++)
      writer.add((Term)keys.elementAt(i),
		 new TermInfo(docFreqs[i], freqPointers[i], proxPointers[i]));

    writer.close();

    end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" milliseconds to write table");

    System.out.println(" table occupies " +
		       store.fileLength("words.tis") + " bytes");

    start = new Date();

    TermInfosReader reader = new TermInfosReader(store, "words", fis);

    end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" milliseconds to open table");

    start = new Date();

    SegmentTermEnum enum = (SegmentTermEnum)reader.terms();
    for (int i = 0; i < keys.size(); i++) {
      enum.next();
      Term key = (Term)keys.elementAt(i);
      if (!key.equals(enum.term()))
	throw new Exception("wrong term: " + enum.term()
			    + ", expected: " + key
			    + " at " + i);
      TermInfo ti = enum.termInfo();
      if (ti.docFreq != docFreqs[i])
	throw
	  new Exception("wrong value: " + Long.toString(ti.docFreq, 16)
			+ ", expected: " + Long.toString(docFreqs[i], 16)
			+ " at " + i);
      if (ti.freqPointer != freqPointers[i])
	throw
	  new Exception("wrong value: " + Long.toString(ti.freqPointer, 16)
			+ ", expected: " + Long.toString(freqPointers[i], 16)
			+ " at " + i);
      if (ti.proxPointer != proxPointers[i])
	throw
	  new Exception("wrong value: " + Long.toString(ti.proxPointer, 16)
			+ ", expected: " + Long.toString(proxPointers[i], 16)
			+ " at " + i);
    }

    end = new Date();

    System.out.print(end.getTime() - start.getTime());
    System.out.println(" milliseconds to iterate over " +
		       keys.size() + " words");

    start = new Date();

    for (int i = 0; i < keys.size(); i++) {
      Term key = (Term)keys.elementAt(i);
      TermInfo ti = reader.get(key);
      if (ti.docFreq != docFreqs[i])
	throw
	  new Exception("wrong value: " + Long.toString(ti.docFreq, 16)
			+ ", expected: " + Long.toString(docFreqs[i], 16)
			+ " at " + i);
      if (ti.freqPointer != freqPointers[i])
	throw
	  new Exception("wrong value: " + Long.toString(ti.freqPointer, 16)
			+ ", expected: " + Long.toString(freqPointers[i], 16)
			+ " at " + i);
      if (ti.proxPointer != proxPointers[i])
	throw
	  new Exception("wrong value: " + Long.toString(ti.proxPointer, 16)
			+ ", expected: " + Long.toString(proxPointers[i], 16)
			+ " at " + i);
    }

    end = new Date();

    System.out.print((end.getTime() - start.getTime()) / (float)keys.size());
    System.out.println(" average milliseconds per lookup");

    TermEnum e = reader.terms(new Term("word", "azz"));
    System.out.println("Word after azz is " + e.term().text);

    reader.close();

    store.close();
  }
}
