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

import com.lucene.analysis.SimpleAnalyzer;
import com.lucene.analysis.Analyzer;
import com.lucene.analysis.TokenStream;
import com.lucene.analysis.Token;

import java.io.Reader;
import java.io.StringReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Random;

class AnalysisTest {
  public static void main(String[] args) {
    try {
      test("This is a test", true);
      test(new File("words.txt"), false);
    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
  }

  static void test(File file, boolean verbose)
       throws Exception {
    long bytes = file.length();
    System.out.println(" Reading test file containing " + bytes + " bytes.");

    FileInputStream is = new FileInputStream(file);
    BufferedReader ir = new BufferedReader(new InputStreamReader(is));
    
    test(ir, verbose, bytes);

    ir.close();
  }

  static void test(String text, boolean verbose) throws Exception {
    System.out.println(" Tokenizing string: " + text);
    test(new StringReader(text), verbose, text.length());
  }

  static void test(Reader reader, boolean verbose, long bytes)
       throws Exception {
    Analyzer analyzer = new SimpleAnalyzer();
    TokenStream stream = analyzer.tokenStream(null, reader);

    Date start = new Date();

    int count = 0;
    for (Token t = stream.next(); t!=null; t = stream.next()) {
      if (verbose) {
	System.out.println("Text=" + t.termText()
			   + " start=" + t.startOffset()
			   + " end=" + t.endOffset());
      }
      count++;
    }

    Date end = new Date();

    long time = end.getTime() - start.getTime();
    System.out.println(time + " milliseconds to extract " + count + " tokens");
    System.out.println((time*1000.0)/count + " microseconds/token");
    System.out.println((bytes * 1000.0 * 60.0 * 60.0)/(time * 1000000.0)
		       + " megabytes/hour");
  }
}
