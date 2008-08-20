package org.apache.lucene;

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

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;

import java.io.Reader;
import java.io.StringReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;

class AnalysisTest {
  static File tmpFile;
  public static void main(String[] args) {
    try {
      test("This is a test", true);
      tmpFile = File.createTempFile("words", ".txt");
      test(tmpFile, false);
    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
    tmpFile.deleteOnExit();
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
    final Token reusableToken = new Token();
    for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
      if (verbose) {
	System.out.println("Text=" + nextToken.term()
			   + " start=" + nextToken.startOffset()
			   + " end=" + nextToken.endOffset());
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
