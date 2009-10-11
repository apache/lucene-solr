package org.apache.lucene.index.memory;

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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.*;

/**
Verifies that Lucene PatternAnalyzer and normal Lucene Analyzers have the same behaviour,
returning the same results for any given free text.
Runs a set of texts against a tokenizers/analyzers
Can also be used as a simple benchmark.
<p>
Example usage:
<pre>
cd lucene-cvs
java org.apache.lucene.index.memory.PatternAnalyzerTest 1 1 patluc 1 2 2 *.txt *.xml docs/*.html src/java/org/apache/lucene/index/*.java xdocs/*.xml ../nux/samples/data/*.xml
</pre>

with WhitespaceAnalyzer problems can be found; These are not bugs but questionable 
Lucene features: CharTokenizer.MAX_WORD_LEN = 255.
Thus the PatternAnalyzer produces correct output, whereas the WhitespaceAnalyzer 
silently truncates text, and so the comparison results in assertEquals() don't match up. 

TODO: Convert to new TokenStream API!
*/
public class PatternAnalyzerTest extends LuceneTestCase {
  
  /** Runs the tests and/or benchmark */
  public static void main(String[] args) throws Throwable {
    new PatternAnalyzerTest().run(args);    
  }
  
  public void testMany() throws Throwable {
//    String[] files = MemoryIndexTest.listFiles(new String[] {
//      "*.txt", "*.html", "*.xml", "xdocs/*.xml", 
//      "src/test/org/apache/lucene/queryParser/*.java",
//      "src/org/apache/lucene/index/memory/*.java",
//    });
    String[] files = MemoryIndexTest.listFiles(new String[] {
      "../../*.txt", "../../*.html", "../../*.xml", "../../xdocs/*.xml", 
      "../../src/test/org/apache/lucene/queryParser/*.java",
      "src/java/org/apache/lucene/index/memory/*.java",
    });
    System.out.println("files = " + java.util.Arrays.asList(files));
    String[] xargs = new String[] {
      "1", "1", "patluc", "1", "2", "2",
    };
    String[] args = new String[xargs.length + files.length];
    System.arraycopy(xargs, 0, args, 0, xargs.length);
    System.arraycopy(files, 0, args, xargs.length, files.length);
    run(args);
  }
  
  private void run(String[] args) throws Throwable {
    int k = -1;
    
    int iters = 1;
    if (args.length > ++k) iters = Math.max(1, Integer.parseInt(args[k]));
    
    int runs = 1;
    if (args.length > ++k) runs = Math.max(1, Integer.parseInt(args[k]));
    
    String cmd = "patluc";
    if (args.length > ++k) cmd = args[k];
    boolean usePattern = cmd.indexOf("pat") >= 0;
    boolean useLucene  = cmd.indexOf("luc") >= 0;
    
    int maxLetters = 1; // = 2: CharTokenizer.MAX_WORD_LEN issue; see class javadoc
    if (args.length > ++k) maxLetters = Integer.parseInt(args[k]);
    
    int maxToLower = 2;
    if (args.length > ++k) maxToLower = Integer.parseInt(args[k]);

    int maxStops = 2;
    if (args.length > ++k) maxStops = Integer.parseInt(args[k]);
    
    File[] files = new File[] {new File("CHANGES.txt"), new File("LICENSE.txt") };
    if (args.length > ++k) {
      files = new File[args.length - k];
      for (int i=k; i < args.length; i++) {
        files[i-k] = new File(args[i]);
      }
    }
    
    for (int iter=0; iter < iters; iter++) {
      System.out.println("\n########### iteration=" + iter);
      long start = System.currentTimeMillis();            
      long bytes = 0;
      
      for (int i=0; i < files.length; i++) {
        File file = files[i];
        if (!file.exists() || file.isDirectory()) continue; // ignore
        bytes += file.length();
        String text = toString(new FileInputStream(file), null);
        System.out.println("\n*********** FILE=" + file);

        for (int letters=0; letters < maxLetters; letters++) {
          boolean lettersOnly = letters == 0;
          
          for (int stops=0; stops < maxStops; stops++) {
            Set stopWords = null;
            if (stops != 0) stopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
                
            for (int toLower=0; toLower < maxToLower; toLower++) {
              boolean toLowerCase = toLower != 0;
                
              for (int run=0; run < runs; run++) {
                TokenStream tokens1 = null; TokenStream tokens2 = null;
                try {
                  if (usePattern) tokens1 = patternTokenStream(text, lettersOnly, toLowerCase, stopWords);
                  if (useLucene) tokens2 = luceneTokenStream(text, lettersOnly, toLowerCase, stopWords);          
                  if (usePattern && useLucene) {
                    final TermAttribute termAtt1 = tokens1.addAttribute(TermAttribute.class),
                      termAtt2 = tokens2.addAttribute(TermAttribute.class);
                    final OffsetAttribute offsetAtt1 = tokens1.addAttribute(OffsetAttribute.class),
                      offsetAtt2 = tokens2.addAttribute(OffsetAttribute.class);
                    final PositionIncrementAttribute posincrAtt1 = tokens1.addAttribute(PositionIncrementAttribute.class),
                      posincrAtt2 = tokens2.addAttribute(PositionIncrementAttribute.class);
                    while (tokens1.incrementToken()) {
                      assertTrue(tokens2.incrementToken());
                      assertEquals(termAtt1, termAtt2);
                      assertEquals(offsetAtt1, offsetAtt2);
                      assertEquals(posincrAtt1, posincrAtt2);
                    }
                    assertFalse(tokens2.incrementToken());
                    tokens1.end(); tokens1.close();
                    tokens2.end(); tokens2.close();
                  }
                } catch (Throwable t) {
                  if (t instanceof OutOfMemoryError) t.printStackTrace();
                  System.out.println("fatal error at file=" + file + ", letters="+ lettersOnly + ", toLowerCase=" + toLowerCase + ", stopwords=" + (stopWords != null ? "english" : "none"));
                  throw t;
                }
              }
            }
          }
        }
        long end = System.currentTimeMillis();
        System.out.println("\nsecs = " + ((end-start)/1000.0f));
        System.out.println("files/sec= " + 
            (1.0f * runs * maxLetters * maxToLower * maxStops * files.length 
            / ((end-start)/1000.0f)));
        float mb = (1.0f * bytes * runs * maxLetters * maxToLower * maxStops) / (1024.0f * 1024.0f);
        System.out.println("MB/sec = " + (mb / ((end-start)/1000.0f)));
      }
    }
    
    if (usePattern && useLucene) 
      System.out.println("No bug found. done.");
    else 
      System.out.println("Done benchmarking (without checking correctness).");
  }

  private TokenStream patternTokenStream(String text, boolean letters, boolean toLowerCase, Set stopWords) {
    Pattern pattern;
    if (letters) 
      pattern = PatternAnalyzer.NON_WORD_PATTERN;
    else               
      pattern = PatternAnalyzer.WHITESPACE_PATTERN;
    PatternAnalyzer analyzer = new PatternAnalyzer(pattern, toLowerCase, stopWords);
    return analyzer.tokenStream("", text);
  }
  
  private TokenStream luceneTokenStream(String text, boolean letters, boolean toLowerCase, Set stopWords) {
    TokenStream stream;
    if (letters) 
      stream = new LetterTokenizer(new StringReader(text));
    else
      stream = new WhitespaceTokenizer(new StringReader(text));
    if (toLowerCase)  stream = new LowerCaseFilter(stream);
    if (stopWords != null) stream = new StopFilter(stream, stopWords);
    return stream;            
  }
  
  // trick to detect default platform charset
  private static final Charset DEFAULT_PLATFORM_CHARSET = 
    Charset.forName(new InputStreamReader(new ByteArrayInputStream(new byte[0])).getEncoding());  
  
  // the following utility methods below are copied from Apache style Nux library - see http://dsd.lbl.gov/nux
  private static String toString(InputStream input, Charset charset) throws IOException {
    if (charset == null) charset = DEFAULT_PLATFORM_CHARSET;      
    byte[] data = toByteArray(input);
    return charset.decode(ByteBuffer.wrap(data)).toString();
  }
  
  private static byte[] toByteArray(InputStream input) throws IOException {
    try {
      // safe and fast even if input.available() behaves weird or buggy
      int len = Math.max(256, input.available());
      byte[] buffer = new byte[len];
      byte[] output = new byte[len];
      
      len = 0;
      int n;
      while ((n = input.read(buffer)) >= 0) {
        if (len + n > output.length) { // grow capacity
          byte tmp[] = new byte[Math.max(output.length << 1, len + n)];
          System.arraycopy(output, 0, tmp, 0, len);
          System.arraycopy(buffer, 0, tmp, len, n);
          buffer = output; // use larger buffer for future larger bulk reads
          output = tmp;
        } else {
          System.arraycopy(buffer, 0, output, len, n);
        }
        len += n;
      }

      if (len == output.length) return output;
      buffer = null; // help gc
      buffer = new byte[len];
      System.arraycopy(output, 0, buffer, 0, len);
      return buffer;
    } finally {
      if (input != null) input.close();
    }
  }
  
}