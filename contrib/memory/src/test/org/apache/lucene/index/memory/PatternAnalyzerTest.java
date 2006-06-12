package org.apache.lucene.index.memory;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import junit.framework.TestCase;

import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

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

@author whoschek.AT.lbl.DOT.gov
*/
public class PatternAnalyzerTest extends TestCase {
  
  /** Runs the tests and/or benchmark */
  public static void main(String[] args) throws Throwable {
    new PatternAnalyzerTest().run(args);    
  }
  
  public void testMany() throws Throwable {
    String[] files = MemoryIndexTest.listFiles(new String[] {
      "*.txt", "*.html", "*.xml", "xdocs/*.xml", 
      "src/test/org/apache/lucene/queryParser/*.java",
      "src/org/apache/lucene/index/memory/*.java",
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
            if (stops != 0) stopWords = StopFilter.makeStopSet(StopAnalyzer.ENGLISH_STOP_WORDS);
                
            for (int toLower=0; toLower < maxToLower; toLower++) {
              boolean toLowerCase = toLower != 0;
                
              for (int run=0; run < runs; run++) {
                List tokens1 = null; List tokens2 = null;
                try {
                  if (usePattern) tokens1 = getTokens(patternTokenStream(text, lettersOnly, toLowerCase, stopWords));
                  if (useLucene) tokens2 = getTokens(luceneTokenStream(text, lettersOnly, toLowerCase, stopWords));          
                  if (usePattern && useLucene) assertEquals(tokens1, tokens2);
                } catch (Throwable t) {
                  if (t instanceof OutOfMemoryError) t.printStackTrace();
                  System.out.println("fatal error at file=" + file + ", letters="+ lettersOnly + ", toLowerCase=" + toLowerCase + ", stopwords=" + (stopWords != null ? "english" : "none"));
                  System.out.println("\n\ntokens1=" + toString(tokens1));
                  System.out.println("\n\ntokens2=" + toString(tokens2));
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
  
  private List getTokens(TokenStream stream) throws IOException {
    ArrayList tokens = new ArrayList();
    Token token;
    while ((token = stream.next()) != null) {
      tokens.add(token);
    }
    return tokens;
  }
  
  private void assertEquals(List tokens1, List tokens2) {
    int size = Math.min(tokens1.size(), tokens2.size());
    int i=0;
    try {
      for (; i < size; i++) {
        Token t1 = (Token) tokens1.get(i);
        Token t2 = (Token) tokens2.get(i);
        if (!(t1.termText().equals(t2.termText()))) throw new IllegalStateException("termText");
        if (t1.startOffset() != t2.startOffset()) throw new IllegalStateException("startOffset");
        if (t1.endOffset() != t2.endOffset()) throw new IllegalStateException("endOffset");
        if (!(t1.type().equals(t2.type()))) throw new IllegalStateException("type");
      }
      if (tokens1.size() != tokens2.size())   throw new IllegalStateException("size1=" + tokens1.size() + ", size2=" + tokens2.size());
    }

    catch (IllegalStateException e) {
      if (size > 0) {
        System.out.println("i=" + i + ", size=" + size);
        System.out.println("t1[size]='" + ((Token) tokens1.get(size-1)).termText() + "'");
        System.out.println("t2[size]='" + ((Token) tokens2.get(size-1)).termText() + "'");
      }
      throw e;
    }
  }
  
  private String toString(List tokens) {
    if (tokens == null) return "null";
    String str = "[";
    for (int i=0; i < tokens.size(); i++) {
      Token t1 = (Token) tokens.get(i);
      str = str + "'" + t1.termText() + "', ";
    }
    return str + "]";
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