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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
Verifies that Lucene MemoryIndex and RAMDirectory have the same behaviour,
returning the same results for any given query.
Runs a set of queries against a set of files and compares results for identity.
Can also be used as a simple benchmark.
<p>
Example usage:
<pre>
cd lucene-cvs
java org.apache.lucene.index.memory.MemoryIndexTest 1 1 memram @testqueries.txt *.txt *.html *.xml xdocs/*.xml src/test/org/apache/lucene/queryParser/*.java 
</pre>
where testqueries.txt is a file with one query per line, such as:
<pre>
#
# queries extracted from TestQueryParser.java
#
Apache
Apach~ AND Copy*

a AND b
(a AND b)
c OR (a AND b)
a AND NOT b
a AND -b
a AND !b
a && b
a && ! b

a OR b
a || b
a OR !b
a OR ! b
a OR -b

+term -term term
foo:term AND field:anotherTerm
term AND "phrase phrase"
"hello there"

germ term^2.0
(term)^2.0
(germ term)^2.0
term^2.0
term^2
"germ term"^2.0
"term germ"^2

(foo OR bar) AND (baz OR boo)
((a OR b) AND NOT c) OR d
+(apple "steve jobs") -(foo bar baz)
+title:(dog OR cat) -author:"bob dole"


a&b
a&&b
.NET

"term germ"~2
"term germ"~2 flork
"term"~2
"~2 germ"
"term germ"~2^2

3
term 1.0 1 2
term term1 term2

term*
term*^2
term~
term~0.7
term~^2
term^2~
term*germ
term*germ^3


term*
Term*
TERM*
term*
Term*
TERM*

// Then 'full' wildcard queries:
te?m
Te?m
TE?M
Te?m*gerM
te?m
Te?m
TE?M
Te?m*gerM

term term term
term +stop term
term -stop term
drop AND stop AND roll
term phrase term
term AND NOT phrase term
stop


[ a TO c]
[ a TO c ]
{ a TO c}
{ a TO c }
{ a TO c }^2.0
[ a TO c] OR bar
[ a TO c] AND bar
( bar blar { a TO c}) 
gack ( bar blar { a TO c}) 


+weltbank +worlbank
+weltbank\n+worlbank
weltbank \n+worlbank
weltbank \n +worlbank
+weltbank\r+worlbank
weltbank \r+worlbank
weltbank \r +worlbank
+weltbank\r\n+worlbank
weltbank \r\n+worlbank
weltbank \r\n +worlbank
weltbank \r \n +worlbank
+weltbank\t+worlbank
weltbank \t+worlbank
weltbank \t +worlbank


term term term
term +term term
term term +term
term +term +term
-term term term


on^1.0
"hello"^2.0
hello^2.0
"on"^1.0
the^3
</pre>

@author whoschek.AT.lbl.DOT.gov
*/
public class MemoryIndexTest extends TestCase {
  
  private Analyzer analyzer;
  private boolean fastMode = false;
  
  private static final String FIELD_NAME = "content";

  /** Runs the tests and/or benchmark */
  public static void main(String[] args) throws Throwable {
    new MemoryIndexTest().run(args);    
  }

//  public void setUp() {  }
//  public void tearDown() {}
  
  public void testMany() throws Throwable {
    String[] files = listFiles(new String[] {
      "*.txt", "*.html", "*.xml", "xdocs/*.xml", 
      "src/java/test/org/apache/lucene/queryParser/*.java",
      "src/java/org/apache/lucene/index/memory/*.java",
    });
    System.out.println("files = " + java.util.Arrays.asList(files));
    String[] xargs = new String[] {
      "1", "1", "memram", 
      "@src/test/org/apache/lucene/index/memory/testqueries.txt",
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
    
    String cmd = "memram";
    if (args.length > ++k) cmd = args[k];
    boolean useMemIndex = cmd.indexOf("mem") >= 0;
    boolean useRAMIndex = cmd.indexOf("ram") >= 0;
    
    String[] queries = { "term", "term*", "term~", "Apache", "Apach~ AND Copy*" };
    if (args.length > ++k) {
      String arg = args[k];
      if (arg.startsWith("@")) 
        queries = readLines(new File(arg.substring(1)));
      else
        queries = new String[] { arg };
    }
    
    File[] files = new File[] {new File("CHANGES.txt"), new File("LICENSE.txt") };
    if (args.length > ++k) {
      files = new File[args.length - k];
      for (int i=k; i < args.length; i++) {
        files[i-k] = new File(args[i]);
      }
    }
    
    boolean toLowerCase = true;
//    boolean toLowerCase = false;
//    Set stopWords = null;
    Set stopWords = StopFilter.makeStopSet(StopAnalyzer.ENGLISH_STOP_WORDS);
    
    Analyzer[] analyzers = new Analyzer[] { 
        new SimpleAnalyzer(),
        new StopAnalyzer(),
        new StandardAnalyzer(),
        PatternAnalyzer.DEFAULT_ANALYZER,
//        new WhitespaceAnalyzer(),
//        new PatternAnalyzer(PatternAnalyzer.NON_WORD_PATTERN, false, null),
//        new PatternAnalyzer(PatternAnalyzer.NON_WORD_PATTERN, true, stopWords),        
//        new SnowballAnalyzer("English", StopAnalyzer.ENGLISH_STOP_WORDS),
    };
    
    for (int iter=0; iter < iters; iter++) {
      System.out.println("\n########### iteration=" + iter);
      long start = System.currentTimeMillis();            
      long bytes = 0;
      
      for (int anal=0; anal < analyzers.length; anal++) {
        this.analyzer = analyzers[anal];
        
        for (int i=0; i < files.length; i++) {
          File file = files[i];
          if (!file.exists() || file.isDirectory()) continue; // ignore
          bytes += file.length();
          String text = toString(new FileInputStream(file), null);
          Document doc = createDocument(text);
          System.out.println("\n*********** FILE=" + file);
          
          for (int q=0; q < queries.length; q++) {
            try {
              Query query = parseQuery(queries[q]);
              
              for (int run=0; run < runs; run++) {
                float score1 = 0.0f; float score2 = 0.0f;
                if (useMemIndex) score1 = query(createMemoryIndex(doc), query); 
                if (useRAMIndex) score2 = query(createRAMIndex(doc), query);
                if (useMemIndex && useRAMIndex) {
                  System.out.println("diff="+ (score1-score2) + ", query=" + queries[q] + ", s1=" + score1 + ", s2=" + score2);
                  if (score1 != score2 || score1 < 0.0f || score2 < 0.0f || score1 > 1.0f || score2 > 1.0f) {
                    throw new IllegalStateException("BUG DETECTED:" + (i*(q+1)) + " at query=" + queries[q] + ", file=" + file + ", anal=" + analyzer);
                  }
                }
              }
            } catch (Throwable t) {
              if (t instanceof OutOfMemoryError) t.printStackTrace();
              System.out.println("Fatal error at query=" + queries[q] + ", file=" + file + ", anal=" + analyzer);
              throw t;
            }
          }
        }
      }
      long end = System.currentTimeMillis();
      System.out.println("\nsecs = " + ((end-start)/1000.0f));
      System.out.println("queries/sec= " + 
        (1.0f * runs * queries.length * analyzers.length * files.length 
            / ((end-start)/1000.0f)));
      float mb = (1.0f * bytes * queries.length * runs) / (1024.0f * 1024.0f);
      System.out.println("MB/sec = " + (mb / ((end-start)/1000.0f)));
    }
    
    if (useMemIndex && useRAMIndex) 
      System.out.println("No bug found. done.");
    else 
      System.out.println("Done benchmarking (without checking correctness).");
  }
  
  // returns file line by line, ignoring empty lines and comments
  private String[] readLines(File file) throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(file))); 
    ArrayList lines = new ArrayList();
    String line;  
    while ((line = reader.readLine()) != null) {
      String t = line.trim(); 
      if (t.length() > 0 && t.charAt(0) != '#' && (!t.startsWith("//"))) {
        lines.add(line);
      }
    }
    reader.close();
    
    String[] result = new String[lines.size()];
    lines.toArray(result);
    return result;
  }
  
  private Document createDocument(String content) {
    Document doc = new Document();
    doc.add(new Field(FIELD_NAME, content, Field.Store.NO, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS));
    return doc;
  }
  
  private MemoryIndex createMemoryIndex(Document doc) {
    MemoryIndex index = new MemoryIndex();
    Enumeration iter = doc.fields();
    while (iter.hasMoreElements()) {
      Field field = (Field) iter.nextElement();
      index.addField(field.name(), field.stringValue(), analyzer);
    }
    return index;
  }
  
  private RAMDirectory createRAMIndex(Document doc) {
    RAMDirectory dir = new RAMDirectory();    
    IndexWriter writer = null;
    try {
      writer = new IndexWriter(dir, analyzer, true);
      writer.setMaxFieldLength(Integer.MAX_VALUE);
      writer.addDocument(doc);
      writer.optimize();
      return dir;
    } catch (IOException e) { // should never happen (RAMDirectory)
      throw new RuntimeException(e);
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (IOException e) { // should never happen (RAMDirectory)
        throw new RuntimeException(e);
      }
    }
  }
    
  private float query(Object index, Query query) {
//    System.out.println("MB=" + (getMemorySize(index) / (1024.0f * 1024.0f)));
    Searcher searcher = null;
    try {
      if (index instanceof Directory)
        searcher = new IndexSearcher((Directory)index);
      else 
        searcher = ((MemoryIndex) index).createSearcher();

      final float[] scores = new float[1]; // inits to 0.0f
      searcher.search(query, new HitCollector() {
        public void collect(int doc, float score) {
          scores[0] = score;
        }
      });
      float score = scores[0];
//      Hits hits = searcher.search(query);
//      float score = hits.length() > 0 ? hits.score(0) : 0.0f;
      return score;
    } catch (IOException e) { // should never happen (RAMDirectory)
      throw new RuntimeException(e);
    } finally {
      try {
        if (searcher != null) searcher.close();
      } catch (IOException e) { // should never happen (RAMDirectory)
        throw new RuntimeException(e);
      }
    }
  }
  
  private int getMemorySize(Object index) {
    if (index instanceof Directory) {
      try {
        Directory dir = (Directory) index;
        int size = 0;
        String[] fileNames = dir.list();
        for (int i=0; i < fileNames.length; i++) {
          size += dir.fileLength(fileNames[i]);
        }
        return size;
      }
      catch (IOException e) { // can never happen (RAMDirectory)
        throw new RuntimeException(e);
      }
    }
    else {
      return ((MemoryIndex) index).getMemorySize();
    }
  }
  
  private Query parseQuery(String expression) throws ParseException {
    QueryParser parser = new QueryParser(FIELD_NAME, analyzer);
//    parser.setPhraseSlop(0);
    return parser.parse(expression);
  }
  
  /** returns all files matching the given file name patterns (quick n'dirty) */
  static String[] listFiles(String[] fileNames) {
    LinkedHashSet allFiles = new LinkedHashSet();
    for (int i=0; i < fileNames.length; i++) {
      int k;
      if ((k = fileNames[i].indexOf("*")) < 0) {
        allFiles.add(fileNames[i]);
      } else {
        String prefix = fileNames[i].substring(0, k);
        if (prefix.length() == 0) prefix = ".";
        final String suffix = fileNames[i].substring(k+1);
        File[] files = new File(prefix).listFiles(new FilenameFilter() {
          public boolean accept(File dir, String name) {
            return name.endsWith(suffix);
          }
        });
        if (files != null) {
          for (int j=0; j < files.length; j++) {
            allFiles.add(files[j].getPath());
          }
        }
      }      
    }
    
    String[] result = new String[allFiles.size()];
    allFiles.toArray(result);
    return result;
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