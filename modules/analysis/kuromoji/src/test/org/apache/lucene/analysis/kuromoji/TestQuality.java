package org.apache.lucene.analysis.kuromoji;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipFile;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.kuromoji.KuromojiTokenizer.Mode;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

// nocommit: we don't need this or its huge files i dont think?
// just compares segmentation to some sentences pre-tokenized by mecab
public class TestQuality extends LuceneTestCase {

  public void testSingleText() throws Exception {
    File datafile = getDataFile("tanakaseg.zip");
    ZipFile zip = new ZipFile(datafile);
    InputStream is = zip.getInputStream(zip.getEntry("sentences.txt"));
    BufferedReader unseg = new BufferedReader(new InputStreamReader(is, IOUtils.CHARSET_UTF_8));
    InputStream is2 = zip.getInputStream(zip.getEntry("segmented.txt"));
    BufferedReader seg = new BufferedReader(new InputStreamReader(is2, IOUtils.CHARSET_UTF_8));

    final boolean ONE_TIME = true;

    /**
     #words: 1578506
     #chars: 4519246
     #edits: 651
     #sentences: 150122
     sentence agreement?: 0.998161495317142
     word agreement?: 0.999587584716181
     */

    StringBuilder sb = new StringBuilder();

    String line1 = null;
    int maxLen = 0;
    int count = 0;
    while ((line1 = unseg.readLine()) != null) {
      seg.readLine();
      // nocommit also try removing the "easy" period at the
      // end of each sentence...
      maxLen = Math.max(line1.length(), maxLen);
      sb.append(line1);
      /*
      if (ONE_TIME && count++ == 100) {
        // nocommit;
        break;
      }
      */
    }
    //System.out.println("maxLen=" + maxLen);

    final Tokenizer tokenizer = new KuromojiTokenizer(new StringReader(""), null, true, Mode.SEARCH_WITH_COMPOUNDS);
    tokenizer.reset();
    final String all = sb.toString();
    System.out.println("all.len=" + all.length());
    final int ITERS = 20;
    CharTermAttribute termAtt = tokenizer.addAttribute(CharTermAttribute.class); 
    for(int iter=0;iter<ITERS;iter++) {
      tokenizer.reset(new StringReader(all));
      tokenizer.reset();
      count = 0;
      long t0 = System.currentTimeMillis();
      while(tokenizer.incrementToken()) {
        if (false && ONE_TIME) {
          System.out.println(count + ": " + termAtt.toString());
        }
        count++;
      }
      long t1 = System.currentTimeMillis();
      System.out.println(all.length()/(t1-t0) + " bytes/msec; count=" + count);

      if (ONE_TIME) {
        break;
      }
    }
  }

  public void test() throws Exception {
    File datafile = getDataFile("tanakaseg.zip");
    ZipFile zip = new ZipFile(datafile);
    InputStream is = zip.getInputStream(zip.getEntry("sentences.txt"));
    BufferedReader unseg = new BufferedReader(new InputStreamReader(is, IOUtils.CHARSET_UTF_8));
    InputStream is2 = zip.getInputStream(zip.getEntry("segmented.txt"));
    BufferedReader seg = new BufferedReader(new InputStreamReader(is2, IOUtils.CHARSET_UTF_8));
    Stats stats = new Stats();
    /**
     #words: 1578506
     #chars: 4519246
     #edits: 651
     #sentences: 150122
     sentence agreement?: 0.998161495317142
     word agreement?: 0.999587584716181
     */
    
    final Tokenizer tokenizer = new KuromojiTokenizer(new StringReader(""), null, true, Mode.SEARCH_WITH_COMPOUNDS);
    String line1 = null;
    String line2 = null;
    int count = 0;
    while ((line1 = unseg.readLine()) != null) {
      line2 = seg.readLine();
      evaluateLine(count++, line1, line2, tokenizer, stats);
    }
    
    System.out.println("#words: " + stats.numWords);
    System.out.println("#chars: " + stats.numChars);
    System.out.println("#edits: " + stats.numEdits);
    System.out.println("#sentences: " + stats.numSentences);
    System.out.println("#tokens: " + stats.numTokens);
    System.out.println("sentence agreement?: " + (stats.numSentencesCorrect/(double)stats.numSentences));
    System.out.println("word agreement?: " + (1D - (stats.numEdits / (double)stats.numWords)));
    unseg.close();
    seg.close();
    zip.close();
  }
  
  static class Stats {
    long numWords = 0;
    long numEdits = 0;
    long numChars = 0;
    long numSentences = 0;
    long numSentencesCorrect = 0;
    long numTokens = 0;
  }

  static class Path {
    public final List<String> tokens;
    public int pos;

    public Path() {
      tokens = new ArrayList<String>();
    }

    public void add(String token, int posLen) {
      tokens.add(token);
      pos += posLen;
    }
  }
  
  public static void evaluateLine(int lineCount, String unseg, String seg, Tokenizer tokenizer, Stats stats) throws Exception {
    if (VERBOSE) {
      System.out.println("\nTEST " + lineCount + ": input " + unseg);
    }
    tokenizer.reset(new StringReader(unseg));
    tokenizer.reset();
    CharTermAttribute termAtt = tokenizer.addAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncAtt = tokenizer.addAttribute(PositionIncrementAttribute.class);
    PositionLengthAttribute posLengthAtt = tokenizer.addAttribute(PositionLengthAttribute.class);
    List<Path> paths = new ArrayList<Path>();
    paths.add(new Path());
    
    int pos = -1;
    int numTokens = 0;
    while(tokenizer.incrementToken()) {
      final int posInc = posIncAtt.getPositionIncrement();
      final int posLength = posLengthAtt.getPositionLength();
      final String token = termAtt.toString();

      //System.out.println("  tok=" + token + " numPaths=" + paths.size() + " posLen=" + posLength);

      pos += posInc;
      numTokens++;

      if (VERBOSE) {
        if (posIncAtt.getPositionIncrement() == 0) {
          System.out.println("  fork @ token=" + token + " posLength=" + posLength);
        }
      }

      assert pos >= 0;
      final int numPaths = paths.size();
      for(int pathID=0;pathID<numPaths;pathID++) {
        final Path path = paths.get(pathID);
        if (pos == path.pos) {
          path.add(token, posLength);
        } else if (pos == path.pos-1 && posInc == 0) {

          // NOTE: this is horribly, horribly inefficient in
          // general!!  Much better to do graph-to-graph
          // alignment to get min edits:

          // nocommit this isn't fully general, ie, it
          // assumes the tokenizer ALWAYS outputs the
          // posLen=1 token first, at a given position
          // Fork!
          assert path.tokens.size() > 0;
          Path newPath = new Path();
          newPath.tokens.addAll(path.tokens);
          newPath.tokens.remove(newPath.tokens.size()-1);
          newPath.pos = pos;
          newPath.add(token, posLength);
          paths.add(newPath);

        } else {
          assert pos < path.pos: "pos=" + pos + " path.pos=" + path.pos + " pathID=" + pathID;
        }
      }
    }

    if (VERBOSE) {
      System.out.println("  " + paths.size() + " paths; " + numTokens + " tokens");
    }

    List<String> expectedTokens = Arrays.asList(seg.split("\\s+"));
    expectedTokens = normalize(expectedTokens);

    int minEdits = Integer.MAX_VALUE;
    List<String> minPath = null;
    for(Path path : paths) {
      List<String> tokens = normalize(path.tokens);
      if (VERBOSE) {
        System.out.println("    path: " + path.tokens);
      }
    
      HashMap<String,Character> transformation = new HashMap<String,Character>();
      CharRef charRef = new CharRef();
      String s1 = transform(tokens, transformation, charRef);
      String s2 = transform(expectedTokens, transformation, charRef);
    
      int edits = getDistance(s2, s1);
      if (edits < minEdits) {
        minEdits = edits;
        minPath = tokens;
        if (VERBOSE) {
          System.out.println("      ** edits=" + edits);
        }
      }
    }
    assert minPath != null;
    /*
    if (minEdits > 0) {
      if (!VERBOSE) {
        System.out.println("\nTEST " + lineCount + ": input " + unseg + "; " + minEdits + " edits");
      }
      System.out.println("    expected: " + expectedTokens);
      System.out.println("      actual: " + minPath);
    }
    */
    stats.numChars += seg.length();
    stats.numEdits += minEdits;
    stats.numWords += expectedTokens.size();
    stats.numTokens += numTokens;
    stats.numSentences++;
    if (minEdits == 0) {
      stats.numSentencesCorrect++;
    }
  }
  
  static class CharRef {
    char c = 'a';
  }
  
  static String transform(List<String> tokens, HashMap<String,Character> transformation, CharRef ref) {
    StringBuilder builder = new StringBuilder();
    for (String token : tokens) {
      Character value = transformation.get(token);
      
      if (value == null) {
        value = new Character(ref.c);
        ref.c++;
        transformation.put(token, value);
      }
      
      builder.append(value.charValue());
    }
    return builder.toString();
  }
  
  static List<String> normalize(List<String> tokens) {
    List<String> newList = new ArrayList<String>();
    Iterator<String> iterator = tokens.iterator();
    while (iterator.hasNext()) {
      String term = iterator.next();
      if (Character.isLetterOrDigit(term.charAt(0)))
        newList.add(term);
    }
    return newList;
  }
  
  
  //*****************************
  // Compute Levenshtein distance: see org.apache.commons.lang.StringUtils#getLevenshteinDistance(String, String)
  //*****************************
  private static int getDistance (String target, String other) {
    char[] sa;
    int n;
    int p[]; //'previous' cost array, horizontally
    int d[]; // cost array, horizontally
    int _d[]; //placeholder to assist in swapping p and d
    
      /*
         The difference between this impl. and the previous is that, rather
         than creating and retaining a matrix of size s.length()+1 by t.length()+1,
         we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
         is the 'current working' distance array that maintains the newest distance cost
         counts as we iterate through the characters of String s.  Each time we increment
         the index of String t we are comparing, d is copied to p, the second int[].  Doing so
         allows us to retain the previous cost counts as required by the algorithm (taking
         the minimum of the cost count to the left, up one, and diagonally up and to the left
         of the current cost count being calculated).  (Note that the arrays aren't really
         copied anymore, just switched...this is clearly much better than cloning an array
         or doing a System.arraycopy() each time  through the outer loop.)

         Effectively, the difference between the two implementations is this one does not
         cause an out of memory condition when calculating the LD over two very large strings.
       */

      sa = target.toCharArray();
      n = sa.length;
      p = new int[n+1]; 
      d = new int[n+1]; 
    
      final int m = other.length();
      if (n == 0 || m == 0) {
        if (n == m) {
          return 0;
        }
        else {
          return Math.max(n, m);
        }
      } 


      // indexes into strings s and t
      int i; // iterates through s
      int j; // iterates through t

      char t_j; // jth character of t

      int cost; // cost

      for (i = 0; i<=n; i++) {
          p[i] = i;
      }

      for (j = 1; j<=m; j++) {
          t_j = other.charAt(j-1);
          d[0] = j;

          for (i=1; i<=n; i++) {
              cost = sa[i-1]==t_j ? 0 : 1;
              // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
              d[i] = Math.min(Math.min(d[i-1]+1, p[i]+1),  p[i-1]+cost);
          }

          // copy current distance counts to 'previous row' distance counts
          _d = p;
          p = d;
          d = _d;
      }

      // our last action in the above loop was to switch d and p, so p now
      // actually has the most recent cost counts
      return Math.abs(p[n]);
  }
}
