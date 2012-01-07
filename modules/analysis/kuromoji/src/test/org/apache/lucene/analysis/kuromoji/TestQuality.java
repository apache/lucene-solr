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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

// nocommit: we don't need this or its huge files i dont think?
// just compares segmentation to some sentences pre-tokenized by mecab
public class TestQuality extends LuceneTestCase {

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
    final Segmenter segmenter = new Segmenter();
    Analyzer testAnalyzer = new KuromojiAnalyzer(segmenter);
    
    String line1 = null;
    String line2 = null;
    while ((line1 = unseg.readLine()) != null) {
      line2 = seg.readLine();
      evaluateLine(line1, line2, testAnalyzer, stats);
    }
    
    System.out.println("#words: " + stats.numWords);
    System.out.println("#chars: " + stats.numChars);
    System.out.println("#edits: " + stats.numEdits);
    System.out.println("#sentences: " + stats.numSentences);
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
  }
  
  public static void evaluateLine(String unseg, String seg, Analyzer analyzer, Stats stats) throws Exception {
    List<String> tokens = new ArrayList<String>();
    TokenStream stream = analyzer.tokenStream("bogus", new StringReader(unseg));
    CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
    stream.reset();
    while (stream.incrementToken()) {
      tokens.add(termAtt.toString());
    }
    stream.close();
    
    List<String> expectedTokens = Arrays.asList(seg.split("\\s+"));
    tokens = normalize(tokens);
    expectedTokens = normalize(expectedTokens);
    
    HashMap<String,Character> transformation = new HashMap<String,Character>();
    CharRef charRef = new CharRef();
    
    String s1 = transform(tokens, transformation, charRef);
    String s2 = transform(expectedTokens, transformation, charRef);
    
    int edits = getDistance(s2, s1);
    //if (edits > 0) {
    //  System.out.println("unseg: " + unseg);
    //  System.out.println(tokens + " vs " + expectedTokens);
    //}
    stats.numChars += seg.length();
    stats.numEdits += edits;
    stats.numWords += expectedTokens.size();
    stats.numSentences++;
    if (edits == 0)
      stats.numSentencesCorrect++;
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
