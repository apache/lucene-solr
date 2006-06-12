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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Loads the <a target="_blank" 
 * href="http://www.cogsci.princeton.edu/~wn/">WordNet </a> prolog file <a
 * href="http://www.cogsci.princeton.edu/2.0/WNprolog-2.0.tar.gz">wn_s.pl </a>
 * into a thread-safe main-memory hash map that can be used for fast
 * high-frequncy lookups of synonyms for any given (lowercase) word string.
 * <p>
 * There holds: If B is a synonym for A (A -> B) then A is also a synonym for B (B -> A).
 * There does not necessary hold: A -> B, B -> C then A -> C.
 * <p>
 * Loading typically takes some 1.5 secs, so should be done only once per
 * (server) program execution, using a singleton pattern. Once loaded, a
 * synonym lookup via {@link #getSynonyms(String)}takes constant time O(1).
 * A loaded default synonym map consumes about 10 MB main memory.
 * An instance is immutable, hence thread-safe.
 * <p>
 * This implementation borrows some ideas from the Lucene Syns2Index demo that 
 * Dave Spencer originally contributed to Lucene. Dave's approach
 * involved a persistent Lucene index which is suitable for occasional
 * lookups or very large synonym tables, but considered unsuitable for 
 * high-frequency lookups of medium size synonym tables.
 * <p>
 * Example Usage:
 * <pre>
 * String[] words = new String[] { "hard", "woods", "forest", "wolfish", "xxxx"};
 * SynonymMap map = new SynonymMap(new FileInputStream("samples/fulltext/wn_s.pl"));
 * for (int i = 0; i &lt; words.length; i++) {
 *     String[] synonyms = map.getSynonyms(words[i]);
 *     System.out.println(words[i] + ":" + java.util.Arrays.asList(synonyms).toString());
 * }
 * 
 * Example output:
 * hard:[arduous, backbreaking, difficult, fermented, firmly, grueling, gruelling, heavily, heavy, intemperately, knockout, laborious, punishing, severe, severely, strong, toilsome, tough]
 * woods:[forest, wood]
 * forest:[afforest, timber, timberland, wood, woodland, woods]
 * wolfish:[edacious, esurient, rapacious, ravening, ravenous, voracious, wolflike]
 * xxxx:[]
 * </pre>
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @see <a target="_blank"
 *      href="http://www.cogsci.princeton.edu/~wn/man/prologdb.5WN.html">prologdb
 *      man page </a>
 * @see <a target="_blank" href="http://www.hostmon.com/rfc/advanced.jsp">Dave's synonym demo site</a>
 */
public class SynonymMap {

  /** the index data; Map<String word, String[] synonyms> */
  private final HashMap table;
  
  private static final String[] EMPTY = new String[0];
  
  private static final boolean DEBUG = false;

  /**
   * Constructs an instance, loading WordNet synonym data from the given input
   * stream. Finally closes the stream. The words in the stream must be in
   * UTF-8 or a compatible subset (for example ASCII, MacRoman, etc.).
   * 
   * @param input
   *            the stream to read from (null indicates an empty synonym map)
   * @throws IOException
   *             if an error occured while reading the stream.
   */
  public SynonymMap(InputStream input) throws IOException {
    this.table = input == null ? new HashMap(0) : read(toByteArray(input));
  }
  
  /**
   * Returns the synonym set for the given word, sorted ascending.
   * 
   * @param word
   *            the word to lookup (must be in lowercase).
   * @return the synonyms; a set of zero or more words, sorted ascending, each
   *         word containing lowercase characters that satisfy
   *         <code>Character.isLetter()</code>.
   */
  public String[] getSynonyms(String word) {
    Object syns = table.get(word);
    if (syns == null) return EMPTY;
    if (syns instanceof String) return new String[] {(String) syns};
    
    String[] synonyms = (String[]) syns;
    String[] copy = new String[synonyms.length]; // copy for guaranteed immutability
    System.arraycopy(synonyms, 0, copy, 0, synonyms.length);
    return copy;
  }
  
  /**
   * Returns a String representation of the index data for debugging purposes.
   * 
   * @return a String representation
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    Iterator iter = new TreeMap(table).keySet().iterator();
    int count = 0;
    int f0 = 0;
    int f1 = 0;
    int f2 = 0;
    int f3 = 0;
    
    while (iter.hasNext()) {
      String word = (String) iter.next();
      buf.append(word + ":");
      String[] synonyms = getSynonyms(word);
      buf.append(Arrays.asList(synonyms));
      buf.append("\n");
      count += synonyms.length;
      if (synonyms.length == 0) f0++;
      if (synonyms.length == 1) f1++;
      if (synonyms.length == 2) f2++;
      if (synonyms.length == 3) f3++;
    }
    
    buf.append("\n\nkeys=" + table.size() + ", synonyms=" + count + ", f0=" + f0 +", f1=" + f1 + ", f2=" + f2 + ", f3=" + f3);
    return buf.toString();
  }
  
  /**
   * Analyzes/transforms the given word on input stream loading. This default implementation simply
   * lowercases the word. Override this method with a custom stemming
   * algorithm or similar, if desired.
   * 
   * @param word
   *            the word to analyze
   * @return the same word, or a different word (or null to indicate that the
   *         word should be ignored)
   */
  protected String analyze(String word) {
    return word.toLowerCase();
  }

  private static boolean isValid(String str) {
    for (int i=str.length(); --i >= 0; ) {
      if (!Character.isLetter(str.charAt(i))) return false;
    }
    return true;
  }

  private HashMap read(byte[] data) {
    int WORDS  = (int) (76401 / 0.7); // presizing
    int GROUPS = (int) (88022 / 0.7); // presizing
    HashMap word2Groups = new HashMap(WORDS);  // Map<String word, int[] groups>
    HashMap group2Words = new HashMap(GROUPS); // Map<int group, String[] words>
    HashMap internedWords = new HashMap(WORDS);// Map<String word, String word>

    Charset charset = Charset.forName("UTF-8");
    int lastNum = -1;
    Integer lastGroup = null;
    int len = data.length;
    int i=0;
    
    while (i < len) { // until EOF
      /* Part A: Parse a line */
      
      // scan to beginning of group
      while (i < len && data[i] != '(') i++;
      if (i >= len) break; // EOF
      i++;
      
      // parse group
      int num = 0;
      while (i < len && data[i] != ',') {
        num = 10*num + (data[i] - 48);
        i++;
      }
      i++;
//      if (DEBUG) System.err.println("num="+ num);
      
      // scan to beginning of word
      while (i < len && data[i] != '\'') i++;
      i++;
  
      // scan to end of word
      int start = i;
      do {
        while (i < len && data[i] != '\'') i++;
        i++;
      } while (i < len && data[i] != ','); // word must end with "',"
      
      if (i >= len) break; // EOF
      String word = charset.decode(ByteBuffer.wrap(data, start, i-start-1)).toString();
//      String word = new String(data, 0, start, i-start-1); // ASCII
      
      /*
       * Part B: ignore phrases (with spaces and hyphens) and
       * non-alphabetic words, and let user customize word (e.g. do some
       * stemming)
       */
      if (!isValid(word)) continue; // ignore
      word = analyze(word);
      if (word == null || word.length() == 0) continue; // ignore
      
      
      /* Part C: Add (group,word) to tables */
      
      // ensure compact string representation, minimizing memory overhead
      String w = (String) internedWords.get(word);
      if (w == null) {
        word = new String(word); // ensure compact string
        internedWords.put(word, word);
      } else {
        word = w;
      }
      
      Integer group = lastGroup;
      if (num != lastNum) {
        group = new Integer(num);
        lastGroup = group;
        lastNum = num;
      }
      
      // add word --> group
      ArrayList groups = (ArrayList) word2Groups.get(word);
      if (groups == null) {
        groups = new ArrayList(1);
        word2Groups.put(word, groups);
      }
      groups.add(group);

      // add group --> word
      ArrayList words = (ArrayList) group2Words.get(group);
      if (words == null) {
        words = new ArrayList(1);
        group2Words.put(group, words);
      } 
      words.add(word);
    }
    
    
    /* Part D: compute index data structure */
    HashMap word2Syns = createIndex(word2Groups, group2Words);    
        
    /* Part E: minimize memory consumption by a factor 3 (or so) */
//    if (true) return word2Syns;
    word2Groups = null; // help gc
    group2Words = null; // help gc    
    return optimize(word2Syns, internedWords);
  }
  
  private HashMap createIndex(Map word2Groups, Map group2Words) {
    HashMap word2Syns = new HashMap();
    Iterator iter = word2Groups.entrySet().iterator();
    
    while (iter.hasNext()) { // for each word
      Map.Entry entry = (Map.Entry) iter.next();
      ArrayList group = (ArrayList) entry.getValue();     
      String word = (String) entry.getKey();
      
//      HashSet synonyms = new HashSet();
      TreeSet synonyms = new TreeSet();
      for (int i=group.size(); --i >= 0; ) { // for each groupID of word
        ArrayList words = (ArrayList) group2Words.get(group.get(i));
        for (int j=words.size(); --j >= 0; ) { // add all words       
          Object synonym = words.get(j); // note that w and word are interned
          if (synonym != word) { // a word is implicitly it's own synonym
            synonyms.add(synonym);
          }
        }
      }

      int size = synonyms.size();
      if (size > 0) {
        String[] syns = new String[size];
        if (size == 1)  
          syns[0] = (String) synonyms.first();
        else
          synonyms.toArray(syns);
//        if (syns.length > 1) Arrays.sort(syns);
//        if (DEBUG) System.err.println("word=" + word + ":" + Arrays.asList(syns));
        word2Syns.put(word, syns);
      }
    }
  
    return word2Syns;
  }

  private HashMap optimize(HashMap word2Syns, HashMap internedWords) {
    if (DEBUG) {
      System.err.println("before gc");
      for (int i=0; i < 10; i++) System.gc();
      System.err.println("after gc");
    }
    
    // collect entries
    int len = 0;
    int size = word2Syns.size();
    String[][] allSynonyms = new String[size][];
    String[] words = new String[size];
    Iterator iter = word2Syns.entrySet().iterator();
    for (int j=0; j < size; j++) {
      Map.Entry entry = (Map.Entry) iter.next();
      allSynonyms[j] = (String[]) entry.getValue(); 
      words[j] = (String) entry.getKey();
      len += words[j].length();
    }
    
    // assemble large string containing all words
    StringBuffer buf = new StringBuffer(len);
    for (int j=0; j < size; j++) buf.append(words[j]);
    String allWords = new String(buf.toString()); // ensure compact string across JDK versions
    buf = null;
    
    // intern words at app level via memory-overlaid substrings
    for (int p=0, j=0; j < size; j++) {
      String word = words[j];
      internedWords.put(word, allWords.substring(p, p + word.length()));
      p += word.length();
    }
    
    // replace words with interned words
    for (int j=0; j < size; j++) {
      String[] syns = allSynonyms[j];
      for (int k=syns.length; --k >= 0; ) {
        syns[k] = (String) internedWords.get(syns[k]);
      }
      Object replacement = syns;
      if (syns.length == 1) replacement = syns[0]; // minimize memory consumption some more
      word2Syns.remove(words[j]);
      word2Syns.put(internedWords.get(words[j]), replacement);
    }
    
    if (DEBUG) {
      words = null;
      allSynonyms = null;
      internedWords = null;
      allWords = null;
      System.err.println("before gc");
      for (int i=0; i < 10; i++) System.gc();
      System.err.println("after gc");
    }
    return word2Syns;
  }
  
  // the following utility methods below are copied from Apache style Nux library - see http://dsd.lbl.gov/nux
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