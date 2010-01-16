package org.apache.lucene.analysis;

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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Loader for text files that represent a list of stopwords.
 */
public class WordlistLoader {
 
  /**
   * Loads a text file associated with a given class (See
   * {@link Class#getResourceAsStream(String)}) and adds every line as an entry
   * to a {@link Set} (omitting leading and trailing whitespace). Every line of
   * the file should contain only one word. The words need to be in lower-case if
   * you make use of an Analyzer which uses LowerCaseFilter (like
   * StandardAnalyzer).
   * 
   * @param aClass
   *          a class that is associated with the given stopwordResource
   * @param stopwordResource
   *          name of the resource file associated with the given class
   * @return a {@link Set} with the file's words
   */
  public static Set<String> getWordSet(Class<?> aClass, String stopwordResource)
      throws IOException {
    final Reader reader = new BufferedReader(new InputStreamReader(aClass
        .getResourceAsStream(stopwordResource), "UTF-8"));
    try {
      return getWordSet(reader);
    } finally {
      reader.close();
    }
  }
  
  /**
   * Loads a text file associated with a given class (See
   * {@link Class#getResourceAsStream(String)}) and adds every line as an entry
   * to a {@link Set} (omitting leading and trailing whitespace). Every line of
   * the file should contain only one word. The words need to be in lower-case if
   * you make use of an Analyzer which uses LowerCaseFilter (like
   * StandardAnalyzer).
   * 
   * @param aClass
   *          a class that is associated with the given stopwordResource
   * @param stopwordResource
   *          name of the resource file associated with the given class
   * @param comment
   *          the comment string to ignore
   * @return a {@link Set} with the file's words
   */
  public static Set<String> getWordSet(Class<?> aClass,
      String stopwordResource, String comment) throws IOException {
    final Reader reader = new BufferedReader(new InputStreamReader(aClass
        .getResourceAsStream(stopwordResource), "UTF-8"));
    try {
      return getWordSet(reader, comment);
    } finally {
      reader.close();
    }
  }
  
  /**
   * Loads a text file and adds every line as an entry to a HashSet (omitting
   * leading and trailing whitespace). Every line of the file should contain only
   * one word. The words need to be in lowercase if you make use of an
   * Analyzer which uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param wordfile File containing the wordlist
   * @return A HashSet with the file's words
   */
  public static HashSet<String> getWordSet(File wordfile) throws IOException {
    FileReader reader = null;
    try {
      reader = new FileReader(wordfile);
      return getWordSet(reader);
    }
    finally {
      if (reader != null)
        reader.close();
    }
  }

  /**
   * Loads a text file and adds every non-comment line as an entry to a HashSet (omitting
   * leading and trailing whitespace). Every line of the file should contain only
   * one word. The words need to be in lowercase if you make use of an
   * Analyzer which uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param wordfile File containing the wordlist
   * @param comment The comment string to ignore
   * @return A HashSet with the file's words
   */
  public static HashSet<String> getWordSet(File wordfile, String comment) throws IOException {
    FileReader reader = null;
    try {
      reader = new FileReader(wordfile);
      return getWordSet(reader, comment);
    }
    finally {
      if (reader != null)
        reader.close();
    }
  }


  /**
   * Reads lines from a Reader and adds every line as an entry to a HashSet (omitting
   * leading and trailing whitespace). Every line of the Reader should contain only
   * one word. The words need to be in lowercase if you make use of an
   * Analyzer which uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param reader Reader containing the wordlist
   * @return A HashSet with the reader's words
   */
  public static HashSet<String> getWordSet(Reader reader) throws IOException {
    final HashSet<String> result = new HashSet<String>();
    BufferedReader br = null;
    try {
      if (reader instanceof BufferedReader) {
        br = (BufferedReader) reader;
      } else {
        br = new BufferedReader(reader);
      }
      String word = null;
      while ((word = br.readLine()) != null) {
        result.add(word.trim());
      }
    }
    finally {
      if (br != null)
        br.close();
    }
    return result;
  }

  /**
   * Reads lines from a Reader and adds every non-comment line as an entry to a HashSet (omitting
   * leading and trailing whitespace). Every line of the Reader should contain only
   * one word. The words need to be in lowercase if you make use of an
   * Analyzer which uses LowerCaseFilter (like StandardAnalyzer).
   *
   * @param reader Reader containing the wordlist
   * @param comment The string representing a comment.
   * @return A HashSet with the reader's words
   */
  public static HashSet<String> getWordSet(Reader reader, String comment) throws IOException {
    final HashSet<String> result = new HashSet<String>();
    BufferedReader br = null;
    try {
      if (reader instanceof BufferedReader) {
        br = (BufferedReader) reader;
      } else {
        br = new BufferedReader(reader);
      }
      String word = null;
      while ((word = br.readLine()) != null) {
        if (word.startsWith(comment) == false){
          result.add(word.trim());
        }
      }
    }
    finally {
      if (br != null)
        br.close();
    }
    return result;
  }

  /**
   * Loads a text file in Snowball format associated with a given class (See
   * {@link Class#getResourceAsStream(String)}) and adds all words as entries to
   * a {@link Set}. The words need to be in lower-case if you make use of an
   * Analyzer which uses LowerCaseFilter (like StandardAnalyzer).
   * 
   * @param aClass a class that is associated with the given stopwordResource
   * @param stopwordResource name of the resource file associated with the given
   *          class
   * @return a {@link Set} with the file's words
   * @see #getSnowballWordSet(Reader)
   */
  public static Set<String> getSnowballWordSet(Class<?> aClass,
      String stopwordResource) throws IOException {
    final Reader reader = new BufferedReader(new InputStreamReader(aClass
        .getResourceAsStream(stopwordResource), "UTF-8"));
    try {
      return getSnowballWordSet(reader);
    } finally {
      reader.close();
    }
  }
  
  /**
   * Reads stopwords from a stopword list in Snowball format.
   * <p>
   * The snowball format is the following:
   * <ul>
   * <li>Lines may contain multiple words separated by whitespace.
   * <li>The comment character is the vertical line (&#124;).
   * <li>Lines may contain trailing comments.
   * </ul>
   * </p>
   * 
   * @param reader Reader containing a Snowball stopword list
   * @return A Set with the reader's words
   */
  public static Set<String> getSnowballWordSet(Reader reader)
      throws IOException {
    final Set<String> result = new HashSet<String>();
    BufferedReader br = null;
    try {
      if (reader instanceof BufferedReader) {
        br = (BufferedReader) reader;
      } else {
        br = new BufferedReader(reader);
      }
      String line = null;
      while ((line = br.readLine()) != null) {
        int comment = line.indexOf('|');
        if (comment >= 0) line = line.substring(0, comment);
        String words[] = line.split("\\s+");
        for (int i = 0; i < words.length; i++)
          if (words[i].length() > 0) result.add(words[i]);
      }
    } finally {
      if (br != null) br.close();
    }
    return result;
  }


  /**
   * Reads a stem dictionary. Each line contains:
   * <pre>word<b>\t</b>stem</pre>
   * (i.e. two tab separated words)
   *
   * @return stem dictionary that overrules the stemming algorithm
   * @throws IOException 
   */
  public static HashMap<String, String> getStemDict(File wordstemfile) throws IOException {
    if (wordstemfile == null)
      throw new NullPointerException("wordstemfile may not be null");
    final HashMap<String, String> result = new HashMap<String,String>();
    BufferedReader br = null;
    
    try {
      br = new BufferedReader(new FileReader(wordstemfile));
      String line;
      while ((line = br.readLine()) != null) {
        String[] wordstem = line.split("\t", 2);
        result.put(wordstem[0], wordstem[1]);
      }
    } finally {
      if(br != null)
        br.close();
    }
    return result;
  }

}
