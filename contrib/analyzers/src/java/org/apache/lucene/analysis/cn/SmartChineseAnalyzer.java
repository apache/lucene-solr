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

package org.apache.lucene.analysis.cn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SentenceTokenizer;
import org.apache.lucene.analysis.cn.smart.WordSegmenter;
import org.apache.lucene.analysis.cn.smart.WordTokenizer;

import org.apache.lucene.analysis.cn.smart.AnalyzerProfile; // for javadoc

/**
 * <p>
 * SmartChineseAnalyzer is an analyzer for Chinese or mixed Chinese-English text.
 * The analyzer uses probabilistic knowledge to find the optimal word segmentation for Simplified Chinese text.
 * The text is first broken into sentences, then each sentence is segmented into words.
 * </p>
 * <p>
 * Segmentation is based upon the <a href="http://en.wikipedia.org/wiki/Hidden_Markov_Model">Hidden Markov Model</a>. 
 * A large training corpus was used to calculate Chinese word frequency probability.
 * </p>
 * <p>
 * This analyzer requires a dictionary to provide statistical data. 
 * To specify the location of the dictionary data, refer to {@link AnalyzerProfile}
 * </p>
 * <p>
 * The included dictionary data is from <a href="http://www.ictclas.org">ICTCLAS1.0</a>.
 * Thanks to ICTCLAS for their hard work, and for contributing the data under the Apache 2 License!
 * </p>
 */
public class SmartChineseAnalyzer extends Analyzer {

  private Set stopWords = null;

  private WordSegmenter wordSegment;

  /**
   * Create a new SmartChineseAnalyzer, using the default stopword list.
   */
  public SmartChineseAnalyzer() {
    this(true);
  }

  /**
   * <p>
   * Create a new SmartChineseAnalyzer, optionally using the default stopword list.
   * </p>
   * <p>
   * The included default stopword list is simply a list of punctuation.
   * If you do not use this list, punctuation will not be removed from the text!
   * </p>
   * 
   * @param useDefaultStopWords true to use the default stopword list.
   */
  public SmartChineseAnalyzer(boolean useDefaultStopWords) {
    if (useDefaultStopWords) {
      stopWords = loadStopWords(this.getClass().getResourceAsStream(
          "stopwords.txt"));
    }
    wordSegment = new WordSegmenter();
  }

  /**
   * <p>
   * Create a new SmartChineseAnalyzer, using the provided {@link Set} of stopwords.
   * </p>
   * <p>
   * Note: the set should include punctuation, unless you want to index punctuation!
   * </p>
   * @param stopWords {@link Set} of stopwords to use.
   * @see SmartChineseAnalyzer#loadStopWords(InputStream)
   */
  public SmartChineseAnalyzer(Set stopWords) {
    this.stopWords = stopWords;
    wordSegment = new WordSegmenter();
  }

  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new SentenceTokenizer(reader);
    result = new WordTokenizer(result, wordSegment);
    // result = new LowerCaseFilter(result);
    // LowerCaseFilter is not needed, as SegTokenFilter lowercases Basic Latin text.
    // The porter stemming is too strict, this is not a bug, this is a feature:)
    result = new PorterStemFilter(result);
    if (stopWords != null) {
      result = new StopFilter(result, stopWords, false);
    }
    return result;
  }

  /**
   * Utility function to return a {@link Set} of stopwords from a UTF-8 encoded {@link InputStream}.
   * The comment "//" can be used in the stopword list.
   * 
   * @param input {@link InputStream} of UTF-8 encoded stopwords
   * @return {@link Set} of stopwords.
   */
  public static Set loadStopWords(InputStream input) {
    /*
     * Note: WordListLoader is not used here because this method allows for inline "//" comments.
     * WordListLoader will only filter out these comments if they are on a separate line.
     */
    String line;
    Set stopWords = new HashSet();
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(input,
          "UTF-8"));
      while ((line = br.readLine()) != null) {
        if (line.indexOf("//") != -1) {
          line = line.substring(0, line.indexOf("//"));
        }
        line = line.trim();
        if (line.length() != 0)
          stopWords.add(line.toLowerCase());
      }
      br.close();
    } catch (IOException e) {
      System.err.println("WARNING: cannot open stop words list!");
    }
    return stopWords;
  }

}
