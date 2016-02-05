/*
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
package org.apache.lucene.analysis.ja;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;

/**
 * Analyzer for Japanese that uses morphological analysis.
 * @see JapaneseTokenizer
 */
public class JapaneseAnalyzer extends StopwordAnalyzerBase {
  private final Mode mode;
  private final Set<String> stoptags;
  private final UserDictionary userDict;
  
  public JapaneseAnalyzer() {
    this(null, JapaneseTokenizer.DEFAULT_MODE, DefaultSetHolder.DEFAULT_STOP_SET, DefaultSetHolder.DEFAULT_STOP_TAGS);
  }
  
  public JapaneseAnalyzer(UserDictionary userDict, Mode mode, CharArraySet stopwords, Set<String> stoptags) {
    super(stopwords);
    this.userDict = userDict;
    this.mode = mode;
    this.stoptags = stoptags;
  }
  
  public static CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  public static Set<String> getDefaultStopTags(){
    return DefaultSetHolder.DEFAULT_STOP_TAGS;
  }
  
  /**
   * Atomically loads DEFAULT_STOP_SET, DEFAULT_STOP_TAGS in a lazy fashion once the 
   * outer class accesses the static final set the first time.
   */
  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET;
    static final Set<String> DEFAULT_STOP_TAGS;

    static {
      try {
        DEFAULT_STOP_SET = loadStopwordSet(true, JapaneseAnalyzer.class, "stopwords.txt", "#");  // ignore case
        final CharArraySet tagset = loadStopwordSet(false, JapaneseAnalyzer.class, "stoptags.txt", "#");
        DEFAULT_STOP_TAGS = new HashSet<>();
        for (Object element : tagset) {
          char chars[] = (char[]) element;
          DEFAULT_STOP_TAGS.add(new String(chars));
        }
      } catch (IOException ex) {
        // default set should always be present as it is part of the distribution (JAR)
        throw new RuntimeException("Unable to load default stopword or stoptag set");
      }
    }
  }
  
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer tokenizer = new JapaneseTokenizer(userDict, true, mode);
    TokenStream stream = new JapaneseBaseFormFilter(tokenizer);
    stream = new JapanesePartOfSpeechStopFilter(stream, stoptags);
    stream = new CJKWidthFilter(stream);
    stream = new StopFilter(stream, stopwords);
    stream = new JapaneseKatakanaStemFilter(stream);
    stream = new LowerCaseFilter(stream);
    return new TokenStreamComponents(tokenizer, stream);
  }
}
