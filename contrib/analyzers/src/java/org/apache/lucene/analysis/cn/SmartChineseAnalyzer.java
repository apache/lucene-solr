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

/**
 * 
 * SmartChineseAnalyzer 是一个智能中文分词模块， 能够利用概率对汉语句子进行最优切分，
 * 并内嵌英文tokenizer，能有效处理中英文混合的文本内容。
 * 
 * 它的原理基于自然语言处理领域的隐马尔科夫模型(HMM)， 利用大量语料库的训练来统计汉语词汇的词频和跳转概率，
 * 从而根据这些统计结果对整个汉语句子计算最似然(likelihood)的切分。
 * 
 * 因为智能分词需要词典来保存词汇的统计值，SmartChineseAnalyzer的运行需要指定词典位置，如何指定词典位置请参考
 * org.apache.lucene.analysis.cn.smart.AnalyzerProfile
 * 
 * SmartChineseAnalyzer的算法和语料库词典来自于ictclas1.0项目(http://www.ictclas.org)，
 * 其中词典已获取www.ictclas.org的apache license v2(APLv2)的授权。在遵循APLv2的条件下，欢迎用户使用。
 * 在此感谢www.ictclas.org以及ictclas分词软件的工作人员的无私奉献！
 * 
 * @see org.apache.lucene.analysis.cn.smart.AnalyzerProfile
 * 
 */
public class SmartChineseAnalyzer extends Analyzer {

  private Set stopWords = null;

  private WordSegmenter wordSegment;

  public SmartChineseAnalyzer() {
    this(false);
  }

  /**
   * SmartChineseAnalyzer内部带有默认停止词库，主要是标点符号。如果不希望结果中出现标点符号，
   * 可以将useDefaultStopWords设为true， useDefaultStopWords为false时不使用任何停止词
   * 
   * @param useDefaultStopWords
   */
  public SmartChineseAnalyzer(boolean useDefaultStopWords) {
    if (useDefaultStopWords) {
      stopWords = loadStopWords(this.getClass().getResourceAsStream(
          "stopwords.txt"));
    }
    wordSegment = new WordSegmenter();
  }

  /**
   * 使用自定义的而不使用内置的停止词库，停止词可以使用SmartChineseAnalyzer.loadStopWords(InputStream)加载
   * 
   * @param stopWords
   * @see SmartChineseAnalyzer.loadStopWords(InputStream)
   */
  public SmartChineseAnalyzer(Set stopWords) {
    this.stopWords = stopWords;
    wordSegment = new WordSegmenter();
  }

  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new SentenceTokenizer(reader);
    result = new WordTokenizer(result, wordSegment);
    // result = new LowerCaseFilter(result);
    // 不再需要LowerCaseFilter，因为SegTokenFilter已经将所有英文字符转换成小写
    // stem太严格了, This is not bug, this feature:)
    result = new PorterStemFilter(result);
    if (stopWords != null) {
      result = new StopFilter(result, stopWords, false);
    }
    return result;
  }

  /**
   * 从停用词文件中加载停用词， 停用词文件是普通UTF-8编码的文本文件， 每一行是一个停用词，注释利用“//”， 停用词中包括中文标点符号， 中文空格，
   * 以及使用率太高而对索引意义不大的词。
   * 
   * @param input 停用词文件
   * @return 停用词组成的HashSet
   */
  public static Set loadStopWords(InputStream input) {
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
