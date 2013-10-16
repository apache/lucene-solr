package org.apache.lucene.analysis.ko.morph;

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

import java.util.ArrayList;
import java.util.List;

/**
 * 공백을 분석한 결과를 저장한다.
 */
public class SpaceOutput {

  // 분석된 결과 
  private AnalysisOutput output;
  
  // 분석 결과 앞에 있는 미등록어, 사람 이름은 대부분 이런 경우임.
  private List<AnalysisOutput> nrWords = new ArrayList<AnalysisOutput>();
  
  // 분석하기 이전의 어절
  private String source;
  
  public void initialize() {
    output = null;
    nrWords = new ArrayList<AnalysisOutput>();
    source = null;
  }

  /**
   * @return the output
   */
  public AnalysisOutput getOutput() {
    return output;
  }

  /**
   * @param output the output to set
   */
  public void setOutput(AnalysisOutput output) {
    this.output = output;
  }

  /**
   * @return the nrWord
   */
  public List<AnalysisOutput> getNRWords() {
    return nrWords;
  }

  /**
   */
  public void setNRWords(List<AnalysisOutput> words) {
    this.nrWords = words;
  }

  /**
   * 
   * @param word  input word
   */
  public void addNRWord(String word) {
    addNRWord(word, AnalysisOutput.SCORE_CORRECT);
  }
  
  /**
   * 
   * @param word  input word
   * @param score input score
   */
  public void addNRWord(String word, int score) {
    AnalysisOutput output = new AnalysisOutput(word,null,null,PatternConstants.PTN_N,score);
    output.setSource(word);
    output.setPos(PatternConstants.POS_NOUN);
    this.nrWords.add(0,output);
  }
  
  /**
   * @return the source
   */
  public String getSource() {
    return source;
  }

  /**
   * @param source the source to set
   */
  public void setSource(String source) {
    this.source = source;
  }
  
  /**
   * 분석된 전체 단어의 길이를 반환한다.
   */
  public int getLength() {
    
    if(this.source ==null) return 0;
    
    return this.source.length();
  }
}
