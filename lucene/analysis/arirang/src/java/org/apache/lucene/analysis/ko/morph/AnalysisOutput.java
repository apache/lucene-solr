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

public class AnalysisOutput implements Cloneable {

  public static final int SCORE_CORRECT = 100;
  public static final int SCORE_COMPOUNDS = 70;  
  public static final int SCORE_ANALYSIS = 30;  
  public static final int SCORE_CANDIDATE = 10;
  public static final int SCORE_FAIL = 0;
  
  private String source; //분석하기 전 문자열(띄워쓰기 모듈에서 사용된다.)
  private int score; // score of this result
  private int patn; // word pattern
  private List<CompoundEntry> compound = new ArrayList<CompoundEntry>(); // compound noun of input word
  private String stem;
  private char pos; // 3 simplified stem type
  private String nsfx; // index of noun suffix
  private String josa; // josa string
  private String eomi;  // Eomi string
  private List<String> elist = new ArrayList<String>(); // unit-Eomi sequence
  private String pomi; // prefinal Eomi
  private String xverb; // Xverb string
  private String vsfx; // verb suffix
  
  private int maxWordLen = 0; // the max length of words within compound nouns
  private int dicWordLen = 0; // the sum of the length of words within compound nouns
  
  public AnalysisOutput(String stem, String josa, String eomi, int patn) {
    this.score = SCORE_ANALYSIS;    
    this.stem=stem;
    this.josa = josa;
    this.eomi = eomi;
    this.patn = patn;
  }
  
  public AnalysisOutput(String stem, String josa, String eomi, int patn, int score) {
    this(stem,josa,eomi,patn);
    this.score = score;
  }
  
  public AnalysisOutput(String stem, String josa, String eomi, char pos, int patn, int score) {
    this(stem,josa,eomi,patn,score);
    this.pos = pos;
  }
  
  public void setScore(int i) {
    this.score = i;
  }
  public void setPatn(int i) {
    this.patn = i;
  }
  
  public void setStem(String s) {
    this.stem = s;
  }
  

  public void setPos(char c) {
    this.pos = c;
  }
  
  public void setNsfx(String s) {
    this.nsfx = s;    
  }
  
  public void setJosa(String s) {
    this.josa = s;
  }
  
  public void setEomi(String s){
    this.eomi = s;
  }
  
  public void addElist(String l){
    this.elist.add(l);
  }
    
  public void setElist(String l, int index){
    this.elist.set(index,l);
  }
  
  public void setPomi(String s) {
    this.pomi = s;
  }
  public void setXverb(String s){
    this.xverb=s;
  }
  public void setVsfx(String s) {
    this.vsfx = s;
  }

  public int getScore() {
    return this.score;
  }
  public int getPatn() {
    return this.patn;
  }

  public String getStem() {
    return stem;
  }  
  public char getPos() {
    return this.pos;
  }
  public String getNsfx() {
    return this.nsfx;
  }
  public String getJosa() {
    return this.josa;
  }
  public String getEomi() {
    return this.eomi;
  }
  public List<String> getElist() {
    return this.elist;
  }
  public String getPomi(){
    return this.pomi;
  }
  public String getXverb() {
    return this.xverb;
  }
  public String getVsfx() {
    return this.vsfx;
  }
  
  public int getMaxWordLen() {
    return maxWordLen;
  }

  public void setMaxWordLen(int maxWordLen) {
    this.maxWordLen = maxWordLen;
  }

  public int getDicWordLen() {
    return dicWordLen;
  }

  public void setDicWordLen(int dicWordLen) {
    this.dicWordLen = dicWordLen;
  }
  
  public void addCNoun(CompoundEntry w) {
    compound.add(w);
  }
  
  public List<CompoundEntry> getCNounList() {
    return compound;
  }
  
  public void setCNoun(List<CompoundEntry> cnoun) {
    compound = cnoun;
  }
  
  public void addCNoun(List<CompoundEntry> cnoun) {
    compound.addAll(cnoun);
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
  
  public AnalysisOutput clone() {
    try {
      return (AnalysisOutput)super.clone();
    } catch (CloneNotSupportedException cnse) {
      throw new AssertionError();
    }
  }
}
