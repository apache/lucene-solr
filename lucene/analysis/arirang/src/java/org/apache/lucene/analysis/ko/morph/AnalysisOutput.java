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

import org.apache.lucene.analysis.ko.dic.CompoundEntry;

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
    this(stem, josa, eomi, patn, SCORE_ANALYSIS);
  }
  
  public AnalysisOutput(String stem, String josa, String eomi, int patn, int score) {
    this(stem, josa, eomi, (char)0, patn, score);
  }
  
  public AnalysisOutput(String stem, String josa, String eomi, char pos, int patn, int score) {
    this.score = score;    
    this.stem = stem;
    this.josa = josa;
    this.eomi = eomi;
    this.patn = patn;
    this.pos = pos;
  }
  
  public int getScore() {
    return score;
  }
  
  public void setScore(int score) {
    this.score = score;
  }
  
  public int getPatn() {
    return patn;
  }
  
  public void setPatn(int patn) {
    this.patn = patn;
  }

  public String getStem() {
    return stem;
  }  
  
  public void setStem(String stem) {
    this.stem = stem;
  }
  
  public char getPos() {
    return pos;
  }
  
  public void setPos(char pos) {
    this.pos = pos;
  }
  
  public String getNsfx() {
    return nsfx;
  }
  
  public void setNsfx(String nsfx) {
    this.nsfx = nsfx;
  }
  
  public String getJosa() {
    return josa;
  }
  
  public void setJosa(String josa) {
    this.josa = josa;
  }
  
  public String getEomi() {
    return eomi;
  }
  
  public void setEomi(String eomi) {
    this.eomi = eomi;
  }
  
  public List<String> getElist() {
    return elist;
  }
  
  public void addElist(String element) {
    elist.add(element);
  }
    
  public void setElist(String element, int index) {
    elist.set(index, element);
  }
  
  public String getPomi() {
    return pomi;
  }
  
  public void setPomi(String pomi) {
    this.pomi = pomi;
  }
  
  public String getXverb() {
    return xverb;
  }
  
  public void setXverb(String xverb) {
    this.xverb = xverb;
  }
  
  public String getVsfx() {
    return vsfx;
  }
  
  public void setVsfx(String vsfx) {
    this.vsfx = vsfx;
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
  
  public List<CompoundEntry> getCNounList() {
    return compound;
  }
  
  public void setCNounList(List<CompoundEntry> cnoun) {
    compound = cnoun;
  }
  
  public void addCNoun(CompoundEntry entry) {
    compound.add(entry);
  }
  
  public void addCNouns(List<CompoundEntry> cnoun) {
    compound.addAll(cnoun);
  }
  
  // nocommit
  public void setCNounList(CompoundEntry[] cnoun) {
    // WTF, something holds on to 'previous' cnoun list after MorphAnalyzer.confirmCnoun sets it to something new.
    compound = new ArrayList<CompoundEntry>();
    addCNouns(cnoun);
  }
  
  // nocommit
  public void addCNouns(CompoundEntry[] cnoun) {
    for (CompoundEntry e : cnoun) {
      compound.add(e);
    }
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
