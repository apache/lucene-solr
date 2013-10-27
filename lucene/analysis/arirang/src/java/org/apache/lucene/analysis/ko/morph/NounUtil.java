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
import org.apache.lucene.analysis.ko.dic.DictionaryUtil;
import org.apache.lucene.analysis.ko.dic.SyllableFeatures;
import org.apache.lucene.analysis.ko.dic.WordEntry;

class NounUtil {
  private NounUtil() {}
    
  /**
   * 
   * 어간부가 음/기 로 끝나는 경우
   * 
   * @param o the analyzed output
   * @param candidates  candidates
   */
  static boolean analysisMJ(AnalysisOutput o, List<AnalysisOutput> candidates) {

    int strlen = o.getStem().length();
       
    if(strlen<2) return false;       

    char[] chrs = MorphUtil.decompose(o.getStem().charAt(strlen-1));

    if(o.getStem().charAt(strlen-1)!='기'&&!(chrs.length==3&&chrs[2]=='ㅁ')) return false;

    String start = o.getStem();
    String end = "";
    if(o.getStem().charAt(strlen-1)=='기') {
      start = o.getStem().substring(0,strlen-1);
      end = "기";
    }else if(o.getStem().charAt(strlen-1)=='음') {
      start = o.getStem().substring(0,strlen-1);
      end = "음";
    }

    String[] eomis = EomiUtil.splitEomi(start, end);
    if(eomis==null) return false;
    String[] pomis = EomiUtil.splitPomi(eomis[0]);
    o.setStem(pomis[0]);
    o.addElist(eomis[1]);       
    o.setPomi(pomis[1]);
     
    if(analysisVMJ(o.clone(),candidates)) return true;         
    if(analysisNSMJ(o.clone(),candidates)) return true;
    if(analysisVMXMJ(o.clone(),candidates)) return true;
              
    if(DictionaryUtil.getVerb(o.getStem())!=null) {
      o.setPos(PatternConstants.POS_VERB);
      o.setPatn(PatternConstants.PTN_VMJ);
      o.setScore(AnalysisOutput.SCORE_CORRECT);
      candidates.add(o);
      return true;
    }
     
    return false;
      
  }

  /**
   * 용언 + '음/기' + 조사(PTN_VMXMJ)
   * @param o the analyzed output
   * @param candidates  candidates
   */
  private static boolean analysisVMJ(AnalysisOutput o, List<AnalysisOutput> candidates) {

    String[] irrs =  IrregularUtil.restoreIrregularVerb(o.getStem(), o.getElist().get(0));
    if(irrs!=null) {
      o.setStem(irrs[0]);
      o.setElist(irrs[1],0);
    }
        
    if(DictionaryUtil.getVerb(o.getStem())!=null) {
      o.setPatn(PatternConstants.PTN_VMJ);
      o.setPos(PatternConstants.POS_VERB);
      o.setScore(AnalysisOutput.SCORE_CORRECT);
      candidates.add(o);
      return true;
    }
      
    return false;
  }
    
  /**
   * 용언 + '아/어' + 보조용언 + '음/기' + 조사(PTN_VMXMJ)
   * @param o the analyzed output
   * @param candidates  candidates
   */
  private static boolean analysisVMXMJ(AnalysisOutput o, List<AnalysisOutput> candidates) {
  
    int idxXVerb = VerbUtil.endsWithXVerb(o.getStem());

    if(idxXVerb!=-1) { // 2. 사랑받아보다
      String eogan = o.getStem().substring(0,idxXVerb);
      o.setXverb(o.getStem().substring(idxXVerb));

      String[] stomis = null;
      if(eogan.endsWith("아")||eogan.endsWith("어"))
        stomis = EomiUtil.splitEomi(eogan.substring(0,eogan.length()-1),eogan.substring(eogan.length()-1));
      else
        stomis = EomiUtil.splitEomi(eogan,"");
      if(stomis==null) return false;
  
      String[] irrs =  IrregularUtil.restoreIrregularVerb(stomis[0], stomis[1]);
      if(irrs!=null) {
        o.setStem(irrs[0]);
        o.addElist(irrs[1]);
      }else {
        o.setStem(stomis[0]);
        o.addElist(stomis[1]);
      }
        
      if(DictionaryUtil.getVerb(o.getStem())!=null) {
        o.setPatn(PatternConstants.PTN_VMXMJ);
        o.setPos(PatternConstants.POS_VERB);
        o.setScore(AnalysisOutput.SCORE_CORRECT);
        candidates.add(o);
        return true;
      }else if(analysisNSMXMJ(o, candidates)){
        return true;          
      }

    }
      
    return false;
  }
    
  /**
   * 체언 + 용언화접미사 + '음/기' + 조사 (PTN_NSMJ)
   * @param o the analyzed output
   * @param candidates  candidates
   */
  private static boolean analysisNSMJ(AnalysisOutput o, List<AnalysisOutput> candidates) {

    int idxVbSfix = VerbUtil.endsWithVerbSuffix(o.getStem());        
    if(idxVbSfix==-1) return false;
    
    o.setVsfx(o.getStem().substring(idxVbSfix));
    o.setStem(o.getStem().substring(0,idxVbSfix));
    o.setPatn(PatternConstants.PTN_NSMJ);
    o.setPos(PatternConstants.POS_NOUN);
      
    WordEntry entry = DictionaryUtil.getWordExceptVerb(o.getStem());

    if(entry!=null) {
      if(!entry.isNoun()) return false;
      else if(o.getVsfx().equals("하") && !entry.hasDOV()) return false;
      else if(o.getVsfx().equals("되") && !entry.hasBEV()) return false;
      else if(o.getVsfx().equals("내") && !entry.hasNE()) return false;
      o.setScore(AnalysisOutput.SCORE_CORRECT); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.      
    }else {
      o.setScore(AnalysisOutput.SCORE_ANALYSIS); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.
    }
    
    candidates.add(o);
      
    return true;
  }         
     
  private static boolean analysisNSMXMJ(AnalysisOutput o, List<AnalysisOutput> candidates) {

    int idxVbSfix = VerbUtil.endsWithVerbSuffix(o.getStem());        
    if(idxVbSfix==-1) return false;
    
    o.setVsfx(o.getStem().substring(idxVbSfix));
    o.setStem(o.getStem().substring(0,idxVbSfix));
    o.setPatn(PatternConstants.PTN_NSMXMJ);
    o.setPos(PatternConstants.POS_NOUN);
      
    WordEntry entry = DictionaryUtil.getWordExceptVerb(o.getStem());

    if(entry!=null) {
      if(!entry.isNoun()) return false;
      else if(o.getVsfx().equals("하") && !entry.hasDOV()) return false;
      else if(o.getVsfx().equals("되") && !entry.hasBEV()) return false;
      else if(o.getVsfx().equals("내") && !entry.hasNE()) return false;
      o.setScore(AnalysisOutput.SCORE_CORRECT); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.      
    }else {
      o.setScore(AnalysisOutput.SCORE_ANALYSIS); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.
    }
    
    candidates.add(o);
      
    return true;
  }
  
  private static boolean isDNoun(char ch) {
    switch(ch) {
      case '등':
      case '들':
      case '상':
      case '간':
      case '뿐':
      case '별':
      case '적': return true;
      default: return false;
    }
  }
    
  /*
     * 마지막 음절이 명사형 접미사(등,상..)인지 조사한다.
     */
  static boolean confirmDNoun(AnalysisOutput output) {
    final String currentStem = output.getStem();
    // empty or single character
    if (currentStem.length() <= 1) {
      return false;
    }
    
    // check suffix char
    final char suffix = currentStem.charAt(currentStem.length()-1);
    if (!isDNoun(suffix)) {
      return false;
    }
    
    // remove suffix
    String stem = currentStem.substring(0, currentStem.length()-1);
    output.setNsfx(Character.toString(suffix));
    output.setStem(stem);
          
    WordEntry cnoun = DictionaryUtil.getAllNoun(stem);
    if(cnoun != null)  {
      if(cnoun.isCompoundNoun())
        output.setCNounList(cnoun.getCompounds());
      else
        output.setCNounList(new ArrayList<CompoundEntry>()); // TODO: dont make all these lists
      output.setScore(AnalysisOutput.SCORE_CORRECT);
    }
          
    return true;
  }
      
  static boolean endsWith2Josa(String input) {
    for (int i = input.length()-2; i > 0; i--) {
      String josa = input.substring(i);

      if (DictionaryUtil.existJosa(josa)) {
        return true;
      } else if (!SyllableFeatures.hasFeature(josa.charAt(0), SyllableFeatures.JOSA2)) {
        return false;
      }
    }
    return false;
  }
      
  static double countFoundNouns(AnalysisOutput o) {
    int count = 0;
    for (CompoundEntry entry : o.getCNounList()) {
      if (entry.isExist()) {
        count++;
      }
    }
    return (count*100)/o.getCNounList().size();
  }
}
