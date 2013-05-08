package org.apache.lucene.analysis.kr.utils;

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

import org.apache.lucene.analysis.kr.morph.AnalysisOutput;
import org.apache.lucene.analysis.kr.morph.MorphException;
import org.apache.lucene.analysis.kr.morph.PatternConstants;
import org.apache.lucene.analysis.kr.morph.WordEntry;

public class NounUtil {

  private static final List DNouns = new ArrayList();
    
  static {
    String[] strs = new String[]{"등", "들","상","간","뿐","별"};
    for(String str:strs) {
      DNouns.add(str);
    }
  };
    
  /**
   * 
   * 어간부가 음/기 로 끝나는 경우
   * 
   * @param o
   * @param candidates
   * @throws MorphException
   */
  public static boolean analysisMJ(AnalysisOutput o, List candidates) throws MorphException {

    int strlen = o.getStem().length();
       
    if(strlen<2) return false;       

    char[] chrs = MorphUtil.decompose(o.getStem().charAt(strlen-1));
    boolean success = false;

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
    if(eomis[0]==null) return false;
    String[] pomis = EomiUtil.splitPomi(eomis[0]);
    o.setStem(pomis[0]);
    o.addElist(eomis[1]);       
    o.setPomi(pomis[1]);
     
    try {
      if(analysisVMJ(o.clone(),candidates)) return true;         
      if(analysisVMXMJ(o.clone(),candidates)) return true;
      if(analysisNSMJ(o.clone(),candidates)) return true;
    } catch (CloneNotSupportedException e) {
      throw new MorphException(e.getMessage(),e);
    }
              
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
   * @param o
   * @param candidates
   * @throws MorphException
   */
  public static boolean analysisVMJ(AnalysisOutput o, List candidates) throws MorphException {

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
   * @param o
   * @param candidates
   * @throws MorphException
   */
  public static boolean analysisVMXMJ(AnalysisOutput o, List candidates) throws MorphException {
  
    int idxXVerb = VerbUtil.endsWithXVerb(o.getStem());

    if(idxXVerb!=-1) { // 2. 사랑받아보다
      String eogan = o.getStem().substring(0,idxXVerb);
      o.setXverb(o.getStem().substring(idxXVerb));

      String[] stomis = null;
      if(eogan.endsWith("아")||eogan.endsWith("어"))
        stomis = EomiUtil.splitEomi(eogan.substring(0,eogan.length()-1),eogan.substring(eogan.length()-1));
      else
        stomis = EomiUtil.splitEomi(eogan,"");
      if(stomis[0]==null) return false;
  
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
   * @param o
   * @param candidates
   * @throws MorphException
   */
  public static boolean analysisNSMJ(AnalysisOutput o, List candidates) throws MorphException {

    int idxVbSfix = VerbUtil.endsWithVerbSuffix(o.getStem());        
    if(idxVbSfix==-1) return false;
    
    o.setVsfx(o.getStem().substring(idxVbSfix));
    o.setStem(o.getStem().substring(0,idxVbSfix));
    o.setPatn(PatternConstants.PTN_NSMJ);
    o.setPos(PatternConstants.POS_NOUN);
      
    WordEntry entry = DictionaryUtil.getWordExceptVerb(o.getStem());

    if(entry!=null) {
      if(entry.getFeature(WordEntry.IDX_NOUN)=='0') return false;
      else if(o.getVsfx().equals("하")&&entry.getFeature(WordEntry.IDX_DOV)!='1') return false;
      else if(o.getVsfx().equals("되")&&entry.getFeature(WordEntry.IDX_BEV)!='1') return false;
      else if(o.getVsfx().equals("내")&&entry.getFeature(WordEntry.IDX_NE)!='1') return false;
      o.setScore(AnalysisOutput.SCORE_CORRECT); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.      
    }else {
      o.setScore(AnalysisOutput.SCORE_ANALYSIS); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.
    }
    
    candidates.add(o);
      
    return true;
  }         
     
  public static boolean analysisNSMXMJ(AnalysisOutput o, List candidates) throws MorphException {

    int idxVbSfix = VerbUtil.endsWithVerbSuffix(o.getStem());        
    if(idxVbSfix==-1) return false;
    
    o.setVsfx(o.getStem().substring(idxVbSfix));
    o.setStem(o.getStem().substring(0,idxVbSfix));
    o.setPatn(PatternConstants.PTN_NSMXMJ);
    o.setPos(PatternConstants.POS_NOUN);
      
    WordEntry entry = DictionaryUtil.getWordExceptVerb(o.getStem());

    if(entry!=null) {
      if(entry.getFeature(WordEntry.IDX_NOUN)=='0') return false;
      else if(o.getVsfx().equals("하")&&entry.getFeature(WordEntry.IDX_DOV)!='1') return false;
      else if(o.getVsfx().equals("되")&&entry.getFeature(WordEntry.IDX_BEV)!='1') return false;
      else if(o.getVsfx().equals("내")&&entry.getFeature(WordEntry.IDX_NE)!='1') return false;
      o.setScore(AnalysisOutput.SCORE_CORRECT); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.      
    }else {
      o.setScore(AnalysisOutput.SCORE_ANALYSIS); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.
    }
    
    candidates.add(o);
      
    return true;
  }
    
     
  /**
   * 복합명사인지 조사하고, 복합명사이면 단위명사들을 찾는다.
   * 복합명사인지 여부는 단위명사가 모두 사전에 있는지 여부로 판단한다.
   * 단위명사는 2글자 이상 단어에서만 찾는다.
   * @param o
   * @throws MorphException
   */     
//     public static boolean confirmCNoun(AnalysisOutput o) throws MorphException  {
//
//       if(o.getStem().length()<3) return false;
//       if(o.getPatn()==PatternConstants.PTN_N
//           &&DictionaryUtil.existJosa(o.getStem().substring(o.getStem().length()-2))) return false;
//       
//      List<CompoundEntry> results = new ArrayList();
//      List<List> queue = new ArrayList();
//      String prefix = o.getStem().substring(0,1);
//      
//      int pos = 0;
//      boolean moreTwo =  false;
//      while(pos<o.getStem().length()) {
//
//        List<WordEntry> nList = findNouns(o.getStem().substring(pos),queue.size(),o);
//        if(nList==null) return false;
//
//        if(pos==0&&DictionaryUtil.existPrefix(prefix)) nList.add(new WordEntry(prefix));
//
//        if(nList.size()==0) {
//          if(queue.size()==0) return false;
//          List<WordEntry> tmpList = queue.get(queue.size()-1);
//
//          tmpList.remove(tmpList.size()-1);  
//          pos -= results.get(queue.size()-1).getWord().length();        
//          if(tmpList.size()==0) {        
//            while(tmpList.size()==0) {        
//              results.remove(queue.size()-1);                  
//              queue.remove(tmpList);
//              if(queue.size()==0) return false;
//              
//              tmpList = queue.get(queue.size()-1);              
//              tmpList.remove(tmpList.size()-1);
//              if(tmpList.size()==0) continue;
//              
//              pos -= results.get(queue.size()-1).getWord().length();          
//              results.set(queue.size()-1, new CompoundEntry(tmpList.get(tmpList.size()-1).getWord(),pos));  
//              pos += tmpList.get(tmpList.size()-1).getWord().length();            
//                        
//            }          
//          }else {        
//            results.set(queue.size()-1, new CompoundEntry(tmpList.get(tmpList.size()-1).getWord(),pos));
//            pos += tmpList.get(tmpList.size()-1).getWord().length();
//          }    
//
//        } else {
//          queue.add(nList);
//          WordEntry noun = nList.get(nList.size()-1);
//          results.add(new CompoundEntry(noun.getWord(),pos));
//          pos += noun.getWord().length();
//          if(noun.getCompounds().size()>0) o.addCNoun(noun.getCompounds());
//          if(noun.getWord().length()>1) moreTwo=true;
//        }
//      }
//
//      if(results.size()>1&&DNouns.contains(results.get(results.size()-1).getWord())) {
//        CompoundEntry dnoun = results.remove(results.size()-1);
//              o.setStem(o.getStem().substring(0,o.getStem().length()-dnoun.getWord().length()));
//              o.setNsfx(dnoun.getWord());      
//      }
//      
//      if(results.size()>1) o.addCNoun(results);  
//      
//      o.setScore(AnalysisOutput.SCORE_CORRECT);
//      return true;
//    }
    
  /**
   * 복합명사에서 단위명사를 분리해낸다.
   * 리스트의 가장 마지막에 위치한 단어가 최장단어이다.
   * @param str  복합명사
   * @param pos
   * @param o    분석결과
   * return    단위명사 리스트
   * @throws MorphException
   */
  private static List findNouns(String str, int pos, AnalysisOutput o) throws MorphException {

    List<WordEntry> nList = new ArrayList();

    if(str.length()==2&&DictionaryUtil.existSuffix(str.substring(0,1))&&DNouns.contains(str.substring(1))) {
      o.setStem(o.getStem().substring(0,o.getStem().length()-1));
      o.setNsfx(str.substring(1));
      nList.add(new WordEntry(str.substring(0,1)));
      return nList;
    }else if(str.length()==2&&DictionaryUtil.existSuffix(str.substring(0,1))&&DictionaryUtil.existJosa(str.substring(1))) {
      return null;
    }
      
    if(pos>=2&&DictionaryUtil.existJosa(str)) return null;
      
    if(str.length()==1&&(DictionaryUtil.existSuffix(str)||DNouns.contains(str))) {
      nList.add(new WordEntry(str));
      return nList;
    }

    for(int i=1;i<str.length();i++) {    
      String sub = str.substring(0,i+1);    
      if(!DictionaryUtil.findWithPrefix(sub).hasNext()) break;
      WordEntry entry = DictionaryUtil.getCNoun(sub);  
      if(entry!=null) {          
        nList.add(entry);
      }
    }

    return nList;      
  }
    
  /*
     * 마지막 음절이 명사형 접미사(등,상..)인지 조사한다.
     */
  public static boolean confirmDNoun(AnalysisOutput output) throws MorphException {

    int strlen = output.getStem().length();
    String d = output.getStem().substring(strlen-1);      
    if(!DNouns.contains(d)) return false;

    String s = output.getStem().substring(0, strlen-1);
    output.setNsfx(d);
    output.setStem(s);
          
    WordEntry cnoun = DictionaryUtil.getCNoun(s);
    if(cnoun != null)  {
      if(cnoun.getFeature(WordEntry.IDX_NOUN)=='2')
        output.setCNoun(cnoun.getCompounds());
      else
        output.setCNoun(new ArrayList());
      output.setScore(AnalysisOutput.SCORE_CORRECT);
    }
          
    return true;
  }
    
//      public static int endsWithDNoun(String stem)   {
//          for(int i = 0; i < DNouns.length; i++)
//              if(stem.endsWith(DNouns[i]))
//                  return stem.lastIndexOf(DNouns[i]);
//
//          return -1;
//      }
      
  public static boolean endsWith2Josa(String input) throws MorphException {

    boolean josaFlag = true;
    for(int i=input.length()-2;i>0;i--) {
        
      String josa = input.substring(i);

      char[] feature =  SyllableUtil.getFeature(josa.charAt(0));    
      if(josaFlag&&DictionaryUtil.existJosa(josa)) return true;
  
        
      if(josaFlag&&feature[SyllableUtil.IDX_JOSA2]=='0') josaFlag = false;        
      if(!josaFlag) break;
    }
      
    return false;
  }
      
  public static double countFoundNouns(AnalysisOutput o) {
    int count = 0;
    for(int i=0;i<o.getCNounList().size();i++) {
      if(o.getCNounList().get(i).isExist()) count++;
    }
    return (count*100)/o.getCNounList().size();
  }
}
