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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.ko.dic.CompoundEntry;
import org.apache.lucene.analysis.ko.dic.DictionaryUtil;
import org.apache.lucene.analysis.ko.dic.SyllableFeatures;
import org.apache.lucene.analysis.ko.dic.WordEntry;

public class WordSpaceAnalyzer {

  private final MorphAnalyzer morphAnal = new MorphAnalyzer(false);
  
  public List<AnalysisOutput> analyze(String input)  {

    WSOutput output = new WSOutput();
    
    int wStart = 0;
        
    Map<Integer, Integer> fCounter = new HashMap<Integer, Integer>();
    
    for(int i=0;i<input.length();i++) {           
      
      char ch = input.charAt(i);
      
      String prefix = i==input.length()-1 ? "X" : input.substring(wStart,i+2);          
      boolean prefixExists = DictionaryUtil.hasWordPrefix(prefix);
      
      List<AnalysisOutput> candidates = new ArrayList<AnalysisOutput>();    
      
      WordEntry entry = null;
          
      if(input.charAt(i)=='있' || input.charAt(i)=='없' || input.charAt(i)=='앞') {
        addSingleWord(input.substring(wStart,i), candidates);
        
      // 다음 음절이 2음절 이상 단어에 포함되어 있고 마지막 음절이 아니라면   띄워쓰기 위치가 아닐 가능성이 크다.
      // 부사, 관형사, 감탄사 등 단일어일 가능성인 경우 띄워쓰기가 가능하나, 
      // 이 경우는 다음 음절을 조사하여 
      } else if(i!= input.length()-1 && prefixExists) { 
        // 아무짓도 하지 않음.
      } else if(!prefixExists && 
          (entry=DictionaryUtil.getBusa(input.substring(wStart,i+1)))!=null) {        
        candidates.add(buildSingleOutput(entry));
        
      // 현 음절이 조사나 어미가 시작되는 음절일 가능성이 있다면... 
      } else if (SyllableFeatures.hasFeature(ch, SyllableFeatures.EOGAN) || 
                 SyllableFeatures.hasFeature(ch, SyllableFeatures.JOSA1)) {        
        if (SyllableFeatures.hasFeature(ch, SyllableFeatures.JOSA1)) { 
          candidates.addAll(anlysisWithJosa(input.substring(wStart), i-wStart));
        }

        if (SyllableFeatures.hasFeature(ch, SyllableFeatures.EOGAN)) { 
          candidates.addAll(anlysisWithEomi(input.substring(wStart), i-wStart));
        }
      }
  
      // 후보가 될 가능성이 높은 순으로 정렬한다.
      Collections.sort(candidates, new WSOuputComparator());
      
      // 길이가 가장 긴 단어를 단일어로 추가한다.
      appendSingleWord(candidates);
      
      // 분석에 실패한 단어를 
      analysisCompouns(candidates);
      
      // 후보가 될 가능성이 높은 순으로 정렬한다.
      Collections.sort(candidates, new WSOuputComparator());      
      
      int reseult = validationAndAppend(output, candidates, input);
      if(reseult==1) {
        i = output.getLastEnd()-1;
        wStart = output.getLastEnd();
      } else if(reseult==-1) {
        Integer index = fCounter.get(output.getLastEnd());
        if(index==null) index = output.getLastEnd();
        else index = index + 1;
        i = index;
        wStart = output.getLastEnd();
        fCounter.put(output.getLastEnd(), index);       
      }

    } // end of for
    
    // 분석에 실패하였다면 원래 문자열을 되돌려 준다.
    if(output.getLastEnd()<input.length()) {
      
      String source = input.substring(output.getLastEnd());
      int score = DictionaryUtil.hasWord(source) ? AnalysisOutput.SCORE_CORRECT : AnalysisOutput.SCORE_ANALYSIS;
      AnalysisOutput o =new AnalysisOutput(source,null,null,PatternConstants.POS_NOUN,
          PatternConstants.PTN_N,score);
      
      o.setSource(source);
      output.getPhrases().add(o);
      morphAnal.confirmCNoun(o);
      
    }
        
    return output.getPhrases();
  }
  
  /**
   * calculate the score which is the worst score of the derived word scores
   * @param list  input
   * @return  calculated score
   */
  public int getOutputScore(List<AnalysisOutput> list) {
    int score = 100;
    for (AnalysisOutput o : list) {
      score = Math.min(score, o.getScore());
    }
    return score;
  }
  
  /**
   * 조사로 끝나는 어구를 분석한다.
   * @param snippet input
   * @param js  josa start position
   * @return  resulsts
   */
  private List<AnalysisOutput> anlysisWithJosa(String snippet, int js) {

    List<AnalysisOutput> candidates = new ArrayList<AnalysisOutput>();
    if(js<1) return candidates;
    
    int jend = findJosaEnd(snippet, js);

    if(jend==-1) return candidates; // 타당한 조사가 아니라면...
  
    String input = snippet.substring(0,jend);
    
    for (int i = input.length()-1; i > 0; i--) {
      String stem = input.substring(0,i);
      String josa = input.substring(i);

      char ch = josa.charAt(0);  
      
      if (SyllableFeatures.hasFeature(ch, SyllableFeatures.JOSA1)) {
        morphAnal.analysisWithJosa(stem, josa, candidates);       
      }
      
      if (!SyllableFeatures.hasFeature(ch, SyllableFeatures.JOSA2)) {
        break;
      }      
    }
    
    if(input.length()==1) {
      AnalysisOutput o =new AnalysisOutput(input,null,null,PatternConstants.POS_NOUN,
           PatternConstants.PTN_N,AnalysisOutput.SCORE_ANALYSIS);
      candidates.add(o);
    }
    
    fillSourceString(input, candidates);
    
    return candidates;
  }
  
  /**
   * 조사의 첫음절부터 조사의 2음절이상에 사용될 수 있는 음절을 조사하여
   * 가장 큰 조사를 찾는다.
   * @param snippet snippet
   * @param jstart  josa start position
   * @return  position
   */
  private int findJosaEnd(String snippet, int jstart) {
    
    int jend = jstart;

    // [것을]이 명사를 이루는 경우는 없다.
    if(snippet.charAt(jstart-1)=='것'&&(snippet.charAt(jstart)=='을')) return jstart+1;
    
    if(snippet.length()>jstart+2&&snippet.charAt(jstart+1)=='스') { // 사랑스러운, 자랑스러운 같은 경우르 처리함.
      char[] chrs = MorphUtil.decompose(snippet.charAt(jstart+2));

      if(chrs.length>=2&&chrs[0]=='ㄹ'&&chrs[1]=='ㅓ') return -1;
    }
    
    // 조사의 2음절로 사용될 수 마지막 음절을 찾는다.
    for (int i = jstart+1; i < snippet.length(); i++) {
      if (!SyllableFeatures.hasFeature(snippet.charAt(i), SyllableFeatures.JOSA2)) {
        break;
      }
      jend = i;       
    }
        
    int start = jend;
    boolean hasJosa = false;
    for(int i=start;i>=jstart;i--) {
      String str = snippet.substring(jstart,i+1);
      if(DictionaryUtil.existJosa(str) && !findNounWithinStr(snippet,i,i+2)) {
        jend = i;
        hasJosa = true;
        break;
      }
    }

    if(!hasJosa) return -1;
    
    return jend+1;
    
  }
  
  /**
   * 향후 계산이나 원 문자열을 보여주기 위해 source string 을 저장한다.
   * @param source  input text
   * @param candidates  candidates
   */
  private void fillSourceString(String source, List<AnalysisOutput> candidates) {
    
    for(AnalysisOutput o : candidates) {
      o.setSource(source);
    }
    
  }
  
  /**
   * 목록의 1번지가 가장 큰 길이를 가진다.
   * @param candidates  candidates
   */
  private void appendSingleWord(List<AnalysisOutput> candidates) {
  
    if(candidates.size()==0) return;
    
    String source = candidates.get(0).getSource();
    
    WordEntry entry = DictionaryUtil.getWordExceptVerb(source);
    
    if(entry!=null) {
      candidates.add(buildSingleOutput(entry));
    } else {

      if(candidates.get(0).getPatn()>PatternConstants.PTN_VM&&
          candidates.get(0).getPatn()<=PatternConstants.PTN_VMXMJ) return;
      
      if(source.length()<5) return;
      
      AnalysisOutput o =new AnalysisOutput(source,null,null,PatternConstants.POS_NOUN,
           PatternConstants.PTN_N,AnalysisOutput.SCORE_ANALYSIS);
      o.setSource(source);
      morphAnal.confirmCNoun(o);      
      if(o.getScore()==AnalysisOutput.SCORE_CORRECT) candidates.add(o);
    }       
  }
  
  private void addSingleWord(String source, List<AnalysisOutput> candidates) {
    
    WordEntry entry = DictionaryUtil.getWordExceptVerb(source);
    
    if(entry!=null) {
      candidates.add(buildSingleOutput(entry));
    } else {
      AnalysisOutput o =new AnalysisOutput(source,null,null,PatternConstants.POS_NOUN,
           PatternConstants.PTN_N,AnalysisOutput.SCORE_ANALYSIS);
      o.setSource(source);
      morphAnal.confirmCNoun(o);      
      candidates.add(o);
    }
    
//    Collections.sort(candidates, new WSOuputComparator());
    
  }
  
  private List<AnalysisOutput> anlysisWithEomi(String snipt, int estart) {

    List<AnalysisOutput> candidates = new ArrayList<AnalysisOutput>();
    
    int eend = findEomiEnd(snipt,estart);   

    // 동사앞에 명사분리
    int vstart = 0;
    for(int i=estart-1;i>=0;i--) {  
      if (DictionaryUtil.hasWordPrefix(snipt.substring(i, estart))) {
        vstart = i;
      } else {
        break;
      }
    }
      
    if(snipt.length()>eend &&
        DictionaryUtil.hasWordPrefix(snipt.substring(vstart,eend+1))) 
      return candidates;  // 다음음절까지 단어의 일부라면.. 분해를 안한다.
    
    String pvword = null;
    if(vstart!=0) pvword = snipt.substring(0,vstart);
      
    while(true) { // ㄹ,ㅁ,ㄴ 이기때문에 어미위치를 뒤로 잡았는데, 용언+어미의 형태가 아니라면.. 어구 끝을 하나 줄인다.
      String input = snipt.substring(vstart,eend);
      anlysisWithEomiDetail(input, candidates);       
      if(candidates.size()==0) break;   
      if(("ㄹ".equals(candidates.get(0).getEomi()) ||
          "ㅁ".equals(candidates.get(0).getEomi()) ||
          "ㄴ".equals(candidates.get(0).getEomi())) &&
          eend>estart+1 && candidates.get(0).getPatn()!=PatternConstants.PTN_VM &&
          candidates.get(0).getPatn()!=PatternConstants.PTN_NSM
          ) {
        eend--;
      }else if(pvword!=null&&candidates.get(0).getPatn()>=PatternConstants.PTN_VM&& // 명사 + 용언 어구 중에.. 용언어구로 단어를 이루는 경우는 없다.
          candidates.get(0).getPatn()<=PatternConstants.PTN_VMXMJ && DictionaryUtil.hasWord(input)){
        candidates.clear();
        break;
      }else if(pvword!=null&&VerbUtil.verbSuffix(candidates.get(0).getStem())
          && DictionaryUtil.hasNoun(pvword)) { // 명사 + 용언화 접미사 + 어미 처리
        candidates.clear();
        anlysisWithEomiDetail(snipt.substring(0,eend), candidates);
        pvword=null;
        break;        
      } else {
        break;
      }
    }
            
    if(candidates.size()>0&&pvword!=null) {
      AnalysisOutput o =new AnalysisOutput(pvword,null,null,PatternConstants.POS_NOUN,
          PatternConstants.PTN_N,AnalysisOutput.SCORE_ANALYSIS);  
      morphAnal.confirmCNoun(o);
      
      List<CompoundEntry> cnouns = o.getCNounList();
      if(cnouns.size()==0) {
        boolean is = DictionaryUtil.getWordExceptVerb(pvword)!=null;
        cnouns.add(new CompoundEntry(pvword, is));
      } 
      
      for(AnalysisOutput candidate : candidates) {
        candidate.getCNounList().addAll(cnouns);
        candidate.getCNounList().add(new CompoundEntry(candidate.getStem(), true));
        candidate.setStem(pvword+candidate.getStem()); // 이렇게 해야 WSOutput 에 복합명사 처리할 때 정상처리됨
      }
      
    }

    fillSourceString(snipt.substring(0,eend), candidates);
  
    return candidates;
  }
  
  private void anlysisWithEomiDetail(String input, List<AnalysisOutput> candidates) {
    int strlen = input.length();
    
    char ch = input.charAt(strlen-1);
    
    if (SyllableFeatures.hasFeature(ch, SyllableFeatures.YNPNA) ||
        SyllableFeatures.hasFeature(ch, SyllableFeatures.YNPLA) ||
        SyllableFeatures.hasFeature(ch, SyllableFeatures.YNPMA)) {
      morphAnal.analysisWithEomi(input,"",candidates);
    }
    
    for (int i = strlen-1; i > 0; i--) {
      String stem = input.substring(0,i);
      String eomi = input.substring(i);

      morphAnal.analysisWithEomi(stem,eomi,candidates); 
      
      if (!SyllableFeatures.hasFeature(eomi.charAt(0), SyllableFeatures.EOMI2)) {
        break;
      }
    }
  }
  
  /**
   * 어미의 첫음절부터 어미의 1음절이상에 사용될 수 있는 음절을 조사하여
   * 가장 큰 조사를 찾는다.
   * @param snippet snippet
   * @return  start position
   */
  private int findEomiEnd(String snippet, int estart) {
    
    int jend = 0;
    
    String tail = null;
    char[] chr = MorphUtil.decompose(snippet.charAt(estart));
    if(chr.length==3 && (chr[2]=='ㄴ')) {
      tail = '은'+snippet.substring(estart+1);
    }else if(chr.length==3 && (chr[2]=='ㄹ')) {
      tail = '을'+snippet.substring(estart+1);     
    }else if(chr.length==3 && (chr[2]=='ㅂ')) {
      tail = '습'+snippet.substring(estart+1);
    }else {
      tail = snippet.substring(estart);
    }       

    // 조사의 2음절로 사용될 수 마지막 음절을 찾는다.
    int start = 0;
    for (int i = 1; i < tail.length(); i++) {
      if (!SyllableFeatures.hasFeature(tail.charAt(i), SyllableFeatures.EOGAN)) {
        break;
      }
      start = i;        
    }
          
    for(int i=start;i>0;i--) { // 찾을 수 없더라도 1음절은 반드시 반환해야 한다.
      String str = tail.substring(0,i+1); 
      char[] chrs = MorphUtil.decompose(tail.charAt(i));  
      if(DictionaryUtil.existEomi(str) || 
          (i<2&&chrs.length==3&&(chrs[2]=='ㄹ'||chrs[2]=='ㅁ'||chrs[2]=='ㄴ'))) { // ㅁ,ㄹ,ㄴ이 연속된 용언은 없다, 사전을 보고 확인을 해보자
        jend = i;
        break;
      }
    }
    
    return estart+jend+1;
    
  }
  
/**
 * validation 후 후보가 될 가능성이 높은 최상위 것을 결과에 추가한다.
 * @param output  output
 * @param candidates  candates
 * @param input input
 * @return  valid position
 */
  private int validationAndAppend(WSOutput output, List<AnalysisOutput> candidates, String input)
  {
    
    if(candidates.size()==0) return 0;
    
    AnalysisOutput o = candidates.remove(0);    
    AnalysisOutput po = output.getPhrases().size()>0 ?  output.getPhrases().get(output.getPhrases().size()-1) : null;
    
    String ejend = o.getSource().substring(o.getStem().length());    
    
    char ja = 'x'; // 임의의 문자
    if(po!=null&&(po.getPatn()==PatternConstants.PTN_VM||po.getPatn()==PatternConstants.PTN_VMCM||po.getPatn()==PatternConstants.PTN_VMXM)) {   
      char[] chs = MorphUtil.decompose(po.getEomi().charAt(po.getEomi().length()-1));
      if(chs.length==3) ja=chs[2];
      else if(chs.length==1) ja=chs[0];     
    }
    
    int nEnd = output.getLastEnd()+o.getSource().length();
    
    boolean hasJOSA1 = nEnd < input.length() ? SyllableFeatures.hasFeature(input.charAt(nEnd), SyllableFeatures.JOSA1) : false;      
    
    // 밥먹고 같은 경우가 가능하나.. 먹고는 명사가 아니다.
    if(po!=null&&po.getPatn()==PatternConstants.PTN_N&&candidates.size()>0&&  
        o.getPatn()==PatternConstants.PTN_VM&&candidates.get(0).getPatn()==PatternConstants.PTN_N) {
      o = candidates.remove(0);       
    }else if(po!=null&&po.getPatn()>=PatternConstants.PTN_VM&&candidates.size()>0&&
        candidates.get(0).getPatn()==PatternConstants.PTN_N&&
        (ja=='ㄴ'||ja=='ㄹ')) { // 다녀가ㄴ, 사,람(e) 로 분해 방지
      o = candidates.remove(0);
    }
    
    //=============================================
    if(o.getPos()==PatternConstants.POS_NOUN && MorphUtil.hasVerbOnly(o.getStem())) {   
      output.removeLast();    
      return -1;
    }else if(nEnd<input.length() && hasJOSA1 
      && DictionaryUtil.hasNoun(o.getSource())) {
      return -1;
    }else if(nEnd<input.length() && o.getScore()==AnalysisOutput.SCORE_ANALYSIS 
      && DictionaryUtil.hasWordPrefix(ejend+input.charAt(nEnd))) { // 루씬하ㄴ 글형태소분석기 방지
      return -1;  
    }else if(po!=null&&po.getPatn()==PatternConstants.PTN_VM&&"ㅁ".equals(po.getEomi())&&
        o.getStem().equals("하")) { // 다짐 합니다 로 분리되는 것 방지
      output.removeLast();
      return -1;  
    }else if(po!=null&&po.getPatn()==PatternConstants.PTN_N&&VerbUtil.verbSuffix(o.getStem())&&
        !"있".equals(o.getStem())) { // 사랑받다, 사랑스러운을 처리, 그러나 있은 앞 단어와 결합하지 않는다.
      output.removeLast();
      return -1;      
    } else {  
      output.addPhrase(o);        
    }
        
    return 1;
  }
  
  
  private AnalysisOutput buildSingleOutput(WordEntry entry) {
    
    char pos = PatternConstants.POS_NOUN;
    
    int ptn = PatternConstants.PTN_N;
    
    if(!entry.isNoun()) {
      pos = PatternConstants.POS_AID;
      ptn = PatternConstants.PTN_AID;
    }
    
    AnalysisOutput o = new AnalysisOutput(entry.getWord(),null,null,pos,
        ptn,AnalysisOutput.SCORE_CORRECT);
    
    o.setSource(entry.getWord());
    
    return o;
  }
  
  private void analysisCompouns(List<AnalysisOutput> candidates) {
    
    // 복합명사 분해여부 결정하여 분해
    boolean changed = false;
    boolean correct = false;
    for(AnalysisOutput o:candidates) {
      
      if(o.getScore()==AnalysisOutput.SCORE_CORRECT) {
        if(o.getPatn()!=PatternConstants.PTN_NJ) correct=true;
        // "활성화해"가 [활성화(N),하(t),어야(e)] 분석성공하였는데 [활성/화해]분해되는 것을 방지
        if("하".equals(o.getVsfx())) break; 
        continue;
      }

      if(o.getPatn()<=PatternConstants.PTN_VM&&o.getStem().length()>2) {
         if(!(correct&&o.getPatn()==PatternConstants.PTN_N)) morphAnal.confirmCNoun(o);
         if(o.getScore()==AnalysisOutput.SCORE_CORRECT) changed=true;
      }
    }
    
  }

  /**
   * 
   * @param str 분석하고자 하는 전체 문자열
   * @param ws  문자열에서 명사를 찾는 시작위치
   * @param es  문자열에서 명사를 찾는 끝 위치
   * @return  if founded
   */
  private boolean findNounWithinStr(String str, int ws, int es) {
    if (str.length() < es) {
      return false;
    }
        
    for (int i = es; i < str.length(); i++) {
      if (SyllableFeatures.hasFeature(str.charAt(i), SyllableFeatures.JOSA1)) {       
        return DictionaryUtil.hasWord(str.substring(ws,i));
      }
    }
    
    return false;
  }
}
