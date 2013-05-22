package org.apache.lucene.analysis.ko;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ko.morph.AnalysisOutput;
import org.apache.lucene.analysis.ko.morph.CompoundEntry;
import org.apache.lucene.analysis.ko.morph.CompoundNounAnalyzer;
import org.apache.lucene.analysis.ko.morph.MorphAnalyzer;
import org.apache.lucene.analysis.ko.morph.MorphException;
import org.apache.lucene.analysis.ko.morph.PatternConstants;
import org.apache.lucene.analysis.ko.morph.WordEntry;
import org.apache.lucene.analysis.ko.morph.WordSpaceAnalyzer;
import org.apache.lucene.analysis.ko.utils.DictionaryUtil;
import org.apache.lucene.analysis.ko.utils.HanjaUtils;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class KoreanFilter extends TokenFilter {

  private LinkedList<IndexWord> morphQueue;
  
  private MorphAnalyzer morph;
  
  private WordSpaceAnalyzer wsAnal;
  
  private boolean bigrammable = true;
  
  private boolean hasOrigin = true;
  
  private boolean originCNoun = true;
  
  private boolean exactMatch = false;
  
  private char[] curTermBuffer;
    
  private int curTermLength;
    
  private String curType;
    
  private String curSource;
    
  private int tokStart;
    
  private int hanStart = 0; // 한글의 시작 위치, 복합명사일경우
    
  private int chStart = 0;
    
  private CompoundNounAnalyzer cnAnalyzer = new CompoundNounAnalyzer();
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    
  private static final String APOSTROPHE_TYPE = ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.APOSTROPHE];
  private static final String ACRONYM_TYPE = ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.ACRONYM];
    
  public KoreanFilter(TokenStream input) {
    super(input);
    morphQueue =  new LinkedList<IndexWord>();
    morph = new MorphAnalyzer();
    wsAnal = new WordSpaceAnalyzer();
    cnAnalyzer.setExactMach(false);
  }

  /**
   * 
   * @param input  input token stream
   * @param bigram  Whether the bigram index term return or not.
   */
  public KoreanFilter(TokenStream input, boolean bigram) {
    this(input);  
    bigrammable = bigram;
  }
  
  public KoreanFilter(TokenStream input, boolean bigram, boolean has) {
    this(input, bigram);
    hasOrigin = has;
  }
  
  public KoreanFilter(TokenStream input, boolean bigram, boolean has, boolean match) {
    this(input, bigram,has);
    this.exactMatch = match;
  }
  
  public KoreanFilter(TokenStream input, boolean bigram, boolean has, boolean match, boolean cnoun) {
    this(input, bigram,has, match);
    this.originCNoun = cnoun;
  }
  
  public final boolean incrementToken() throws IOException {

    if(curTermBuffer!=null&&morphQueue.size()>0) {
      setTermBufferByQueue(false);
      return true;
    }

    if(!input.incrementToken()) return false;
    
    curTermBuffer = termAtt.buffer().clone();
    curTermLength = termAtt.length();
    tokStart = offsetAtt.startOffset();    
    curType = typeAtt.type();

    try {      
      if(KoreanTokenizer.TOKEN_TYPES[KoreanTokenizer.KOREAN].equals(curType)) {            
        analysisKorean(new String(curTermBuffer,0,termAtt.length()));
      } else if(KoreanTokenizer.TOKEN_TYPES[KoreanTokenizer.CHINESE].equals(curType)) {
        analysisChinese(new String(curTermBuffer,0,termAtt.length()));
      } else {
        analysisETC(new String(curTermBuffer,0,termAtt.length()));
      }        
    }catch(MorphException e) {
      throw new IOException("Korean Filter MorphException\n"+e.getMessage());
    }

    if(morphQueue!=null&&morphQueue.size()>0) {
      setTermBufferByQueue(true);  
    } else {
      return incrementToken();
    }

    return true;

  }
  
  /**
   * queue에 저장된 값으로 buffer의 값을 복사한다.
   */
  private void setTermBufferByQueue(boolean isFirst) {
    
    clearAttributes();
        
    IndexWord iw = morphQueue.removeFirst();
    int pos = iw.getOffset();

    termAtt.copyBuffer(iw.getWord().toCharArray(), 0, iw.getWord().length());
    offsetAtt.setOffset(tokStart+pos, tokStart + pos + iw.getWord().length());
        
    if(!isFirst && iw.getOffset()==0) {
      posIncrAtt.setPositionIncrement(0);
//          posLenAtt.setPositionLength(iw.getWord().length());
    }
        
  }
  
  /**
   * 한글을 분석한다.
   * @throws MorphException
   */
  private void analysisKorean(String input) throws MorphException {

    List<AnalysisOutput> outputs = morph.analyze(input);
    if(outputs.size()==0) return;
    
    Map<String,IndexWord> map = new LinkedHashMap<String,IndexWord>();
    if(hasOrigin) map.put(input, new IndexWord(input,0));

    if(outputs.get(0).getScore()>=AnalysisOutput.SCORE_COMPOUNDS) {
      extractKeyword(outputs, map);      
    } else {
      try {
        List<AnalysisOutput> list = wsAnal.analyze(input);
        
        List<AnalysisOutput> results = new ArrayList<AnalysisOutput>();      
        if(list.size()>1) {
          for(AnalysisOutput o : list) {
            if(hasOrigin) map.put(o.getSource(), new IndexWord(o.getSource(),0));        
            results.addAll(morph.analyze(o.getSource()));
          }        
        } else {
          results.addAll(list);
        }
  
        extractKeyword(results, map);
      } catch(Exception e) {
        extractKeyword(outputs, map);
      }
    }
    
    Iterator<String> iter = map.keySet().iterator();
    
    while(iter.hasNext()) {      
      String text = iter.next();    
      if(text.length()<=1) continue;  
      morphQueue.add(map.get(text));
    }
  
  }
  
  private void extractKeyword(List<AnalysisOutput> outputs, Map<String,IndexWord> map) throws MorphException {

    for(AnalysisOutput output : outputs) {  

      if(output.getPos()!=PatternConstants.POS_VERB) {
        if(originCNoun || (!originCNoun&&output.getCNounList().size()==0)) {
          map.put(output.getStem(), new IndexWord(output.getStem(),0));  
        }
//      }else {
//        map.put(output.getStem()+"다", new Integer(1));  
      }    

      if(exactMatch) continue;

      if(output.getScore()>=AnalysisOutput.SCORE_COMPOUNDS) {
        
        List<CompoundEntry> cnouns = output.getCNounList();
        int start = 0;

        for(int jj=0;jj<cnouns.size();jj++) {
          
          CompoundEntry cnoun = cnouns.get(jj);

          if(cnoun.getWord().length()>1) map.put(cnoun.getWord(),  new IndexWord(cnoun.getWord(), start));
          
          if(jj==0 && cnoun.getWord().length()==1) {
            map.put(cnoun.getWord()+cnouns.get(jj+1).getWord(),  new IndexWord(cnoun.getWord(), start));
          } else if(jj>1 && cnoun.getWord().length()==1) {
            String iw = cnouns.get(jj-1).getWord()+cnoun.getWord();
            map.put(iw,  new IndexWord(iw, start-cnouns.get(jj-1).getWord().length()));
          }
          
          start += cnoun.getWord().length();
        }
        
      } else if(bigrammable) {
        addBiagramToMap(output.getStem(),map);
      }
    }
  }
  
  private void addBiagramToMap(String input, Map<String,IndexWord> map) {
    
    int offset = 0;
    int strlen = input.length();
    
    while(offset<strlen-1) {
      
      if(isAlphaNumChar(input.charAt(offset))) {
        String text = findAlphaNumeric(input.substring(offset));
        map.put(text,  new IndexWord(text, offset));
        offset += text.length();
      } else {
        String text = input.substring(offset,
            offset+2>strlen?strlen:offset+2);
        map.put(text,  new IndexWord(text, offset));
        offset++;
      }
    }
  }
  
  private String findAlphaNumeric(String text) {
    int pos = 0;
    for(int i=0;i<text.length();i++) {
      if(!isAlphaNumChar(text.charAt(i))) break;
      pos++;
    }        
    return text.substring(0,pos);
  }
  
  /**
   * 한자는 2개이상의 한글 음으로 읽혀질 수 있다.
   * 두음법칙이 아님.
   * @param term
   * @throws MorphException
   */
  private void analysisChinese(String term) throws MorphException {  
    
    morphQueue.add(new IndexWord(term,0));
    if(term.length()<2) return; // 1글자 한자는 색인어로 한글을 추출하지 않는다.
    
    List<StringBuffer> candiList = new ArrayList<StringBuffer>();
    candiList.add(new StringBuffer());
    
    for(int i=0;i<term.length();i++) {

      char[] chs = HanjaUtils.convertToHangul(term.charAt(i));      
      if(chs==null) continue;
    
      List<StringBuffer> removeList = new ArrayList<StringBuffer>(); // 제거될 후보를 저장  
      
      int caniSize = candiList.size();
      
      for(int j=0;j<caniSize;j++) { 
        String origin = candiList.get(j).toString();

        for(int k=0;k<chs.length;k++) { // 추가로 생성된 음에 대해서 새로운 텍스트를 생성한다.
          
          if(k==4) break; // 4개 이상의 음을 가지고 있는 경우 첫번째 음으로만 처리를 한다.
          
          StringBuffer sb = candiList.get(j);
          if(k>0) sb = new StringBuffer(origin);
          
          sb.append(chs[k]);          
          if(k>0)  candiList.add(sb);
          
          Iterator<String[]> iter = DictionaryUtil.findWithPrefix(sb.toString());
          if(!iter.hasNext()) // 사전에 없으면 삭제 후보
            removeList.add(sb);    
        }        
      }            

      if(removeList.size()==candiList.size()) { // 사전에서 찾은 단어가 하나도 없다면.. 
        candiList = candiList.subList(0, 1); // 첫번째만 생성하고 나머지는 버림
      } 
      
      for(StringBuffer rsb : removeList) {
        if(candiList.size()>1) candiList.remove(rsb);
      }
    }

    int maxCandidate = 5;
    if(candiList.size()<maxCandidate) maxCandidate=candiList.size();
    
    for(int i=0;i<maxCandidate;i++) {
      morphQueue.add(new IndexWord(candiList.get(i).toString(),0));
    }
    
    Map<String, String> cnounMap = new HashMap<String, String>();
    
    // 추출된 명사가 복합명사인 경우 분리한다.
    for(int i=0;i<maxCandidate;i++) {
      List<CompoundEntry> results = confirmCNoun(candiList.get(i).toString());
      
      int pos = 0;
      int offset = 0;
      for(CompoundEntry entry : results) {        
        pos += entry.getWord().length();  
        if(cnounMap.get(entry.getWord())!=null) continue;
         
        // 한글과 매치되는 한자를 짤라서 큐에 저장한다.           
        morphQueue.add(new IndexWord(term.substring(offset,pos),offset));
         
        cnounMap.put(entry.getWord(), entry.getWord());
         
        if(entry.getWord().length()<2) continue; //  한글은 2글자 이상만 저장한다.
         
        // 분리된 한글을 큐에 저장한다.  
        morphQueue.add(new IndexWord(entry.getWord(),offset));
         
        offset = pos;
      }       
    }    
  }
  
  private List<CompoundEntry> confirmCNoun(String input) throws MorphException {
    
    WordEntry cnoun = DictionaryUtil.getCNoun(input);
    if(cnoun!=null && cnoun.getFeature(WordEntry.IDX_NOUN)=='2') {
      return cnoun.getCompounds();
    }
       
    return cnAnalyzer.analyze(input);
  }
  
  private void analysisETC(String term) throws MorphException {

    final char[] buffer = termAtt.buffer();
    final int bufferLength = termAtt.length();
    final String type = typeAtt.type();

    if (type == APOSTROPHE_TYPE &&      // remove 's
        bufferLength >= 2 &&
        buffer[bufferLength-2] == '\'' &&
        (buffer[bufferLength-1] == 's' || buffer[bufferLength-1] == 'S')) {
      // Strip last 2 characters off
      morphQueue.add(new IndexWord(term.substring(0,bufferLength - 2),0));
    } else if (type == ACRONYM_TYPE) {      // remove dots
      int upto = 0;
      for(int i=0;i<bufferLength;i++) {
        char c = buffer[i];
        if (c != '.')
          buffer[upto++] = c;
      }
      morphQueue.add(new IndexWord(term.substring(0,upto),0));
    } else {
      morphQueue.add(new IndexWord(term,0));
    }
  }
  
  private boolean isAlphaNumChar(int c) {
    if((c>=48&&c<=57)||(c>=65&&c<=122)) return true;    
    return false;
  }
  
  public void setHasOrigin(boolean has) {
    hasOrigin = has;
  }

  public void setExactMatch(boolean match) {
    this.exactMatch = match;
  }
}
