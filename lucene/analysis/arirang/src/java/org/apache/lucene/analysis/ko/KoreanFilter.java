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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ko.dic.CompoundEntry;
import org.apache.lucene.analysis.ko.dic.DictionaryUtil;
import org.apache.lucene.analysis.ko.dic.HanjaMapper;
import org.apache.lucene.analysis.ko.morph.AnalysisOutput;
import org.apache.lucene.analysis.ko.morph.CompoundNounAnalyzer;
import org.apache.lucene.analysis.ko.morph.MorphAnalyzer;
import org.apache.lucene.analysis.ko.morph.PatternConstants;
import org.apache.lucene.analysis.ko.morph.WordSpaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public final class KoreanFilter extends TokenFilter {

  private final LinkedList<Token> morphQueue = new LinkedList<Token>();
  private final MorphAnalyzer morph;
  private final WordSpaceAnalyzer wsAnal = new WordSpaceAnalyzer();
  private final CompoundNounAnalyzer cnAnalyzer;
  
  private State currentState = null;
  
  private final boolean bigrammable;
  private final boolean hasOrigin;
  private final boolean originCNoun;
    
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    
  private static final String KOREAN_TYPE = KoreanTokenizer.TOKEN_TYPES[KoreanTokenizer.KOREAN];
  private static final String CHINESE_TYPE = KoreanTokenizer.TOKEN_TYPES[KoreanTokenizer.CHINESE];
    
  public KoreanFilter(TokenStream input) {
    this(input, true);
  }

  /**
   * 
   * @param input  input token stream
   * @param bigram  Whether the bigram index term return or not.
   */
  public KoreanFilter(TokenStream input, boolean bigram) {
    this(input, bigram, false);
  }
  
  public KoreanFilter(TokenStream input, boolean bigram, boolean has) {
    this(input, bigram, has, false);
  }
  
  public KoreanFilter(TokenStream input, boolean bigram, boolean has, boolean exactMatch) {
    this(input, bigram, has, exactMatch, true);
  }

  public KoreanFilter(TokenStream input, boolean bigram, boolean has, boolean exactMatch, boolean cnoun) {
    super(input);
    this.bigrammable = bigram;
    this.hasOrigin = has;
    this.originCNoun = cnoun;
    this.cnAnalyzer = new CompoundNounAnalyzer(exactMatch);
    this.morph = new MorphAnalyzer(exactMatch);
  }
  
  public boolean incrementToken() throws IOException {
    if (!morphQueue.isEmpty()) {
      restoreState(currentState);
      setAttributesFromQueue(false);
      return true;
    }

    while (input.incrementToken()) {
      final String type = typeAtt.type();
      if (KOREAN_TYPE.equals(type)) {
        analysisKorean(termAtt.toString());
      } else if (CHINESE_TYPE.equals(type)) {
        analysisChinese(termAtt.toString());
      } else {
        return true; // pass anything else thru
      }        
  
      if (!morphQueue.isEmpty()) {
        setAttributesFromQueue(true);
        return true;
      }
    }

    return false;
  }
  

  private void setAttributesFromQueue(boolean isFirst) {
    final Token iw = morphQueue.removeFirst();
    if (isFirst && !morphQueue.isEmpty()) {
      // our queue has more elements remaining (e.g. we decompounded)
      // capture state for those. We set the term attribute to be empty
      // so we save lots of array copying later.
      termAtt.setEmpty();
      currentState = captureState();
    }
    
    termAtt.setEmpty().append(iw.word);
    offsetAtt.setOffset(iw.offset, iw.offset + iw.word.length());
    
    // on the first Token we preserve incoming increment:
    if (!isFirst) {
      posIncrAtt.setPositionIncrement(iw.increment);
    }
    
    // TODO: How to handle PositionLengthAttribute correctly?
  }
  
  /**
   * 한글을 분석한다.
   */
  private void analysisKorean(String input) {

    List<AnalysisOutput> outputs = morph.analyze(input);
    if(outputs.size()==0) return;
    
    Map<String,Token> map = new LinkedHashMap<String,Token>();
    if(hasOrigin) map.put("0:"+input, new Token(input,0));

    if(outputs.get(0).getScore()>=AnalysisOutput.SCORE_COMPOUNDS) {
      extractKeyword(outputs,offsetAtt.startOffset(), map, 0);      
    } else {
      try {
        List<AnalysisOutput> list = wsAnal.analyze(input);
        
        List<AnalysisOutput> results = new ArrayList<AnalysisOutput>();    
        if(list.size()>1 && wsAnal.getOutputScore(list)>AnalysisOutput.SCORE_ANALYSIS) {
          int offset = 0;
          for(AnalysisOutput o : list) {
            if(hasOrigin) map.put(o.getSource(), new Token(o.getSource(),offsetAtt.startOffset()+offset,1));        
            results.addAll(morph.analyze(o.getSource()));
            offset += o.getSource().length();
          }       
        } else {
          results.addAll(outputs);
        }
        extractKeyword(results, offsetAtt.startOffset(), map, 0);
      } catch (StringIndexOutOfBoundsException e) {
        // nocommit: Fix this stupidness with catch, instead fix in WSOutput
        extractKeyword(outputs.subList(0, 1), offsetAtt.startOffset(), map, 0);
      }
    }
        
    morphQueue.addAll(map.values());
  }
  
  private void extractKeyword(List<AnalysisOutput> outputs, int startoffset, Map<String,Token> map, int position) {

    int maxDecompounds = 0;
    int maxStem = 0;
    
    for(AnalysisOutput output : outputs) 
    {
      if(output.getPos()==PatternConstants.POS_VERB) continue; // extract keywords from only noun
      if(!originCNoun&&output.getCNounList().size()>0) continue; // except compound nound
      int inc = map.size()>0 ? 0 : 1;
      map.put(position+":"+output.getStem(), new Token(output.getStem(),startoffset,inc));
        
      if(output.getStem().length()>maxStem) maxStem = output.getStem().length();
      if(output.getCNounList().size()>maxDecompounds) maxDecompounds = output.getCNounList().size();
    }

    if(maxDecompounds>1) 
    {      
      for(int i=0; i<maxDecompounds; i++) 
      {
        position += i;
        
        int cPosition = position;
        for(AnalysisOutput output : outputs) 
        {
          if(output.getPos()==PatternConstants.POS_VERB ||
              output.getCNounList().size()<=i) continue;     
          
          CompoundEntry cEntry = output.getCNounList().get(i);
          int cStartoffset = getStartOffset(output, i) + startoffset;
          int inc = i==0 ? 0 : 1;
          map.put((cPosition)+":"+cEntry.getWord(), 
              new Token(cEntry.getWord(),cStartoffset,inc));
          
          if(bigrammable&&!cEntry.isExist()) 
            cPosition = addBiagramToMap(cEntry.getWord(), cStartoffset, map, cPosition);
        }                
      }      
    } 
    else 
    {
      for(AnalysisOutput output : outputs) 
      {
        if(output.getPos()==PatternConstants.POS_VERB) continue;
        
        if(bigrammable&&output.getScore()<AnalysisOutput.SCORE_COMPOUNDS) 
          addBiagramToMap(output.getStem(), startoffset, map, position);   
      }  
    }    
  }
  
  private int addBiagramToMap(String input, int startoffset, Map<String, Token> map, int position) {
    int offset = 0;
    int strlen = input.length();
    if(strlen<2) return position;
    
    while(offset<strlen-1) {
      
      int inc = offset==0 ? 0 : 1;
      
      if(isAlphaNumChar(input.charAt(offset))) {
        String text = findAlphaNumeric(input.substring(offset));
        map.put(position+":"+text,  new Token(text,startoffset+offset,inc));
        offset += text.length();
      } else {
        String text = input.substring(offset,
            offset+2>strlen?strlen:offset+2);
        map.put(position+":"+text,  new Token(text,startoffset+offset,inc));
        offset++;
      }
      
      position += 1;
    }
    
    return position-1;
  }
  
  /**
   * return the start offset of current decompounds entry.
   * @param output  morphlogical analysis output
   * @param index     the index of current decompounds entry
   * @return        the start offset of current decoumpounds entry
   */
  private int getStartOffset(AnalysisOutput output, int index) {    
    int sOffset = 0;
    for(int i=0; i<index;i++) {
      sOffset += output.getCNounList().get(i).getWord().length();
    }
    return sOffset;
  }
  
  private String findAlphaNumeric(String text) {
    int pos = 0;
    for(int i=0;i<text.length();i++) {
      if(!isAlphaNumChar(text.charAt(i))) break;
      pos++;
    }    
    if(pos<text.length()) pos += 1;
    
    return text.substring(0,pos);
  }
  
  /**
   * 한자는 2개이상의 한글 음으로 읽혀질 수 있다.
   * 두음법칙이 아님.
   * @param term  term
   */
  private void analysisChinese(String term) {  
    
    morphQueue.add(new Token(term,0));
    if(term.length()<2) return; // 1글자 한자는 색인어로 한글을 추출하지 않는다.
    
    List<StringBuilder> candiList = new ArrayList<StringBuilder>();
    candiList.add(new StringBuilder());
    
    for(int i=0;i<term.length();i++) {

      char[] chs = HanjaMapper.convertToHangul(term.charAt(i));      
    
      List<StringBuilder> removeList = new ArrayList<StringBuilder>(); // 제거될 후보를 저장  
      
      int caniSize = candiList.size();
      
      for(int j=0;j<caniSize;j++) { 
        String origin = candiList.get(j).toString();

        for(int k=0;k<chs.length;k++) { // 추가로 생성된 음에 대해서 새로운 텍스트를 생성한다.
          
          if(k==4) break; // 4개 이상의 음을 가지고 있는 경우 첫번째 음으로만 처리를 한다.
          
          StringBuilder sb = candiList.get(j);
          if(k>0) sb = new StringBuilder(origin);
          
          sb.append(chs[k]);          
          if(k>0)  candiList.add(sb);
          
          if (!DictionaryUtil.hasWordPrefix(sb)) {
            removeList.add(sb); // 사전에 없으면 삭제 후보
          }  
        }        
      }            

      if(removeList.size()==candiList.size()) { // 사전에서 찾은 단어가 하나도 없다면.. 
        candiList = candiList.subList(0, 1); // 첫번째만 생성하고 나머지는 버림
      } 
      
      for(StringBuilder rsb : removeList) {
        if(candiList.size()>1) candiList.remove(rsb);
      }
    }

    int maxCandidate = 5;
    if(candiList.size()<maxCandidate) maxCandidate=candiList.size();
    
    for(int i=0;i<maxCandidate;i++) {
      morphQueue.add(new Token(candiList.get(i).toString(),0));
    }
    
    Map<String, String> cnounMap = new HashMap<String, String>();
    
    // 추출된 명사가 복합명사인 경우 분리한다.
    for(int i=0;i<maxCandidate;i++) {
      CompoundEntry results[] = cnAnalyzer.analyze(candiList.get(i).toString());
      
      int pos = 0;
      int offset = 0;
      if (results != null) {
        for(CompoundEntry entry : results) {        
          pos += entry.getWord().length();  
          if(cnounMap.get(entry.getWord())!=null) continue;
         
                    // 한글과 매치되는 한자를 짤라서 큐에 저장한다.           
          // nocommit: this is avoiding AIOOBE, original code:
          // morphQueue.add(new IndexWord(term.substring(offset,pos),offset));
          morphQueue.add(new Token(term.substring(offset,Math.min(pos, term.length())),offset));
          cnounMap.put(entry.getWord(), entry.getWord());
         
          if(entry.getWord().length()<2) continue; //  한글은 2글자 이상만 저장한다.
         
                    // 분리된 한글을 큐에 저장한다.  
          morphQueue.add(new Token(entry.getWord(),offset));
         
          offset = pos;
        }
      }
    }    
  }
  
  private boolean isAlphaNumChar(int c) {
    if((c>=48&&c<=57)||(c>=65&&c<=122)) return true;    
    return false;
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    morphQueue.clear();
    currentState = null;
  }
  
  private static final class Token {

    // the word to be indexed
    final String word;
    
    // the offset from the start of input text
    final int offset;

    // when the input text is a chinese text, the korean sound text of it is extracted as a index word. 
    final int increment;
    
    Token(String word, int offset) {
      this(word, offset, 1);
    }
    
    Token(String word, int offset, int inc) {
      this.word = word;
      this.offset = offset;
      this.increment = inc;
    }
    
  }
}
