package org.apache.lucene.search.vectorhighlight;
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
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.search.vectorhighlight.FieldQuery.QueryPhraseMap;
import org.apache.lucene.search.vectorhighlight.FieldTermStack.TermInfo;

/**
 * FieldPhraseList has a list of WeightedPhraseInfo that is used by FragListBuilder
 * to create a FieldFragList object.
 */
public class FieldPhraseList {

  LinkedList<WeightedPhraseInfo> phraseList = new LinkedList<WeightedPhraseInfo>();
  
  /**
   * create a FieldPhraseList that has no limit on the number of phrases to analyze
   * 
   * @param fieldTermStack FieldTermStack object
   * @param fieldQuery FieldQuery object
   */
  public FieldPhraseList( FieldTermStack fieldTermStack, FieldQuery fieldQuery){
      this (fieldTermStack, fieldQuery, Integer.MAX_VALUE);
  }
  
  /**
   * return the list of WeightedPhraseInfo.
   * 
   * @return phraseList.
   */ 
  public List<WeightedPhraseInfo> getPhraseList() {
    return phraseList; 
  }
  
  /**
   * a constructor.
   * 
   * @param fieldTermStack FieldTermStack object
   * @param fieldQuery FieldQuery object
   * @param phraseLimit maximum size of phraseList
   */
  public FieldPhraseList( FieldTermStack fieldTermStack, FieldQuery fieldQuery, int phraseLimit ){
    final String field = fieldTermStack.getFieldName();

    LinkedList<TermInfo> phraseCandidate = new LinkedList<TermInfo>();
    QueryPhraseMap currMap = null;
    QueryPhraseMap nextMap = null;
    while( !fieldTermStack.isEmpty() && (phraseList.size() < phraseLimit) )
    {      
      phraseCandidate.clear();

      TermInfo ti = fieldTermStack.pop();
      currMap = fieldQuery.getFieldTermMap( field, ti.getText() );

      // if not found, discard top TermInfo from stack, then try next element
      if( currMap == null ) continue;
      
      // if found, search the longest phrase
      phraseCandidate.add( ti );
      while( true ){
        ti = fieldTermStack.pop();
        nextMap = null;
        if( ti != null )
          nextMap = currMap.getTermMap( ti.getText() );
        if( ti == null || nextMap == null ){
          if( ti != null ) 
            fieldTermStack.push( ti );
          if( currMap.isValidTermOrPhrase( phraseCandidate ) ){
            addIfNoOverlap( new WeightedPhraseInfo( phraseCandidate, currMap.getBoost(), currMap.getTermOrPhraseNumber() ) );
          }
          else{
            while( phraseCandidate.size() > 1 ){
              fieldTermStack.push( phraseCandidate.removeLast() );
              currMap = fieldQuery.searchPhrase( field, phraseCandidate );
              if( currMap != null ){
                addIfNoOverlap( new WeightedPhraseInfo( phraseCandidate, currMap.getBoost(), currMap.getTermOrPhraseNumber() ) );
                break;
              }
            }
          }
          break;
        }
        else{
          phraseCandidate.add( ti );
          currMap = nextMap;
        }
      }
    }
  }

  public void addIfNoOverlap( WeightedPhraseInfo wpi ){
    for( WeightedPhraseInfo existWpi : getPhraseList() ){
      if( existWpi.isOffsetOverlap( wpi ) ) {
        // WeightedPhraseInfo.addIfNoOverlap() dumps the second part of, for example, hyphenated words (social-economics). 
        // The result is that all informations in TermInfo are lost and not available for further operations. 
        existWpi.getTermsInfos().addAll( wpi.getTermsInfos() );
        return;
      }
    }
    getPhraseList().add( wpi );
  }
  
  /**
   * Represents the list of term offsets and boost for some text
   */
  public static class WeightedPhraseInfo {

    private String text;  // unnecessary member, just exists for debugging purpose
    private List<Toffs> termsOffsets;   // usually termsOffsets.size() == 1,
                            // but if position-gap > 1 and slop > 0 then size() could be greater than 1
    private float boost;  // query boost
    private int seqnum;
    
    private ArrayList<TermInfo> termsInfos;
    
    /**
     * @return the text
     */
    public String getText() {
      return text;
    }

    /**
     * @return the termsOffsets
     */
    public List<Toffs> getTermsOffsets() {
      return termsOffsets;
    }

    /**
     * @return the boost
     */
    public float getBoost() {
      return boost;
    }

    /**
     * @return the termInfos 
     */    
    public List<TermInfo> getTermsInfos() {
      return termsInfos;
    }

    public WeightedPhraseInfo( LinkedList<TermInfo> terms, float boost ){
      this( terms, boost, 0 );
    }
    
    public WeightedPhraseInfo( LinkedList<TermInfo> terms, float boost, int seqnum ){
      this.boost = boost;
      this.seqnum = seqnum;
      
      // We keep TermInfos for further operations
      termsInfos = new ArrayList<TermInfo>( terms );
      
      termsOffsets = new ArrayList<Toffs>( terms.size() );
      TermInfo ti = terms.get( 0 );
      termsOffsets.add( new Toffs( ti.getStartOffset(), ti.getEndOffset() ) );
      if( terms.size() == 1 ){
        text = ti.getText();
        return;
      }
      StringBuilder sb = new StringBuilder();
      sb.append( ti.getText() );
      int pos = ti.getPosition();
      for( int i = 1; i < terms.size(); i++ ){
        ti = terms.get( i );
        sb.append( ti.getText() );
        if( ti.getPosition() - pos == 1 ){
          Toffs to = termsOffsets.get( termsOffsets.size() - 1 );
          to.setEndOffset( ti.getEndOffset() );
        }
        else{
          termsOffsets.add( new Toffs( ti.getStartOffset(), ti.getEndOffset() ) );
        }
        pos = ti.getPosition();
      }
      text = sb.toString();
    }
    
    public int getStartOffset(){
      return termsOffsets.get( 0 ).startOffset;
    }
    
    public int getEndOffset(){
      return termsOffsets.get( termsOffsets.size() - 1 ).endOffset;
    }
    
    public boolean isOffsetOverlap( WeightedPhraseInfo other ){
      int so = getStartOffset();
      int eo = getEndOffset();
      int oso = other.getStartOffset();
      int oeo = other.getEndOffset();
      if( so <= oso && oso < eo ) return true;
      if( so < oeo && oeo <= eo ) return true;
      if( oso <= so && so < oeo ) return true;
      if( oso < eo && eo <= oeo ) return true;
      return false;
    }
    
    @Override
    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append( text ).append( '(' ).append( boost ).append( ")(" );
      for( Toffs to : termsOffsets ){
        sb.append( to );
      }
      sb.append( ')' );
      return sb.toString();
    }
    
    /**
     * @return the seqnum
     */
    public int getSeqnum() {
      return seqnum;
    }

    /**
     * Term offsets (start + end)
     */
    public static class Toffs {
      private int startOffset;
      private int endOffset;
      public Toffs( int startOffset, int endOffset ){
        this.startOffset = startOffset;
        this.endOffset = endOffset;
      }
      public void setEndOffset( int endOffset ){
        this.endOffset = endOffset;
      }
      public int getStartOffset(){
        return startOffset;
      }
      public int getEndOffset(){
        return endOffset;
      }
      @Override
      public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append( '(' ).append( startOffset ).append( ',' ).append( endOffset ).append( ')' );
        return sb.toString();
      }
    }
  }
}
