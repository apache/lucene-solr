package org.apache.lucene.search.vectorhighlight;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo.Toffs;

/**
 * FieldFragList has a list of "frag info" that is used by FragmentsBuilder class
 * to create fragments (snippets).
 */
public class FieldFragList {

  private List<WeightedFragInfo> fragInfos = new ArrayList<WeightedFragInfo>();

  /**
   * a constructor.
   * 
   * @param fragCharSize the length (number of chars) of a fragment
   */
  public FieldFragList( int fragCharSize ){
  }

  /**
   * convert the list of WeightedPhraseInfo to WeightedFragInfo, then add it to the fragInfos
   * 
   * @param startOffset start offset of the fragment
   * @param endOffset end offset of the fragment
   * @param phraseInfoList list of WeightedPhraseInfo objects
   */
  public void add( int startOffset, int endOffset, List<WeightedPhraseInfo> phraseInfoList ){
    fragInfos.add( new WeightedFragInfo( startOffset, endOffset, phraseInfoList ) );
  }
  
  /**
   * return the list of WeightedFragInfos.
   * 
   * @return fragInfos.
   */ 
  public List<WeightedFragInfo> getFragInfos() {
    return fragInfos;
  }

  public static class WeightedFragInfo {

    List<SubInfo> subInfos;
    float totalBoost;
    int startOffset;
    int endOffset;

    public WeightedFragInfo( int startOffset, int endOffset, List<WeightedPhraseInfo> phraseInfoList ){
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      subInfos = new ArrayList<SubInfo>();
      for( WeightedPhraseInfo phraseInfo : phraseInfoList ){
        SubInfo subInfo = new SubInfo( phraseInfo.text, phraseInfo.termsOffsets, phraseInfo.seqnum );
        subInfos.add( subInfo );
        totalBoost += phraseInfo.boost;
      }
    }
    
    public List<SubInfo> getSubInfos(){
      return subInfos;
    }
    
    public float getTotalBoost(){
      return totalBoost;
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
      sb.append( "subInfos=(" );
      for( SubInfo si : subInfos )
        sb.append( si.toString() );
      sb.append( ")/" ).append( totalBoost ).append( '(' ).append( startOffset ).append( ',' ).append( endOffset ).append( ')' );
      return sb.toString();
    }
    
    public static class SubInfo {
      final String text;  // unnecessary member, just exists for debugging purpose
      final List<Toffs> termsOffsets;   // usually termsOffsets.size() == 1,
                              // but if position-gap > 1 and slop > 0 then size() could be greater than 1
      int seqnum;

      SubInfo( String text, List<Toffs> termsOffsets, int seqnum ){
        this.text = text;
        this.termsOffsets = termsOffsets;
        this.seqnum = seqnum;
      }
      
      public List<Toffs> getTermsOffsets(){
        return termsOffsets;
      }
      
      public int getSeqnum(){
        return seqnum;
      }
      
      @Override
      public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append( text ).append( '(' );
        for( Toffs to : termsOffsets )
          sb.append( to.toString() );
        sb.append( ')' );
        return sb.toString();
      }
    }
  }
}
