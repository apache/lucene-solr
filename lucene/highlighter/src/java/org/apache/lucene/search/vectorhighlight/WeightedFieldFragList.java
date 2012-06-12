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
import java.util.HashSet;
import java.util.List;

import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo;
import org.apache.lucene.search.vectorhighlight.FieldTermStack.TermInfo;

/**
 * A weighted implementation of {@link FieldFragList}.
 */
public class WeightedFieldFragList extends FieldFragList {

  /**
   * a constructor.
   * 
   * @param fragCharSize the length (number of chars) of a fragment
   */
  public WeightedFieldFragList( int fragCharSize ) {
    super( fragCharSize );
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.vectorhighlight.FieldFragList#add( int startOffset, int endOffset, List<WeightedPhraseInfo> phraseInfoList )
   */ 
  @Override
  public void add( int startOffset, int endOffset, List<WeightedPhraseInfo> phraseInfoList ) {
    
    float totalBoost = 0;
    
    List<SubInfo> subInfos = new ArrayList<SubInfo>();
    
    HashSet<String> distinctTerms = new HashSet<String>();
    
    int length = 0;

    for( WeightedPhraseInfo phraseInfo : phraseInfoList ){
      
      subInfos.add( new SubInfo( phraseInfo.getText(), phraseInfo.getTermsOffsets(), phraseInfo.getSeqnum() ) );
      
      for ( TermInfo ti :  phraseInfo.getTermsInfos()) {
        if ( distinctTerms.add( ti.getText() ) )
          totalBoost += ti.getWeight() * phraseInfo.getBoost();
        length++;
      }
    }
    
    // We want that terms per fragment (length) is included into the weight. Otherwise a one-word-query
    // would cause an equal weight for all fragments regardless of how much words they contain.  
    // To avoid that fragments containing a high number of words possibly "outrank" more relevant fragments
    // we "bend" the length with a standard-normalization a little bit.  
    totalBoost *= length * ( 1 / Math.sqrt( length ) );
    
    getFragInfos().add( new WeightedFragInfo( startOffset, endOffset, subInfos, totalBoost ) );
  }
  
}