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
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo;

/**
 * A simple implementation of {@link FragListBuilder}.
 */
public class SimpleFragListBuilder implements FragListBuilder {
  
  public static final int MARGIN_DEFAULT = 6;
  public static final int MIN_FRAG_CHAR_SIZE_FACTOR = 3;

  final int margin;
  final int minFragCharSize;

  public SimpleFragListBuilder( int margin ){
    if( margin < 0 )
      throw new IllegalArgumentException( "margin(" + margin + ") is too small. It must be 0 or higher." );

    this.margin = margin;
    this.minFragCharSize = Math.max( 1, margin * MIN_FRAG_CHAR_SIZE_FACTOR );
  }

  public SimpleFragListBuilder(){
    this( MARGIN_DEFAULT );
  }

  public FieldFragList createFieldFragList(FieldPhraseList fieldPhraseList, int fragCharSize) {
    if( fragCharSize < minFragCharSize )
      throw new IllegalArgumentException( "fragCharSize(" + fragCharSize + ") is too small. It must be " +
          minFragCharSize + " or higher." );

    FieldFragList ffl = new FieldFragList( fragCharSize );

    List<WeightedPhraseInfo> wpil = new ArrayList<WeightedPhraseInfo>();
    Iterator<WeightedPhraseInfo> ite = fieldPhraseList.phraseList.iterator();
    WeightedPhraseInfo phraseInfo = null;
    int startOffset = 0;
    boolean taken = false;
    while( true ){
      if( !taken ){
        if( !ite.hasNext() ) break;
        phraseInfo = ite.next();
      }
      taken = false;
      if( phraseInfo == null ) break;

      // if the phrase violates the border of previous fragment, discard it and try next phrase
      if( phraseInfo.getStartOffset() < startOffset ) continue;

      wpil.clear();
      wpil.add( phraseInfo );
      int st = phraseInfo.getStartOffset() - margin < startOffset ?
          startOffset : phraseInfo.getStartOffset() - margin;
      int en = st + fragCharSize;
      if( phraseInfo.getEndOffset() > en )
        en = phraseInfo.getEndOffset();
      startOffset = en;

      while( true ){
        if( ite.hasNext() ){
          phraseInfo = ite.next();
          taken = true;
          if( phraseInfo == null ) break;
        }
        else
          break;
        if( phraseInfo.getEndOffset() <= en )
          wpil.add( phraseInfo );
        else
          break;
      }
      ffl.add( st, en, wpil );
    }
    return ffl;
  }

}
