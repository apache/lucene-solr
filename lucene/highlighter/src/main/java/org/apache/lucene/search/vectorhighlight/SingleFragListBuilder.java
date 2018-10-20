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
package org.apache.lucene.search.vectorhighlight;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo;

/**
 * An implementation class of {@link FragListBuilder} that generates one {@link WeightedFragInfo} object.
 * Typical use case of this class is that you can get an entire field contents
 * by using both of this class and {@link SimpleFragmentsBuilder}.<br>
 * <pre class="prettyprint">
 * FastVectorHighlighter h = new FastVectorHighlighter( true, true,
 *   new SingleFragListBuilder(), new SimpleFragmentsBuilder() );
 * </pre>
 */
public class SingleFragListBuilder implements FragListBuilder {

  @Override
  public FieldFragList createFieldFragList(FieldPhraseList fieldPhraseList,
      int fragCharSize) {

    FieldFragList ffl = new SimpleFieldFragList( fragCharSize );

    List<WeightedPhraseInfo> wpil = new ArrayList<>();
    Iterator<WeightedPhraseInfo> ite = fieldPhraseList.phraseList.iterator();
    WeightedPhraseInfo phraseInfo = null;
    while( true ){
      if( !ite.hasNext() ) break;
      phraseInfo = ite.next();
      if( phraseInfo == null ) break;

      wpil.add( phraseInfo );
    }
    if( wpil.size() > 0 )
      ffl.add( 0, Integer.MAX_VALUE, wpil );
    return ffl;
  }

}
