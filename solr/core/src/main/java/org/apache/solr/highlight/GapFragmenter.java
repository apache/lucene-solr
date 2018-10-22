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
package org.apache.solr.highlight;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;

public class GapFragmenter extends HighlightingPluginBase implements SolrFragmenter
{
  @Override
  public Fragmenter getFragmenter(String fieldName, SolrParams params )
  {
    numRequests.inc();
    params = SolrParams.wrapDefaults(params, defaults);
    
    int fragsize = params.getFieldInt( fieldName, HighlightParams.FRAGSIZE, 100 );
    return (fragsize <= 0) ? new NullFragmenter() : new LuceneGapFragmenter(fragsize);
  }
  

  ///////////////////////////////////////////////////////////////////////
  //////////////////////// SolrInfoMBeans methods ///////////////////////
  ///////////////////////////////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "GapFragmenter";
  }
}


/**
 * A simple modification of SimpleFragmenter which additionally creates new
 * fragments when an unusually-large position increment is encountered
 * (this behaves much better in the presence of multi-valued fields).
 */
class LuceneGapFragmenter extends SimpleFragmenter {
  /** 
   * When a gap in term positions is observed that is at least this big, treat
   * the gap as a fragment delimiter.
   */
  public static final int INCREMENT_THRESHOLD = 50;
  protected int fragOffset = 0;
  
  private OffsetAttribute offsetAtt;
  private PositionIncrementAttribute posIncAtt;
  
  public LuceneGapFragmenter() {
  }
  
  public LuceneGapFragmenter(int fragsize) {
     super(fragsize);
  }
  
  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.TextFragmenter#start(java.lang.String)
   */
  @Override
  public void start(String originalText, TokenStream tokenStream) {
    offsetAtt = tokenStream.getAttribute(OffsetAttribute.class);
    posIncAtt = tokenStream.getAttribute(PositionIncrementAttribute.class);
    fragOffset = 0;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.TextFragmenter#isNewFragment(org.apache.lucene.analysis.Token)
   */
  @Override
  public boolean isNewFragment() {
    int endOffset = offsetAtt.endOffset();
    boolean isNewFrag = 
      endOffset >= fragOffset + getFragmentSize() ||
      posIncAtt.getPositionIncrement() > INCREMENT_THRESHOLD;
    if(isNewFrag) {
        fragOffset = endOffset;
    }
    return isNewFrag;
  }
}
