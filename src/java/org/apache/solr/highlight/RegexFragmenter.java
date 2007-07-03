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
package org.apache.solr.highlight;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;

public class RegexFragmenter extends HighlightingPluginBase implements SolrFragmenter
{
  public Fragmenter getFragmenter(String fieldName, SolrParams params )
  { 
    numRequests++;
    if( defaults != null ) {
      params = new DefaultSolrParams( params, defaults );
    }
    
    int fragsize  = params.getFieldInt(   fieldName, HighlightParams.FRAGSIZE,  LuceneRegexFragmenter.DEFAULT_FRAGMENT_SIZE );
    int increment = params.getFieldInt(   fieldName, HighlightParams.INCREMENT, LuceneRegexFragmenter.DEFAULT_INCREMENT_GAP );
    float slop    = params.getFieldFloat( fieldName, HighlightParams.SLOP,      LuceneRegexFragmenter.DEFAULT_SLOP );
    int maxchars  = params.getFieldInt(   fieldName, HighlightParams.MAX_CHARS, LuceneRegexFragmenter.DEFAULT_MAX_ANALYZED_CHARS );
    
    if( fragsize <= 0 ) {
      return new NullFragmenter();
    }
    
    return new LuceneRegexFragmenter( fragsize, increment, slop, maxchars );
  }
  

  ///////////////////////////////////////////////////////////////////////
  //////////////////////// SolrInfoMBeans methods ///////////////////////
  ///////////////////////////////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "GapFragmenter";
  }

  @Override
  public String getVersion() {
      return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}


/**
 * Kind of cool but kind of slow compared to regular fragmenting
 *
 * Interestingly, the slowdown comes almost entirely from the pre-analysis,
 * and could be completely avoided by pre-computation.
 *
 * it is also possible that a hand-crafted state machine (switch statement)
 * could be significantly faster.  Could even build in custom tricks...
 * perhaps JavaCC should be used? TODO
 * 
 * @author Mike Klaas
 */
class LuceneRegexFragmenter implements Fragmenter
{
  // ** defaults
  public static final int DEFAULT_FRAGMENT_SIZE = 70;
  public static final int DEFAULT_INCREMENT_GAP = 50;
  public static final float DEFAULT_SLOP = 0.6f;
  public static final int DEFAULT_MAX_ANALYZED_CHARS = 3000;

  // ** settings

  // desired length of fragments, in characters
  protected int targetFragChars;
  // increment gap which indicates a new fragment should occur 
  // (often due to multi-valued fields)
  protected int incrementGapThreshold;
  // factor by which we are allowed to bend the frag size (larger or smaller)
  protected float slop;
  // analysis limit (ensures we don't waste too much time on long fields)
  protected int maxAnalyzedChars;

  // ** state
  protected int currentNumFrags;
  protected int currentOffset;
  protected int targetOffset;
  protected int[] hotspots;

  // ** other
  // note: could dynamically change size of sentences extracted to match
  // target frag size
  protected static final Pattern textRE = Pattern.compile("[-\\w ,\"']{20,200}");

  // twice as fast, but not terribly good.
  //protected static final Pattern textRE = Pattern.compile("\\w{20,200}");

  public LuceneRegexFragmenter() {
    this(DEFAULT_FRAGMENT_SIZE, 
         DEFAULT_INCREMENT_GAP,
         DEFAULT_SLOP,
         DEFAULT_MAX_ANALYZED_CHARS);
  }
  public LuceneRegexFragmenter(int targetFragChars) {
    this(targetFragChars, 
         DEFAULT_INCREMENT_GAP,
         DEFAULT_SLOP,
         DEFAULT_MAX_ANALYZED_CHARS);
  }

  public LuceneRegexFragmenter(int targetFragChars, 
                         int incrementGapThreshold,
                         float slop,
                         int maxAnalyzedChars ) {
    this.targetFragChars = targetFragChars;
    this.incrementGapThreshold = incrementGapThreshold;    
    this.slop = slop;
    this.maxAnalyzedChars = maxAnalyzedChars;
  }
  

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.TextFragmenter#start(java.lang.String)
   */
  public void start(String originalText) {
    currentNumFrags = 1;
    currentOffset = 0;
    addHotSpots(originalText);
  }

  ////////////////////////////////////
  // pre-analysis
  ////////////////////////////////////

  protected void addHotSpots(String text) {
    //System.out.println("hot spotting");
    ArrayList<Integer> temphs = new ArrayList<Integer>(
                              text.length() / targetFragChars);
    Matcher match = textRE.matcher(text);
    int cur = 0;
    while(match.find() && cur < maxAnalyzedChars) {
      int start=match.start(), end=match.end();
      temphs.add(start);
      temphs.add(end);
      cur = end;
      //System.out.println("Matched " + match.group());
    }    
    //System.out.println("matches: " + temphs.size() + "\n\n");
    hotspots = new int[temphs.size()];
    for(int i = 0; i < temphs.size(); i++) {
      hotspots[i] = temphs.get(i);
    }
    // perhaps not necessary--I don't know if re matches are non-overlapping
    Arrays.sort(hotspots);
  }

  ////////////////////////////////////
  // fragmenting
  ////////////////////////////////////

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.TextFragmenter#isNewFragment(org.apache.lucene.analysis.Token)
   */
  public boolean isNewFragment(Token token)
  {
    boolean isNewFrag = false;
    int minFragLen = (int)((1.0f - slop)*targetFragChars);

    // ** determin isNewFrag
    if(token.getPositionIncrement() > incrementGapThreshold) {
      // large position gaps always imply new fragments
      isNewFrag = true;

    } else if(token.endOffset() - currentOffset < minFragLen) {
      // we're not in our range of flexibility
      isNewFrag = false;

    } else if(targetOffset > 0) {
      // we've already decided on a target
      isNewFrag = token.endOffset() > targetOffset;

    } else {
      // we might be able to do something
      int minOffset = currentOffset + minFragLen;
      int maxOffset = (int)(currentOffset + (1.0f + slop)*targetFragChars);
      int hotIndex;

      // look for a close hotspot
      hotIndex = Arrays.binarySearch(hotspots, token.endOffset());
      if(hotIndex < 0) hotIndex = -hotIndex;
      if(hotIndex >= hotspots.length) {
        // no more hotspots in this input stream
        targetOffset = currentOffset + targetFragChars;

      } else if(hotspots[hotIndex] > maxOffset) {
        // no hotspots within slop
        targetOffset = currentOffset + targetFragChars;

      } else {
        // try to find hotspot in slop
        int goal = hotspots[hotIndex];
        while(goal < minOffset && hotIndex < hotspots.length) {
          hotIndex++;
          goal = hotspots[hotIndex];
        }        
        targetOffset = goal <= maxOffset ? goal : currentOffset + targetFragChars;
      }

      isNewFrag = token.endOffset() > targetOffset;
    }      
      
    // ** operate on isNewFrag
    if(isNewFrag) {
        currentNumFrags++;
        currentOffset = token.endOffset();
        targetOffset = -1;
    }
    return isNewFrag;
  }
  
}
