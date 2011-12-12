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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * {@link org.apache.lucene.search.highlight.Fragmenter} that tries to produce snippets that "look" like a regular 
 * expression.
 *
 * <code>solrconfig.xml</code> parameters:
 * <ul>
 * <li><code>hl.regex.pattern</code>: regular expression corresponding to "nice" fragments.</li>
 * <li><code>hl.regex.slop</code>: how far the fragmenter can stray from the ideal fragment size.
       A slop of 0.2 means that the fragmenter can go over or under by 20%.</li>
 * <li><code>hl.regex.maxAnalyzedChars</code>: how many characters to apply the
       regular expression to (independent from the global highlighter setting).</li>
 * </ul>
 *
 * NOTE: the default for <code>maxAnalyzedChars</code> is much lower for this 
 * fragmenter.  After this limit is exhausted, fragments are produced in the
 * same way as <code>GapFragmenter</code>
 */

public class RegexFragmenter extends HighlightingPluginBase implements SolrFragmenter
{
  protected String defaultPatternRaw;
  protected Pattern defaultPattern;

  @Override
  public void init(NamedList args) {
    super.init(args);
    defaultPatternRaw = LuceneRegexFragmenter.DEFAULT_PATTERN_RAW;
    if( defaults != null ) {
      defaultPatternRaw = defaults.get(HighlightParams.PATTERN, LuceneRegexFragmenter.DEFAULT_PATTERN_RAW);      
    }
    defaultPattern = Pattern.compile(defaultPatternRaw);
  }

  public Fragmenter getFragmenter(String fieldName, SolrParams params )
  { 
    numRequests++;
    params = SolrParams.wrapDefaults(params, defaults);

    int fragsize  = params.getFieldInt(   fieldName, HighlightParams.FRAGSIZE,  LuceneRegexFragmenter.DEFAULT_FRAGMENT_SIZE );
    int increment = params.getFieldInt(   fieldName, HighlightParams.INCREMENT, LuceneRegexFragmenter.DEFAULT_INCREMENT_GAP );
    float slop    = params.getFieldFloat( fieldName, HighlightParams.SLOP,      LuceneRegexFragmenter.DEFAULT_SLOP );
    int maxchars  = params.getFieldInt(   fieldName, HighlightParams.MAX_RE_CHARS, LuceneRegexFragmenter.DEFAULT_MAX_ANALYZED_CHARS );
    String rawpat = params.getFieldParam( fieldName, HighlightParams.PATTERN,   LuceneRegexFragmenter.DEFAULT_PATTERN_RAW );

    Pattern p = rawpat == defaultPatternRaw ? defaultPattern : Pattern.compile(rawpat);

    if( fragsize <= 0 ) {
      return new NullFragmenter();
    }
    
    return new LuceneRegexFragmenter( fragsize, increment, slop, maxchars, p );
  }
  

  ///////////////////////////////////////////////////////////////////////
  //////////////////////// SolrInfoMBeans methods ///////////////////////
  ///////////////////////////////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "RegexFragmenter (" + defaultPatternRaw + ")";
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
 * Fragmenter that tries to produce snippets that "look" like a regular 
 * expression.
 *
 * NOTE: the default for <code>maxAnalyzedChars</code> is much lower for this 
 * fragmenter.  After this limit is exhausted, fragments are produced in the
 * same way as <code>GapFragmenter</code>
 */
class LuceneRegexFragmenter implements Fragmenter
{
  // ** defaults
  public static final int DEFAULT_FRAGMENT_SIZE = 70;
  public static final int DEFAULT_INCREMENT_GAP = 50;
  public static final float DEFAULT_SLOP = 0.6f;
  public static final int DEFAULT_MAX_ANALYZED_CHARS = 10000;

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
  // default desirable pattern for text fragments.
  protected Pattern textRE;
  

  // ** state
  protected int currentNumFrags;
  protected int currentOffset;
  protected int targetOffset;
  protected int[] hotspots;

  private PositionIncrementAttribute posIncAtt;
  private OffsetAttribute offsetAtt;

  // ** other
  // note: could dynamically change size of sentences extracted to match
  // target frag size
  public static final String 
    DEFAULT_PATTERN_RAW = "[-\\w ,\\n\"']{20,200}";
  public static final Pattern 
    DEFAULT_PATTERN = Pattern.compile(DEFAULT_PATTERN_RAW);


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
    this(targetFragChars, incrementGapThreshold, slop, maxAnalyzedChars,
         DEFAULT_PATTERN);
         
  }

  public LuceneRegexFragmenter(int targetFragChars, 
                               int incrementGapThreshold,
                               float slop,
                               int maxAnalyzedChars,
                               Pattern targetPattern) {
    this.targetFragChars = targetFragChars;
    this.incrementGapThreshold = incrementGapThreshold;    
    this.slop = slop;
    this.maxAnalyzedChars = maxAnalyzedChars;
    this.textRE = targetPattern;
  }
  

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.TextFragmenter#start(java.lang.String)
   */
  public void start(String originalText, TokenStream tokenStream) {
    currentNumFrags = 1;
    currentOffset = 0;
    addHotSpots(originalText);
    posIncAtt = tokenStream.getAttribute(PositionIncrementAttribute.class);
    offsetAtt = tokenStream.getAttribute(OffsetAttribute.class);
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
  public boolean isNewFragment()
  {
    boolean isNewFrag = false;
    int minFragLen = (int)((1.0f - slop)*targetFragChars);
    int endOffset = offsetAtt.endOffset();
    
    // ** determin isNewFrag
    if(posIncAtt.getPositionIncrement() > incrementGapThreshold) {
      // large position gaps always imply new fragments
      isNewFrag = true;

    } else if(endOffset - currentOffset < minFragLen) {
      // we're not in our range of flexibility
      isNewFrag = false;

    } else if(targetOffset > 0) {
      // we've already decided on a target
      isNewFrag = endOffset > targetOffset;

    } else {
      // we might be able to do something
      int minOffset = currentOffset + minFragLen;
      int maxOffset = (int)(currentOffset + (1.0f + slop)*targetFragChars);
      int hotIndex;

      // look for a close hotspot
      hotIndex = Arrays.binarySearch(hotspots, endOffset);
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

      isNewFrag = endOffset > targetOffset;
    }      
      
    // ** operate on isNewFrag
    if(isNewFrag) {
        currentNumFrags++;
        currentOffset = endOffset;
        targetOffset = -1;
    }
    return isNewFrag;
  }
  
}
