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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo.Toffs;

public abstract class BaseFragmentsBuilder implements FragmentsBuilder {

  protected String[] preTags, postTags;
  public static final String[] COLORED_PRE_TAGS = {
    "<b style=\"background:yellow\">", "<b style=\"background:lawngreen\">", "<b style=\"background:aquamarine\">",
    "<b style=\"background:magenta\">", "<b style=\"background:palegreen\">", "<b style=\"background:coral\">",
    "<b style=\"background:wheat\">", "<b style=\"background:khaki\">", "<b style=\"background:lime\">",
    "<b style=\"background:deepskyblue\">", "<b style=\"background:deeppink\">", "<b style=\"background:salmon\">",
    "<b style=\"background:peachpuff\">", "<b style=\"background:violet\">", "<b style=\"background:mediumpurple\">",
    "<b style=\"background:palegoldenrod\">", "<b style=\"background:darkkhaki\">", "<b style=\"background:springgreen\">",
    "<b style=\"background:turquoise\">", "<b style=\"background:powderblue\">"
  };
  public static final String[] COLORED_POST_TAGS = { "</b>" };
  private char multiValuedSeparator = ' ';
  
  protected BaseFragmentsBuilder(){
    this( new String[]{ "<b>" }, new String[]{ "</b>" } );
  }
  
  protected BaseFragmentsBuilder( String[] preTags, String[] postTags ){
    this.preTags = preTags;
    this.postTags = postTags;
  }
  
  static Object checkTagsArgument( Object tags ){
    if( tags instanceof String ) return tags;
    else if( tags instanceof String[] ) return tags;
    throw new IllegalArgumentException( "type of preTags/postTags must be a String or String[]" );
  }
  
  public abstract List<WeightedFragInfo> getWeightedFragInfoList( List<WeightedFragInfo> src );

  private static final Encoder NULL_ENCODER = new DefaultEncoder();
  
  public String createFragment( IndexReader reader, int docId,
      String fieldName, FieldFragList fieldFragList ) throws IOException {
    return createFragment( reader, docId, fieldName, fieldFragList,
        preTags, postTags, NULL_ENCODER );
  }

  public String[] createFragments( IndexReader reader, int docId,
      String fieldName, FieldFragList fieldFragList, int maxNumFragments )
      throws IOException {
    return createFragments( reader, docId, fieldName, fieldFragList, maxNumFragments,
        preTags, postTags, NULL_ENCODER );
  }
  
  public String createFragment( IndexReader reader, int docId,
      String fieldName, FieldFragList fieldFragList, String[] preTags, String[] postTags,
      Encoder encoder ) throws IOException {
    String[] fragments = createFragments( reader, docId, fieldName, fieldFragList, 1,
        preTags, postTags, encoder );
    if( fragments == null || fragments.length == 0 ) return null;
    return fragments[0];
  }

  public String[] createFragments( IndexReader reader, int docId,
      String fieldName, FieldFragList fieldFragList, int maxNumFragments,
      String[] preTags, String[] postTags, Encoder encoder ) throws IOException {
    if( maxNumFragments < 0 )
      throw new IllegalArgumentException( "maxNumFragments(" + maxNumFragments + ") must be positive number." );

    List<WeightedFragInfo> fragInfos = getWeightedFragInfoList( fieldFragList.getFragInfos() );
    
    List<String> fragments = new ArrayList<String>( maxNumFragments );
    Field[] values = getFields( reader, docId, fieldName );
    if( values.length == 0 ) return null;
    StringBuilder buffer = new StringBuilder();
    int[] nextValueIndex = { 0 };
    for( int n = 0; n < maxNumFragments && n < fragInfos.size(); n++ ){
      WeightedFragInfo fragInfo = fragInfos.get( n );
      fragments.add( makeFragment( buffer, nextValueIndex, values, fragInfo, preTags, postTags, encoder ) );
    }
    return fragments.toArray( new String[fragments.size()] );
  }
  
  protected Field[] getFields( IndexReader reader, int docId, String fieldName) throws IOException {
    // according to javadoc, doc.getFields(fieldName) cannot be used with lazy loaded field???
    Document doc = reader.document( docId, new MapFieldSelector(fieldName) );
    return doc.getFields( fieldName ); // according to Document class javadoc, this never returns null
  }

  protected String makeFragment( StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo fragInfo,
      String[] preTags, String[] postTags, Encoder encoder ){
    final int s = fragInfo.startOffset;
    return makeFragment( fragInfo, getFragmentSource( buffer, index, values, s, fragInfo.endOffset ), s,
        preTags, postTags, encoder );
  }
  
  private String makeFragment( WeightedFragInfo fragInfo, String src, int s,
      String[] preTags, String[] postTags, Encoder encoder ){
    StringBuilder fragment = new StringBuilder();
    int srcIndex = 0;
    for( SubInfo subInfo : fragInfo.subInfos ){
      for( Toffs to : subInfo.termsOffsets ){
        fragment
          .append( encoder.encodeText( src.substring( srcIndex, to.startOffset - s ) ) )
          .append( getPreTag( preTags, subInfo.seqnum ) )
          .append( encoder.encodeText( src.substring( to.startOffset - s, to.endOffset - s ) ) )
          .append( getPostTag( postTags, subInfo.seqnum ) );
        srcIndex = to.endOffset - s;
      }
    }
    fragment.append( encoder.encodeText( src.substring( srcIndex ) ) );
    return fragment.toString();
  }
  
  protected String getFragmentSource( StringBuilder buffer, int[] index, Field[] values,
      int startOffset, int endOffset ){
    while( buffer.length() < endOffset && index[0] < values.length ){
      buffer.append( values[index[0]].stringValue() );
      if( values[index[0]].isTokenized() )
        buffer.append( multiValuedSeparator );
      index[0]++;
    }
    int eo = buffer.length() < endOffset ? buffer.length() : endOffset;
    return buffer.substring( startOffset, eo );
  }
  
  public void setMultiValuedSeparator( char separator ){
    multiValuedSeparator = separator;
  }
  
  public char getMultiValuedSeparator(){
    return multiValuedSeparator;
  }

  protected String getPreTag( int num ){
    return getPreTag( preTags, num );
  }
  
  protected String getPostTag( int num ){
    return getPostTag( postTags, num );
  }
  
  protected String getPreTag( String[] preTags, int num ){
    int n = num % preTags.length;
    return preTags[n];
  }
  
  protected String getPostTag( String[] postTags, int num ){
    int n = num % postTags.length;
    return postTags[n];
  }
}
