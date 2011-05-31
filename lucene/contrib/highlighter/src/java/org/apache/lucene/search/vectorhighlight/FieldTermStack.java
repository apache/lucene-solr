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
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;

/**
 * <code>FieldTermStack</code> is a stack that keeps query terms in the specified field
 * of the document to be highlighted.
 */
public class FieldTermStack {
  
  private final String fieldName;
  LinkedList<TermInfo> termList = new LinkedList<TermInfo>();
  
  //public static void main( String[] args ) throws Exception {
  //  Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_CURRENT);
  //  QueryParser parser = new QueryParser(Version.LUCENE_CURRENT,  "f", analyzer );
  //  Query query = parser.parse( "a x:b" );
  //  FieldQuery fieldQuery = new FieldQuery( query, true, false );
    
  //  Directory dir = new RAMDirectory();
  //  IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer));
  //  Document doc = new Document();
  //  doc.add( new Field( "f", "a a a b b c a b b c d e f", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS ) );
  //  doc.add( new Field( "f", "b a b a f", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS ) );
  //  writer.addDocument( doc );
  //  writer.close();
    
  //  IndexReader reader = IndexReader.open( dir, true );
  //  new FieldTermStack( reader, 0, "f", fieldQuery );
  //  reader.close();
  //}

  /**
   * a constructor.
   * 
   * @param reader IndexReader of the index
   * @param docId document id to be highlighted
   * @param fieldName field of the document to be highlighted
   * @param fieldQuery FieldQuery object
   * @throws IOException
   */
  public FieldTermStack( IndexReader reader, int docId, String fieldName, final FieldQuery fieldQuery ) throws IOException {
    this.fieldName = fieldName;

    TermFreqVector tfv = reader.getTermFreqVector( docId, fieldName );
    if( tfv == null ) return; // just return to make null snippets
    TermPositionVector tpv = null;
    try{
      tpv = (TermPositionVector)tfv;
    }
    catch( ClassCastException e ){
      return; // just return to make null snippets
    }
    
    Set<String> termSet = fieldQuery.getTermSet( fieldName );
    // just return to make null snippet if un-matched fieldName specified when fieldMatch == true
    if( termSet == null ) return;
    final CharsRef spare = new CharsRef();
    for( BytesRef term : tpv.getTerms() ){
      if( !termSet.contains( term.utf8ToChars(spare).toString() ) ) continue;
      int index = tpv.indexOf( term );
      TermVectorOffsetInfo[] tvois = tpv.getOffsets( index );
      if( tvois == null ) return; // just return to make null snippets
      int[] poss = tpv.getTermPositions( index );
      if( poss == null ) return; // just return to make null snippets
      for( int i = 0; i < tvois.length; i++ )
        termList.add( new TermInfo( term.utf8ToChars(spare).toString(), tvois[i].getStartOffset(), tvois[i].getEndOffset(), poss[i] ) );
    }
    
    // sort by position
    Collections.sort( termList );
  }

  /**
   * @return field name
   */
  public String getFieldName(){
    return fieldName;
  }

  /**
   * @return the top TermInfo object of the stack
   */
  public TermInfo pop(){
    return termList.poll();
  }

  /**
   * @param termInfo the TermInfo object to be put on the top of the stack
   */
  public void push( TermInfo termInfo ){
    // termList.push( termInfo );  // avoid Java 1.6 feature
    termList.addFirst( termInfo );
  }

  /**
   * to know whether the stack is empty
   * 
   * @return true if the stack is empty, false if not
   */
  public boolean isEmpty(){
    return termList == null || termList.size() == 0;
  }
  
  public static class TermInfo implements Comparable<TermInfo>{

    final String text;
    final int startOffset;
    final int endOffset;
    final int position;

    TermInfo( String text, int startOffset, int endOffset, int position ){
      this.text = text;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.position = position;
    }
    
    public String getText(){ return text; }
    public int getStartOffset(){ return startOffset; }
    public int getEndOffset(){ return endOffset; }
    public int getPosition(){ return position; }
    
    @Override
    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append( text ).append( '(' ).append(startOffset).append( ',' ).append( endOffset ).append( ',' ).append( position ).append( ')' );
      return sb.toString();
    }

    public int compareTo( TermInfo o ) {
      return ( this.position - o.position );
    }
  }
}
