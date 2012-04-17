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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;

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
  //  FieldType ft = new FieldType(TextField.TYPE_STORED);
  //  ft.setStoreTermVectors(true);
  //  ft.setStoreTermVectorOffsets(true);
  //  ft.setStoreTermVectorPositions(true);
  //  doc.add( new Field( "f", ft, "a a a b b c a b b c d e f" ) );
  //  doc.add( new Field( "f", ft, "b a b a f" ) );
  //  writer.addDocument( doc );
  //  writer.close();
    
  //  IndexReader reader = IndexReader.open(dir1);
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
    
    Set<String> termSet = fieldQuery.getTermSet( fieldName );
    // just return to make null snippet if un-matched fieldName specified when fieldMatch == true
    if( termSet == null ) return;

    final Fields vectors = reader.getTermVectors(docId);
    if (vectors == null) {
      // null snippet
      return;
    }

    final Terms vector = vectors.terms(fieldName);
    if (vector == null) {
      // null snippet
      return;
    }

    final CharsRef spare = new CharsRef();
    final TermsEnum termsEnum = vector.iterator(null);
    DocsAndPositionsEnum dpEnum = null;
    BytesRef text;
    while ((text = termsEnum.next()) != null) {
      UnicodeUtil.UTF8toUTF16(text, spare);
      final String term = spare.toString();
      if (!termSet.contains(term)) {
        continue;
      }
      dpEnum = termsEnum.docsAndPositions(null, dpEnum, true);
      if (dpEnum == null) {
        // null snippet
        return;
      }

      dpEnum.nextDoc();

      final int freq = dpEnum.freq();
      
      for(int i = 0;i < freq;i++) {
        int pos = dpEnum.nextPosition();
        termList.add(new TermInfo(term, dpEnum.startOffset(), dpEnum.endOffset(), pos));
      }
    }
    
    // sort by position
    Collections.sort(termList);
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
    termList.push( termInfo );
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
