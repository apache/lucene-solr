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
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;

/**
 * <code>FieldTermStack</code> is a stack that keeps query terms in the specified field
 * of the document to be highlighted.
 */
public class FieldTermStack {
  
  private final String fieldName;
  LinkedList<TermInfo> termList = new LinkedList<>();
  
  //public static void main( String[] args ) throws Exception {
  //  Analyzer analyzer = new WhitespaceAnalyzer(Version.LATEST);
  //  QueryParser parser = new QueryParser(Version.LATEST,  "f", analyzer );
  //  Query query = parser.parse( "a x:b" );
  //  FieldQuery fieldQuery = new FieldQuery( query, true, false );
    
  //  Directory dir = new RAMDirectory();
  //  IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Version.LATEST, analyzer));
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
   * @throws IOException If there is a low-level I/O error
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
    if (vector == null || vector.hasPositions() == false) {
      // null snippet
      return;
    }

    final CharsRefBuilder spare = new CharsRefBuilder();
    final TermsEnum termsEnum = vector.iterator();
    PostingsEnum dpEnum = null;
    BytesRef text;
    
    int numDocs = reader.maxDoc();
    
    while ((text = termsEnum.next()) != null) {
      spare.copyUTF8Bytes(text);
      final String term = spare.toString();
      if (!termSet.contains(term)) {
        continue;
      }
      dpEnum = termsEnum.postings(dpEnum, PostingsEnum.POSITIONS);
      dpEnum.nextDoc();
      
      // For weight look here: http://lucene.apache.org/core/3_6_0/api/core/org/apache/lucene/search/DefaultSimilarity.html
      final float weight = ( float ) ( Math.log( numDocs / ( double ) ( reader.docFreq( new Term(fieldName, text) ) + 1 ) ) + 1.0 );

      final int freq = dpEnum.freq();
      
      for(int i = 0;i < freq;i++) {
        int pos = dpEnum.nextPosition();
        if (dpEnum.startOffset() < 0) {
          return; // no offsets, null snippet
        }
        termList.add( new TermInfo( term, dpEnum.startOffset(), dpEnum.endOffset(), pos, weight ) );
      }
    }
    
    // sort by position
    Collections.sort(termList);
    
    // now look for dups at the same position, linking them together
    int currentPos = -1;
    TermInfo previous = null;
    TermInfo first = null;
    Iterator<TermInfo> iterator = termList.iterator();
    while (iterator.hasNext()) {
      TermInfo current = iterator.next();
      if (current.position == currentPos) {
        assert previous != null;
        previous.setNext(current);
        previous = current;
        iterator.remove();
      } else {
        if (previous != null) {
          previous.setNext(first);
        }
        previous = first = current;
        currentPos = current.position;
      }
    }
    if (previous != null) {
      previous.setNext(first);
    }
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
  
  /**
   * Single term with its position/offsets in the document and IDF weight.
   * It is Comparable but considers only position.
   */
  public static class TermInfo implements Comparable<TermInfo>{

    private final String text;
    private final int startOffset;
    private final int endOffset;
    private final int position;    

    // IDF-weight of this term
    private final float weight;
    
    // pointer to other TermInfo's at the same position.
    // this is a circular list, so with no syns, just points to itself
    private TermInfo next;

    public TermInfo( String text, int startOffset, int endOffset, int position, float weight ){
      this.text = text;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.position = position;
      this.weight = weight;
      this.next = this;
    }
    
    void setNext(TermInfo next) { this.next = next; }
    /** 
     * Returns the next TermInfo at this same position.
     * This is a circular list!
     */
    public TermInfo getNext() { return next; }
    public String getText(){ return text; }
    public int getStartOffset(){ return startOffset; }
    public int getEndOffset(){ return endOffset; }
    public int getPosition(){ return position; }
    public float getWeight(){ return weight; }
    
    @Override
    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append( text ).append( '(' ).append(startOffset).append( ',' ).append( endOffset ).append( ',' ).append( position ).append( ')' );
      return sb.toString();
    }

    @Override
    public int compareTo( TermInfo o ){
      return ( this.position - o.position );
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + position;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TermInfo other = (TermInfo) obj;
      if (position != other.position) {
        return false;
      }
      return true;
    }
  }
}
