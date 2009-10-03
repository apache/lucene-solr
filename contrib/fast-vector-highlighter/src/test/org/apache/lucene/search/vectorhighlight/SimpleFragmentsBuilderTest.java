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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Query;

public class SimpleFragmentsBuilderTest extends AbstractTestCase {
  
  public void test1TermIndex() throws Exception {
    FieldFragList ffl = ffl( "a", "a" );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    assertEquals( "<b>a</b>", sfb.createFragment( reader, 0, F, ffl ) );

    // change tags
    sfb = new SimpleFragmentsBuilder( new String[]{ "[" }, new String[]{ "]" } );
    assertEquals( "[a]", sfb.createFragment( reader, 0, F, ffl ) );
  }
  
  public void test2Frags() throws Exception {
    FieldFragList ffl = ffl( "a", "a b b b b b b b b b b b a b a b" );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    String[] f = sfb.createFragments( reader, 0, F, ffl, 3 );
    // 3 snippets requested, but should be 2
    assertEquals( 2, f.length );
    assertEquals( "<b>a</b> b b b b b b b b b ", f[0] );
    assertEquals( "b b <b>a</b> b <b>a</b> b", f[1] );
  }
  
  public void test3Frags() throws Exception {
    FieldFragList ffl = ffl( "a c", "a b b b b b b b b b b b a b a b b b b b c a a b b" );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    String[] f = sfb.createFragments( reader, 0, F, ffl, 3 );
    assertEquals( 3, f.length );
    assertEquals( "<b>a</b> b b b b b b b b b ", f[0] );
    assertEquals( "b b <b>a</b> b <b>a</b> b b b b b ", f[1] );
    assertEquals( "<b>c</b> <b>a</b> <b>a</b> b b", f[2] );
  }

  private FieldFragList ffl( String queryValue, String indexValue ) throws Exception {
    make1d1fIndex( indexValue );
    Query query = paW.parse( queryValue );
    FieldQuery fq = new FieldQuery( query, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    return new SimpleFragListBuilder().createFieldFragList( fpl, 20 );
  }
  
  public void test1PhraseShortMV() throws Exception {
    makeIndexShortMV();

    FieldQuery fq = new FieldQuery( tq( "d" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    assertEquals( "a b c <b>d</b> e", sfb.createFragment( reader, 0, F, ffl ) );
  }
  
  public void test1PhraseLongMV() throws Exception {
    makeIndexLongMV();

    FieldQuery fq = new FieldQuery( pqF( "search", "engines" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    assertEquals( " most <b>search engines</b> use only one of these methods. Even the <b>search engines</b> that says they can use t",
        sfb.createFragment( reader, 0, F, ffl ) );
  }

  public void test1PhraseLongMVB() throws Exception {
    makeIndexLongMVB();

    FieldQuery fq = new FieldQuery( pqF( "sp", "pe", "ee", "ed" ), true, true ); // "speed" -(2gram)-> "sp","pe","ee","ed"
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    assertEquals( "ssing <b>speed</b>, the", sfb.createFragment( reader, 0, F, ffl ) );
  }
  
  public void testUnstoredField() throws Exception {
    makeUnstoredIndex();

    FieldQuery fq = new FieldQuery( tq( "aaa" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    assertNull( sfb.createFragment( reader, 0, F, ffl ) );
  }
  
  protected void makeUnstoredIndex() throws Exception {
    IndexWriter writer = new IndexWriter( dir, analyzerW, true, MaxFieldLength.LIMITED );
    Document doc = new Document();
    doc.add( new Field( F, "aaa", Store.NO, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS ) );
    writer.addDocument( doc );
    writer.close();

    reader = IndexReader.open( dir, true );
  }
}
