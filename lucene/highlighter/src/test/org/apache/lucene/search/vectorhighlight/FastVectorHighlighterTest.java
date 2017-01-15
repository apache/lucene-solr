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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;


public class FastVectorHighlighterTest extends LuceneTestCase {
  
  
  public void testSimpleHighlightTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Field field = new Field("field", "This is a test where foo is highlighed and should be highlighted", type);
    
    doc.add(field);
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    
    IndexReader reader = DirectoryReader.open(writer);
    int docId = 0;
    FieldQuery fieldQuery  = highlighter.getFieldQuery( new TermQuery(new Term("field", "foo")), reader );
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    // highlighted results are centered 
    assertEquals("This is a test where <b>foo</b> is highlighed and should be highlighted", bestFragments[0]);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 52, 1);
    assertEquals("This is a test where <b>foo</b> is highlighed and should be", bestFragments[0]);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 30, 1);
    assertEquals("a test where <b>foo</b> is highlighed", bestFragments[0]);
    reader.close();
    writer.close();
    dir.close();
  }

  public void testCustomScoreQueryHighlight() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Field field = new Field("field", "This is a test where foo is highlighed and should be highlighted", type);

    doc.add(field);
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();

    IndexReader reader = DirectoryReader.open(writer);
    int docId = 0;
    FieldQuery fieldQuery  = highlighter.getFieldQuery( new CustomScoreQuery(new TermQuery(new Term("field", "foo"))), reader );
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    // highlighted results are centered
    assertEquals("This is a test where <b>foo</b> is highlighed and should be highlighted", bestFragments[0]);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 52, 1);
    assertEquals("This is a test where <b>foo</b> is highlighed and should be", bestFragments[0]);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 30, 1);
    assertEquals("a test where <b>foo</b> is highlighed", bestFragments[0]);
    reader.close();
    writer.close();
    dir.close();
  }
  
  public void testPhraseHighlightLongTextTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Field text = new Field("text", 
        "Netscape was the general name for a series of web browsers originally produced by Netscape Communications Corporation, now a subsidiary of AOL The original browser was once the dominant browser in terms of usage share, but as a result of the first browser war it lost virtually all of its share to Internet Explorer Netscape was discontinued and support for all Netscape browsers and client products was terminated on March 1, 2008 Netscape Navigator was the name of Netscape\u0027s web browser from versions 1.0 through 4.8 The first beta release versions of the browser were released in 1994 and known as Mosaic and then Mosaic Netscape until a legal challenge from the National Center for Supercomputing Applications (makers of NCSA Mosaic, which many of Netscape\u0027s founders used to develop), led to the name change to Netscape Navigator The company\u0027s name also changed from Mosaic Communications Corporation to Netscape Communications Corporation The browser was easily the most advanced...", type);
    doc.add(text);
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer);
    int docId = 0;
    String field = "text";
    {
      BooleanQuery.Builder query = new BooleanQuery.Builder();
      query.add(new TermQuery(new Term(field, "internet")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "explorer")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query.build(), reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 128, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("first browser war it lost virtually all of its share to <b>Internet</b> <b>Explorer</b> Netscape was discontinued and support for all Netscape browsers", bestFragments[0]);
    }
    
    {
      PhraseQuery query = new PhraseQuery(field, "internet", "explorer");
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 128, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("first browser war it lost virtually all of its share to <b>Internet Explorer</b> Netscape was discontinued and support for all Netscape browsers", bestFragments[0]);
    }
    reader.close();
    writer.close();
    dir.close();
  }
  
  // see LUCENE-4899
  public void testPhraseHighlightTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Field longTermField = new Field("long_term", "This is a test thisisaverylongwordandmakessurethisfails where foo is highlighed and should be highlighted", type);
    Field noLongTermField = new Field("no_long_term", "This is a test where foo is highlighed and should be highlighted", type);

    doc.add(longTermField);
    doc.add(noLongTermField);
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer);
    int docId = 0;
    String field = "no_long_term";
    {
      BooleanQuery.Builder query = new BooleanQuery.Builder();
      query.add(new TermQuery(new Term(field, "test")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "foo")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "highlighed")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query.build(), reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("<b>foo</b> is <b>highlighed</b> and", bestFragments[0]);
    }
    {
      BooleanQuery.Builder query = new BooleanQuery.Builder();
      PhraseQuery pq = new PhraseQuery(5, field, "test", "foo", "highlighed");
      query.add(new TermQuery(new Term(field, "foo")), Occur.MUST);
      query.add(pq, Occur.MUST);
      query.add(new TermQuery(new Term(field, "highlighed")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query.build(), reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      // highlighted results are centered
      assertEquals(0, bestFragments.length);
      bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 30, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and", bestFragments[0]);
      
    }
    {
      PhraseQuery query = new PhraseQuery(3, field, "test", "foo", "highlighed");
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      // highlighted results are centered
      assertEquals(0, bestFragments.length);
      bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 30, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and", bestFragments[0]);
      
    }
    {
      PhraseQuery query = new PhraseQuery(30, field, "test", "foo", "highlighed");
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      assertEquals(0, bestFragments.length);
    }
    {
      BooleanQuery.Builder query = new BooleanQuery.Builder();
      PhraseQuery pq = new PhraseQuery(5, field, "test", "foo", "highlighed");
      BooleanQuery.Builder inner = new BooleanQuery.Builder();
      inner.add(pq, Occur.MUST);
      inner.add(new TermQuery(new Term(field, "foo")), Occur.MUST);
      query.add(inner.build(), Occur.MUST);
      query.add(pq, Occur.MUST);
      query.add(new TermQuery(new Term(field, "highlighed")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query.build(), reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      assertEquals(0, bestFragments.length);
      
      bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 30, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and", bestFragments[0]);
    }
    
    field = "long_term";
    {
      BooleanQuery.Builder query = new BooleanQuery.Builder();
      query.add(new TermQuery(new Term(field,
          "thisisaverylongwordandmakessurethisfails")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "foo")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "highlighed")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query.build(), reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("<b>thisisaverylongwordandmakessurethisfails</b>",
          bestFragments[0]);
    }
    reader.close();
    writer.close();
    dir.close();
  }

  public void testBoostedPhraseHighlightTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter( dir, newIndexWriterConfig(new MockAnalyzer( random() ) ) );
    Document doc = new Document();
    FieldType type = new FieldType( TextField.TYPE_STORED  );
    type.setStoreTermVectorOffsets( true );
    type.setStoreTermVectorPositions( true );
    type.setStoreTermVectors( true );
    type.freeze();
    StringBuilder text = new StringBuilder();
    text.append("words words junk junk junk junk junk junk junk junk highlight junk junk junk junk together junk ");
    for ( int i = 0; i<10; i++ ) {
      text.append("junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk ");
    }
    text.append("highlight words together ");
    for ( int i = 0; i<10; i++ ) {
      text.append("junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk ");
    }
    doc.add( new Field( "text", text.toString().trim(), type ) );
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer);

    // This mimics what some query parsers do to <highlight words together>
    BooleanQuery.Builder terms = new BooleanQuery.Builder();
    terms.add( clause( "text", "highlight" ), Occur.MUST );
    terms.add( clause( "text", "words" ), Occur.MUST );
    terms.add( clause( "text", "together" ), Occur.MUST );
    // This mimics what some query parsers do to <"highlight words together">
    BooleanQuery.Builder phraseB = new BooleanQuery.Builder();
    phraseB.add( clause( "text", "highlight", "words", "together" ), Occur.MUST );
    Query phrase = phraseB.build();
    phrase = new BoostQuery(phrase, 100f);
    // Now combine those results in a boolean query which should pull the phrases to the front of the list of fragments 
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add( phrase, Occur.MUST );
    query.add( phrase, Occur.SHOULD );
    FieldQuery fieldQuery = new FieldQuery( query.build(), reader, true, false );
    String fragment = highlighter.getBestFragment( fieldQuery, reader, 0, "text", 100 );
    assertEquals( "junk junk junk junk junk junk junk junk <b>highlight words together</b> junk junk junk junk junk junk junk junk", fragment );

    reader.close();
    writer.close();
    dir.close();
  }

  public void testCommonTermsQueryHighlight() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET)));
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    String[] texts = {
        "Hello this is a piece of text that is very long and contains too much preamble and the meat is really here which says kennedy has been shot",
        "This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy",
        "JFK has been shot", "John Kennedy has been shot",
        "This text has a typo in referring to Keneddy",
        "wordx wordy wordz wordx wordy wordx worda wordb wordy wordc", "y z x y z a b", "lets is a the lets is a the lets is a the lets" };
    for (int i = 0; i < texts.length; i++) {
      Document doc = new Document();
      Field field = new Field("field", texts[i], type);
      doc.add(field);
      writer.addDocument(doc);
    }
    CommonTermsQuery query = new CommonTermsQuery(Occur.MUST, Occur.SHOULD, 2);
    query.add(new Term("field", "text"));
    query.add(new Term("field", "long"));
    query.add(new Term("field", "very"));
   
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(2, hits.totalHits);
    FieldQuery fieldQuery  = highlighter.getFieldQuery(query, reader);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, hits.scoreDocs[0].doc, "field", 1000, 1);
    assertEquals("This piece of <b>text</b> refers to Kennedy at the beginning then has a longer piece of <b>text</b> that is <b>very</b> <b>long</b> in the middle and finally ends with another reference to Kennedy", bestFragments[0]);

    fieldQuery  = highlighter.getFieldQuery(query, reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, hits.scoreDocs[1].doc, "field", 1000, 1);
    assertEquals("Hello this is a piece of <b>text</b> that is <b>very</b> <b>long</b> and contains too much preamble and the meat is really here which says kennedy has been shot", bestFragments[0]);

    reader.close();
    writer.close();
    dir.close();
  }
  
  public void testMatchedFields() throws IOException {
    // Searching just on the stored field doesn't highlight a stopword
    matchedFieldsTestCase( false, true, "a match", "a <b>match</b>",
      clause( "field", "a" ), clause( "field", "match" ) );

    // Even if you add an unqueried matched field that would match it
    matchedFieldsTestCase( "a match", "a <b>match</b>",
      clause( "field", "a" ), clause( "field", "match" ) );

    // Nor if you query the field but don't add it as a matched field to the highlighter
    matchedFieldsTestCase( false, false, "a match", "a <b>match</b>",
      clause( "field_exact", "a" ), clause( "field", "match" ) );

    // But if you query the field and add it as a matched field to the highlighter then it is highlighted
    matchedFieldsTestCase( "a match", "<b>a</b> <b>match</b>",
      clause( "field_exact", "a" ), clause( "field", "match" ) );

    // It is also ok to match just the matched field but get highlighting from the stored field
    matchedFieldsTestCase( "a match", "<b>a</b> <b>match</b>",
      clause( "field_exact", "a" ), clause( "field_exact", "match" ) );

    // Boosted matched fields work too
    matchedFieldsTestCase( "a match", "<b>a</b> <b>match</b>",
      clause( "field_exact", 5, "a" ), clause( "field", "match" ) );

    // It is also ok if both the stored and the matched field match the term
    matchedFieldsTestCase( "a match", "a <b>match</b>",
      clause( "field_exact", "match" ), clause( "field", "match" ) );

    // And the highlighter respects the boosts on matched fields when sorting fragments
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "junk junk <b>a cat</b> junk junk",
      clause( "field", "cat" ), clause( "field_exact", 5, "a", "cat" ) );
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>cat</b> <b>cat</b> junk junk junk junk",
      clause( "field", "cat" ), clause( "field_exact", "a", "cat" ) );

    // The same thing works across three fields as well
    matchedFieldsTestCase( "cat cat CAT junk junk junk junk junk junk junk a cat junk junk",
      "junk junk <b>a cat</b> junk junk",
      clause( "field", "cat" ), clause( "field_exact", 200, "a", "cat" ), clause( "field_super_exact", 5, "CAT" ) );
    matchedFieldsTestCase( "a cat cat junk junk junk junk junk junk junk a CAT junk junk",
      "junk junk <b>a CAT</b> junk junk",
      clause( "field", "cat" ), clause( "field_exact", 5, "a", "cat" ), clause( "field_super_exact", 200, "a", "CAT" ) );

    // And across fields with different tokenizers!
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "junk junk <b>a cat</b> junk junk",
      clause( "field_exact", 5, "a", "cat" ), clause( "field_characters", "c" ) );
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>c</b>at <b>c</b>at junk junk junk junk",
      clause( "field_exact", "a", "cat" ), clause( "field_characters", "c" ) );
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "ca<b>t</b> ca<b>t</b> junk junk junk junk",
      clause( "field_exact", "a", "cat" ), clause( "field_characters", "t" ) );
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>cat</b> <b>cat</b> junk junk junk junk", // See how the phrases are joined?
      clause( "field", "cat" ), clause( "field_characters", 5, "c" ) );
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "junk junk <b>a cat</b> junk junk",
      clause( "field", "cat" ), clause( "field_characters", 5, "a", " ", "c", "a", "t" ) );

    // Phrases and tokens inside one another are joined
    matchedFieldsTestCase( "cats wow", "<b>cats w</b>ow",
      clause( "field", "cats" ), clause( "field_tripples", "s w" ) );

    // Everything works pretty well even if you don't require a field match
    matchedFieldsTestCase( true, false, "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "junk junk <b>a cat</b> junk junk",
      clause( "field", "cat" ), clause( "field_characters", 10, "a", " ", "c", "a", "t" ) );

    // Even boosts keep themselves pretty much intact
    matchedFieldsTestCase( true, false, "a cat cat junk junk junk junk junk junk junk a CAT junk junk",
      "junk junk <b>a CAT</b> junk junk",
      clause( "field", "cat" ), clause( "field_exact", 5, "a", "cat" ), clause( "field_super_exact", 200, "a", "CAT" ) );
    matchedFieldsTestCase( true, false, "cat cat CAT junk junk junk junk junk junk junk a cat junk junk",
      "junk junk <b>a cat</b> junk junk",
      clause( "field", "cat" ), clause( "field_exact", 200, "a", "cat" ), clause( "field_super_exact", 5, "CAT" ) );

    // Except that all the matched field matches apply even if they aren't mentioned in the query
    // which can make for some confusing scoring.  This isn't too big a deal, just something you
    // need to think about when you don't force a field match.
    matchedFieldsTestCase( true, false, "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>cat</b> <b>cat</b> junk junk junk junk",
      clause( "field", "cat" ), clause( "field_characters", 4, "a", " ", "c", "a", "t" ) );

    // It is also cool to match fields that don't have _exactly_ the same text so long as you are careful.
    // In this case field_sliced is a prefix of field.
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>cat</b> <b>cat</b> junk junk junk junk", clause( "field_sliced", "cat" ) );

    // Multiple matches add to the score of the segment
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>cat</b> <b>cat</b> junk junk junk junk",
      clause( "field", "cat" ), clause( "field_sliced", "cat" ), clause( "field_exact", 2, "a", "cat" ) );
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "junk junk <b>a cat</b> junk junk",
      clause( "field", "cat" ), clause( "field_sliced", "cat" ), clause( "field_exact", 4, "a", "cat" ) );

    // Even fields with tokens on top of one another are ok
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>cat</b> cat junk junk junk junk",
      clause( "field_der_red", 2, "der" ), clause( "field_exact", "a", "cat" ) );
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>cat</b> cat junk junk junk junk",
      clause( "field_der_red", 2, "red" ), clause( "field_exact", "a", "cat" ) );
    matchedFieldsTestCase( "cat cat junk junk junk junk junk junk junk a cat junk junk",
      "<b>cat</b> cat junk junk junk junk",
      clause( "field_der_red", "red" ), clause( "field_der_red", "der" ), clause( "field_exact", "a", "cat" ) );
  }

  public void testMultiValuedSortByScore() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter( dir, newIndexWriterConfig(new MockAnalyzer( random() ) ) );
    Document doc = new Document();
    FieldType type = new FieldType( TextField.TYPE_STORED );
    type.setStoreTermVectorOffsets( true );
    type.setStoreTermVectorPositions( true );
    type.setStoreTermVectors( true );
    type.freeze();
    doc.add( new Field( "field", "zero if naught", type ) ); // The first two fields contain the best match
    doc.add( new Field( "field", "hero of legend", type ) ); // but total a lower score (3) than the bottom
    doc.add( new Field( "field", "naught of hero", type ) ); // two fields (4)
    doc.add( new Field( "field", "naught of hero", type ) );
    writer.addDocument(doc);

    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    
    ScoreOrderFragmentsBuilder fragmentsBuilder = new ScoreOrderFragmentsBuilder();    
    fragmentsBuilder.setDiscreteMultiValueHighlighting( true );
    IndexReader reader = DirectoryReader.open(writer);
    String[] preTags = new String[] { "<b>" };
    String[] postTags = new String[] { "</b>" };
    Encoder encoder = new DefaultEncoder();
    int docId = 0;
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add( clause( "field", "hero" ), Occur.SHOULD);
    query.add( clause( "field", "of" ), Occur.SHOULD);
    query.add( clause( "field", "legend" ), Occur.SHOULD);
    FieldQuery fieldQuery = highlighter.getFieldQuery( query.build(), reader );

    for ( FragListBuilder fragListBuilder : new FragListBuilder[] {
      new SimpleFragListBuilder(), new WeightedFragListBuilder() } ) {
      String[] bestFragments = highlighter.getBestFragments( fieldQuery, reader, docId, "field", 20, 1,
          fragListBuilder, fragmentsBuilder, preTags, postTags, encoder );
      assertEquals("<b>hero</b> <b>of</b> <b>legend</b>", bestFragments[0]);
      bestFragments = highlighter.getBestFragments( fieldQuery, reader, docId, "field", 28, 1,
          fragListBuilder, fragmentsBuilder, preTags, postTags, encoder );
      assertEquals("<b>hero</b> <b>of</b> <b>legend</b>", bestFragments[0]);
      bestFragments = highlighter.getBestFragments( fieldQuery, reader, docId, "field", 30000, 1,
          fragListBuilder, fragmentsBuilder, preTags, postTags, encoder );
      assertEquals("<b>hero</b> <b>of</b> <b>legend</b>", bestFragments[0]);
    }

    reader.close();
    writer.close();
    dir.close();
  }

  public void testWithSynonym() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();

    Document doc = new Document();
    doc.add( new Field("field", "the quick brown fox", type ));
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();

    IndexReader reader = DirectoryReader.open(writer);
    int docId = 0;

    // query1: simple synonym query
    SynonymQuery synQuery = new SynonymQuery(new Term("field", "quick"), new Term("field", "fast"));
    FieldQuery fieldQuery  = highlighter.getFieldQuery(synQuery, reader);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    assertEquals("the <b>quick</b> brown fox", bestFragments[0]);

    // query2: boolean query with synonym query
    BooleanQuery.Builder bq =
        new BooleanQuery.Builder()
            .add(new BooleanClause(synQuery, Occur.MUST))
            .add(new BooleanClause(new TermQuery(new Term("field", "fox")), Occur.MUST));
    fieldQuery  = highlighter.getFieldQuery(bq.build(), reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    assertEquals("the <b>quick</b> brown <b>fox</b>", bestFragments[0]);

    reader.close();
    writer.close();
    dir.close();
  }
  
  public void testBooleanPhraseWithSynonym() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_NOT_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Token syn = new Token("httpwwwfacebookcom", 6, 29);
    syn.setPositionIncrement(0);
    CannedTokenStream ts = new CannedTokenStream(
        new Token("test", 0, 4),
        new Token("http", 6, 10),
        syn,
        new Token("www", 13, 16),
        new Token("facebook", 17, 25),
        new Token("com", 26, 29)
    );
    Field field = new Field("field", ts, type);
    doc.add(field);
    doc.add(new StoredField("field", "Test: http://www.facebook.com"));
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    
    IndexReader reader = DirectoryReader.open(writer);
    int docId = 0;
    
    // query1: match
    PhraseQuery pq = new PhraseQuery("field", "test", "http", "www", "facebook", "com");
    FieldQuery fieldQuery  = highlighter.getFieldQuery(pq, reader);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    assertEquals("<b>Test: http://www.facebook.com</b>", bestFragments[0]);
    
    // query2: match
    PhraseQuery pq2 = new PhraseQuery("field", "test", "httpwwwfacebookcom", "www", "facebook", "com");
    fieldQuery  = highlighter.getFieldQuery(pq2, reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    assertEquals("<b>Test: http://www.facebook.com</b>", bestFragments[0]);
    
    // query3: OR query1 and query2 together
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(pq, BooleanClause.Occur.SHOULD);
    bq.add(pq2, BooleanClause.Occur.SHOULD);
    fieldQuery  = highlighter.getFieldQuery(bq.build(), reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    assertEquals("<b>Test: http://www.facebook.com</b>", bestFragments[0]);
    
    reader.close();
    writer.close();
    dir.close();
  }

  public void testPhrasesSpanningFieldValues() throws IOException {
    Directory dir = newDirectory();
    // positionIncrementGap is 0 so the pharse is found across multiple field
    // values.
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();

    Document doc = new Document();
    doc.add( new Field( "field", "one two three five", type ) );
    doc.add( new Field( "field", "two three four", type ) );
    doc.add( new Field( "field", "five six five", type ) );
    doc.add( new Field( "field", "six seven eight nine eight nine eight " +
      "nine eight nine eight nine eight nine", type ) );
    doc.add( new Field( "field", "eight nine", type ) );
    doc.add( new Field( "field", "ten eleven", type ) );
    doc.add( new Field( "field", "twelve thirteen", type ) );
    writer.addDocument(doc);

    BaseFragListBuilder fragListBuilder = new SimpleFragListBuilder();
    BaseFragmentsBuilder fragmentsBuilder = new SimpleFragmentsBuilder();
    fragmentsBuilder.setDiscreteMultiValueHighlighting(true);
    FastVectorHighlighter highlighter = new FastVectorHighlighter(true, true, fragListBuilder, fragmentsBuilder);
    IndexReader reader = DirectoryReader.open(writer);
    int docId = 0;

    // Phrase that spans a field value
    Query q = new PhraseQuery("field", "four", "five");
    FieldQuery fieldQuery  = highlighter.getFieldQuery(q, reader);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 1000, 1000);
    assertEquals("two three <b>four</b>", bestFragments[0]);
    assertEquals("<b>five</b> six five", bestFragments[1]);
    assertEquals(2, bestFragments.length);

    // Phrase that ends at a field value
    q = new PhraseQuery("field", "three", "five");
    fieldQuery  = highlighter.getFieldQuery(q, reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 1000, 1000);
    assertEquals("one two <b>three five</b>", bestFragments[0]);
    assertEquals(1, bestFragments.length);

    // Phrase that spans across three values
    q = new PhraseQuery("field", "nine", "ten", "eleven", "twelve");
    fieldQuery  = highlighter.getFieldQuery(q, reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 1000, 1000);
    assertEquals("eight <b>nine</b>", bestFragments[0]);
    assertEquals("<b>ten eleven</b>", bestFragments[1]);
    assertEquals("<b>twelve</b> thirteen", bestFragments[2]);
    assertEquals(3, bestFragments.length);

    // Term query that appears in multiple values
    q = new TermQuery(new Term("field", "two"));
    fieldQuery  = highlighter.getFieldQuery(q, reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 1000, 1000);
    assertEquals("one <b>two</b> three five", bestFragments[0]);
    assertEquals("<b>two</b> three four", bestFragments[1]);
    assertEquals(2, bestFragments.length);

    reader.close();
    writer.close();
    dir.close();
  }

  private void matchedFieldsTestCase( String fieldValue, String expected, Query... queryClauses ) throws IOException {
    matchedFieldsTestCase( true, true, fieldValue, expected, queryClauses );
  }

  private void matchedFieldsTestCase( boolean useMatchedFields, boolean fieldMatch, String fieldValue, String expected, Query... queryClauses ) throws IOException {
    Document doc = new Document();
    FieldType stored = new FieldType( TextField.TYPE_STORED );
    stored.setStoreTermVectorOffsets( true );
    stored.setStoreTermVectorPositions( true );
    stored.setStoreTermVectors( true );
    stored.freeze();
    FieldType matched = new FieldType( TextField.TYPE_NOT_STORED );
    matched.setStoreTermVectorOffsets( true );
    matched.setStoreTermVectorPositions( true );
    matched.setStoreTermVectors( true );
    matched.freeze();
    doc.add( new Field( "field", fieldValue, stored ) );               // Whitespace tokenized with English stop words
    doc.add( new Field( "field_exact", fieldValue, matched ) );        // Whitespace tokenized without stop words
    doc.add( new Field( "field_super_exact", fieldValue, matched ) );  // Whitespace tokenized without toLower
    doc.add( new Field( "field_characters", fieldValue, matched ) );   // Each letter is a token
    doc.add( new Field( "field_tripples", fieldValue, matched ) );     // Every three letters is a token
    doc.add( new Field( "field_sliced", fieldValue.substring( 0,       // Sliced at 10 chars then analyzed just like field
      Math.min( fieldValue.length() - 1 , 10 ) ), matched ) );
    doc.add( new Field( "field_der_red", new CannedTokenStream(        // Hacky field containing "der" and "red" at pos = 0
          token( "der", 1, 0, 3 ),
          token( "red", 0, 0, 3 )
        ), matched ) );

    final Map<String, Analyzer> fieldAnalyzers = new TreeMap<>();
    fieldAnalyzers.put( "field", new MockAnalyzer( random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET ) );
    fieldAnalyzers.put( "field_exact", new MockAnalyzer( random() ) );
    fieldAnalyzers.put( "field_super_exact", new MockAnalyzer( random(), MockTokenizer.WHITESPACE, false ) );
    fieldAnalyzers.put( "field_characters", new MockAnalyzer( random(), new CharacterRunAutomaton( new RegExp(".").toAutomaton() ), true ) );
    fieldAnalyzers.put( "field_tripples", new MockAnalyzer( random(), new CharacterRunAutomaton( new RegExp("...").toAutomaton() ), true ) );
    fieldAnalyzers.put( "field_sliced", fieldAnalyzers.get( "field" ) );
    fieldAnalyzers.put( "field_der_red", fieldAnalyzers.get( "field" ) );  // This is required even though we provide a token stream
    Analyzer analyzer = new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
      public Analyzer getWrappedAnalyzer(String fieldName) {
        return fieldAnalyzers.get( fieldName );
      }
    };

    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter( dir, newIndexWriterConfig(analyzer));
    writer.addDocument( doc );

    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    FragListBuilder fragListBuilder = new SimpleFragListBuilder();
    FragmentsBuilder fragmentsBuilder = new ScoreOrderFragmentsBuilder();
    IndexReader reader = DirectoryReader.open(writer);
    String[] preTags = new String[] { "<b>" };
    String[] postTags = new String[] { "</b>" };
    Encoder encoder = new DefaultEncoder();
    int docId = 0;
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    for ( Query clause : queryClauses ) {
      query.add( clause, Occur.MUST );
    }
    FieldQuery fieldQuery = new FieldQuery( query.build(), reader, true, fieldMatch );
    String[] bestFragments;
    if ( useMatchedFields ) {
      Set< String > matchedFields = new HashSet<>();
      matchedFields.add( "field" );
      matchedFields.add( "field_exact" );
      matchedFields.add( "field_super_exact" );
      matchedFields.add( "field_characters" );
      matchedFields.add( "field_tripples" );
      matchedFields.add( "field_sliced" );
      matchedFields.add( "field_der_red" );
      bestFragments = highlighter.getBestFragments( fieldQuery, reader, docId, "field", matchedFields, 25, 1,
        fragListBuilder, fragmentsBuilder, preTags, postTags, encoder );
    } else {
      bestFragments = highlighter.getBestFragments( fieldQuery, reader, docId, "field", 25, 1,
        fragListBuilder, fragmentsBuilder, preTags, postTags, encoder );
    }
    assertEquals( expected, bestFragments[ 0 ] );

    reader.close();
    writer.close();
    dir.close();
  }

  private Query clause( String field, String... terms ) {
    return clause( field, 1, terms );
  }

  private Query clause( String field, float boost, String... terms ) {
    Query q;
    if ( terms.length == 1 ) {
      q = new TermQuery( new Term( field, terms[ 0 ] ) );
    } else {
      q = new PhraseQuery(field, terms);
    }
    q = new BoostQuery( q, boost );
    return q;
  }

  private static Token token( String term, int posInc, int startOffset, int endOffset ) {
    Token t = new Token( term, startOffset, endOffset );
    t.setPositionIncrement( posInc );
    return t;
  }
}
