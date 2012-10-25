package org.apache.lucene.search.vectorhighlight;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util._TestUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SimpleFragmentsBuilderTest extends AbstractTestCase {
  
  public void test1TermIndex() throws Exception {
    FieldFragList ffl = ffl(new TermQuery(new Term(F, "a")), "a" );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    assertEquals( "<b>a</b>", sfb.createFragment( reader, 0, F, ffl ) );

    // change tags
    sfb = new SimpleFragmentsBuilder( new String[]{ "[" }, new String[]{ "]" } );
    assertEquals( "[a]", sfb.createFragment( reader, 0, F, ffl ) );
  }
  
  public void test2Frags() throws Exception {
    FieldFragList ffl = ffl(new TermQuery(new Term(F, "a")), "a b b b b b b b b b b b a b a b" );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    String[] f = sfb.createFragments( reader, 0, F, ffl, 3 );
    // 3 snippets requested, but should be 2
    assertEquals( 2, f.length );
    assertEquals( "<b>a</b> b b b b b b b b b b", f[0] );
    assertEquals( "b b <b>a</b> b <b>a</b> b", f[1] );
  }
  
  public void test3Frags() throws Exception {
    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(F, "a")), BooleanClause.Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term(F, "c")), BooleanClause.Occur.SHOULD);
    
    FieldFragList ffl = ffl(booleanQuery, "a b b b b b b b b b b b a b a b b b b b c a a b b" );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    String[] f = sfb.createFragments( reader, 0, F, ffl, 3 );
    assertEquals( 3, f.length );
    assertEquals( "<b>a</b> b b b b b b b b b b", f[0] );
    assertEquals( "b b <b>a</b> b <b>a</b> b b b b b c", f[1] );
    assertEquals( "<b>c</b> <b>a</b> <b>a</b> b b", f[2] );
  }
  
  public void testTagsAndEncoder() throws Exception {
    FieldFragList ffl = ffl(new TermQuery(new Term(F, "a")), "<h1> a </h1>" );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    String[] preTags = { "[" };
    String[] postTags = { "]" };
    assertEquals( "&lt;h1&gt; [a] &lt;/h1&gt;",
        sfb.createFragment( reader, 0, F, ffl, preTags, postTags, new SimpleHTMLEncoder() ) );
  }

  private FieldFragList ffl(Query query, String indexValue ) throws Exception {
    make1d1fIndex( indexValue );
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
    // Should we probably be trimming?
    assertEquals( "  a b c  <b>d</b> e", sfb.createFragment( reader, 0, F, ffl ) );
  }
  
  public void test1PhraseLongMV() throws Exception {
    makeIndexLongMV();

    FieldQuery fq = new FieldQuery( pqF( "search", "engines" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    assertEquals( "customization: The most <b>search engines</b> use only one of these methods. Even the <b>search engines</b> that says they can",
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
    assertEquals( "additional hardware. \nWhen you talk about processing <b>speed</b>, the", sfb.createFragment( reader, 0, F, ffl ) );
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
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzerW).setOpenMode(OpenMode.CREATE));
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorOffsets(true);
    customType.setStoreTermVectorPositions(true);
    doc.add( new Field( F, "aaa", customType) );
    //doc.add( new Field( F, "aaa", Store.NO, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS ) );
    writer.addDocument( doc );
    writer.close();
    if (reader != null) reader.close();
    reader = DirectoryReader.open(dir);
  }
  
  public void test1StrMV() throws Exception {
    makeIndexStrMV();

    FieldQuery fq = new FieldQuery( tq( "defg" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    sfb.setMultiValuedSeparator( '/' );
    assertEquals( "abc/<b>defg</b>/hijkl", sfb.createFragment( reader, 0, F, ffl ) );
  }
  
  public void testMVSeparator() throws Exception {
    makeIndexShortMV();

    FieldQuery fq = new FieldQuery( tq( "d" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    sfb.setMultiValuedSeparator( '/' );
    assertEquals( "//a b c//<b>d</b> e", sfb.createFragment( reader, 0, F, ffl ) );
  }

  public void testDiscreteMultiValueHighlighting() throws Exception {
    makeIndexShortMV();

    FieldQuery fq = new FieldQuery( tq( "d" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
    sfb.setDiscreteMultiValueHighlighting(true);
    assertEquals( "<b>d</b> e", sfb.createFragment( reader, 0, F, ffl ) );

    make1dmfIndex("some text to highlight", "highlight other text");
    fq = new FieldQuery( tq( "text" ), true, true );
    stack = new FieldTermStack( reader, 0, F, fq );
    fpl = new FieldPhraseList( stack, fq );
    sflb = new SimpleFragListBuilder();
    ffl = sflb.createFieldFragList( fpl, 32 );
    String[] result = sfb.createFragments(reader, 0, F, ffl, 3);
    assertEquals(2, result.length);
    assertEquals("some <b>text</b> to highlight", result[0]);
    assertEquals("highlight other <b>text</b>", result[1]);

    fq = new FieldQuery( tq( "highlight" ), true, true );
    stack = new FieldTermStack( reader, 0, F, fq );
    fpl = new FieldPhraseList( stack, fq );
    sflb = new SimpleFragListBuilder();
    ffl = sflb.createFieldFragList( fpl, 32 );
    result = sfb.createFragments(reader, 0, F, ffl, 3);
    assertEquals(2, result.length);
    assertEquals("text to <b>highlight</b>", result[0]);
    assertEquals("<b>highlight</b> other text", result[1]);
  }

  public void testRandomDiscreteMultiValueHighlighting() throws Exception {
    String[] randomValues = new String[3 + random().nextInt(10 * RANDOM_MULTIPLIER)];
    for (int i = 0; i < randomValues.length; i++) {
      String randomValue;
      do {
        randomValue = _TestUtil.randomSimpleString(random());
      } while ("".equals(randomValue));
      randomValues[i] = randomValue;
    }

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT,
            new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorOffsets(true);
    customType.setStoreTermVectorPositions(true);

    int numDocs = randomValues.length * 5;
    int numFields = 2 + random().nextInt(5);
    int numTerms = 2 + random().nextInt(3);
    List<Doc> docs = new ArrayList<Doc>(numDocs);
    List<Document> documents = new ArrayList<Document>(numDocs);
    Map<String, Set<Integer>> valueToDocId = new HashMap<String, Set<Integer>>();
    for (int i = 0; i < numDocs; i++) {
      Document document = new Document();
      String[][] fields = new String[numFields][numTerms];
      for (int j = 0; j < numFields; j++) {
        String[] fieldValues = new String[numTerms];
        fieldValues[0] = getRandomValue(randomValues, valueToDocId, i);
        StringBuilder builder = new StringBuilder(fieldValues[0]);
        for (int k = 1; k < numTerms; k++) {
          fieldValues[k] = getRandomValue(randomValues, valueToDocId, i);
          builder.append(' ').append(fieldValues[k]);
        }
        document.add(new Field(F, builder.toString(), customType));
        fields[j] = fieldValues;
      }
      docs.add(new Doc(fields));
      documents.add(document);
    }
    writer.addDocuments(documents);
    writer.close();
    IndexReader reader = DirectoryReader.open(dir);

    try {
      int highlightIters = 1 + random().nextInt(120 * RANDOM_MULTIPLIER);
      for (int highlightIter = 0; highlightIter < highlightIters; highlightIter++) {
        String queryTerm = randomValues[random().nextInt(randomValues.length)];
        int randomHit = valueToDocId.get(queryTerm).iterator().next();
        List<StringBuilder> builders = new ArrayList<StringBuilder>();
        for (String[] fieldValues : docs.get(randomHit).fieldValues) {
          StringBuilder builder = new StringBuilder();
          boolean hit = false;
          for (int i = 0; i < fieldValues.length; i++) {
            if (queryTerm.equals(fieldValues[i])) {
              builder.append("<b>").append(queryTerm).append("</b>");
              hit = true;
            } else {
              builder.append(fieldValues[i]);
            }
            if (i != fieldValues.length - 1) {
              builder.append(' ');
            }
          }
          if (hit) {
            builders.add(builder);
          }
        }

        FieldQuery fq = new FieldQuery(tq(queryTerm), true, true);
        FieldTermStack stack = new FieldTermStack(reader, randomHit, F, fq);

        FieldPhraseList fpl = new FieldPhraseList(stack, fq);
        SimpleFragListBuilder sflb = new SimpleFragListBuilder(100);
        FieldFragList ffl = sflb.createFieldFragList(fpl, 300);

        SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
        sfb.setDiscreteMultiValueHighlighting(true);
        String[] actualFragments = sfb.createFragments(reader, randomHit, F, ffl, numFields);
        assertEquals(builders.size(), actualFragments.length);
        for (int i = 0; i < actualFragments.length; i++) {
          assertEquals(builders.get(i).toString(), actualFragments[i]);
        }
      }
    } finally {
      reader.close();
      dir.close();
    }
  }

  private String getRandomValue(String[] randomValues, Map<String, Set<Integer>> valueToDocId, int docId) {
    String value = randomValues[random().nextInt(randomValues.length)];
    if (!valueToDocId.containsKey(value)) {
      valueToDocId.put(value, new HashSet<Integer>());
    }
    valueToDocId.get(value).add(docId);
    return value;
  }

  private static class Doc {

    final String[][] fieldValues;

    private Doc(String[][] fieldValues) {
      this.fieldValues = fieldValues;
    }
  }

}
