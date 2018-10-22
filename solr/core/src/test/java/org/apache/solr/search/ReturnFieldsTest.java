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
package org.apache.solr.search;

import org.apache.lucene.util.TestUtil;
import org.apache.lucene.document.Document;
import static org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.response.transform.*;
import static org.apache.solr.response.DocsStreamer.convertLuceneDocToSolrDoc;
import org.apache.solr.schema.IndexSchema;

import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.List;
import java.util.Random;

public class ReturnFieldsTest extends SolrTestCaseJ4 {

  // :TODO: datatypes produced by the functions used may change

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
    String v = "how now brown cow";
    assertU(adoc("id","1", "text",v,  "text_np", v, "#foo_s", v));
    v = "now cow";
    assertU(adoc("id","2", "text",v,  "text_np", v));
    assertU(commit());
  }

  @Test
  public void testCopyRename() throws Exception {

    // original
    assertQ(req("q","id:1", "fl","id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=1] "
        ,"*//doc[1]/str[1][.='1'] "
        );
    
    // rename
    assertQ(req("q","id:1", "fl","xxx:id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=1] "
        ,"*//doc[1]/str[1][.='1'] "
        );

    // original and copy
    assertQ(req("q","id:1", "fl","id,xxx:id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=2] "
        ,"*//doc[1]/str[1][.='1'] "
        ,"*//doc[1]/str[2][.='1'] "
        );
    assertQ(req("q","id:1", "fl","xxx:id,id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=2] "
        ,"*//doc[1]/str[1][.='1'] "
        ,"*//doc[1]/str[2][.='1'] "
        );

    // two copies
    assertQ(req("q","id:1", "fl","xxx:id,yyy:id")
        ,"//*[@numFound='1'] "
        ,"*[count(//doc/str)=2] "
        ,"*//doc[1]/str[1][.='1'] "
        ,"*//doc[1]/str[2][.='1'] "
        );
  }

  @Test
  public void testToString() {
    for (Method m : SolrReturnFields.class.getMethods()) {
      if (m.getName().equals("toString")) {
        assertTrue(m + " is not overridden ! ", m.getDeclaringClass() == SolrReturnFields.class);
        break;
      }
    }

    final ReturnFields rf1 = new SolrReturnFields();
    final String rf1ToString = "SolrReturnFields=(globs=[]"
        +",fields=[]"
        +",okFieldNames=[]"
        +",reqFieldNames=null"
        +",transformer=null,wantsScore=false,wantsAllFields=true)";
    assertEquals(rf1ToString, rf1.toString());

    final ReturnFields rf2 = new SolrReturnFields(
        req("fl", SolrReturnFields.SCORE));
    final String rf2ToStringA = "SolrReturnFields=(globs=[]"
        +",fields=["+SolrReturnFields.SCORE+"]"
        +",okFieldNames=[null, "+SolrReturnFields.SCORE+"]"
        +",reqFieldNames=["+SolrReturnFields.SCORE+"]"
        +",transformer=score,wantsScore=true,wantsAllFields=false)";
    final String rf2ToStringB = "SolrReturnFields=(globs=[]"
        +",fields=["+SolrReturnFields.SCORE+"]"
        +",okFieldNames=["+SolrReturnFields.SCORE+", null]"
        +",reqFieldNames=["+SolrReturnFields.SCORE+"]"
        +",transformer=score,wantsScore=true,wantsAllFields=false)";
    assertTrue(
        rf2ToStringA.equals(rf2.toString()) ||
        rf2ToStringB.equals(rf2.toString()));
  }

  @Test
  public void testSeparators() {
    ReturnFields rf = new SolrReturnFields( req("fl", "id name test subject score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);

    rf = new SolrReturnFields( req("fl", "id,name,test,subject,score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);

    rf = new SolrReturnFields( req("fl", "id,name test,subject score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);

    rf = new SolrReturnFields( req("fl", "id, name  test , subject,score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);
  }

  @Test
  public void testWilcards() {
    ReturnFields rf = new SolrReturnFields( req("fl", "*") );
    assertFalse( rf.wantsScore() );
    assertTrue( rf.wantsField( "xxx" ) );
    assertTrue( rf.wantsAllFields() );
    assertNull( rf.getTransformer() );

    rf = new SolrReturnFields( req("fl", " * ") );
    assertFalse( rf.wantsScore() );
    assertTrue( rf.wantsField( "xxx" ) );
    assertTrue( rf.wantsAllFields() );
    assertNull( rf.getTransformer() );

    // Check that we want wildcards
    rf = new SolrReturnFields( req("fl", "id,aaa*,*bbb") );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "aaaxxx" ) );
    assertFalse(rf.wantsField("xxxaaa"));
    assertTrue( rf.wantsField( "xxxbbb" ) );
    assertFalse(rf.wantsField("bbbxxx"));
    assertFalse( rf.wantsField( "aa" ) );
    assertFalse( rf.wantsField( "bb" ) );
  }

  @Test
  public void testManyParameters() {
    ReturnFields rf = new SolrReturnFields( req("fl", "id name", "fl", "test subject", "fl", "score") );
    assertTrue( rf.wantsScore() );
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "name" ) );
    assertTrue( rf.wantsField( "test" ) );
    assertTrue( rf.wantsField( "subject" ) );
    assertTrue( rf.wantsField( "score" ) );
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof ScoreAugmenter);
  }

  @Test
  public void testFunctions() {
    ReturnFields rf = new SolrReturnFields( req("fl", "exists(text),id,sum(1,1)") );
    assertFalse(rf.wantsScore());
    assertTrue( rf.wantsField( "id" ) );
    assertTrue( rf.wantsField( "sum(1,1)" ));
    assertTrue( rf.wantsField( "exists(text)" ));
    assertFalse( rf.wantsAllFields() );
    assertFalse( rf.wantsField( "xxx" ) );
    assertTrue( rf.getTransformer() instanceof DocTransformers);
    DocTransformers transformers = (DocTransformers)rf.getTransformer();
    assertEquals("exists(text)", transformers.getTransformer(0).getName());
    assertEquals("sum(1,1)", transformers.getTransformer(1).getName());
  }

  @Test
  public void testTransformers() {
    ReturnFields rf = new SolrReturnFields( req("fl", "[explain]") );
    assertFalse( rf.wantsScore() );
    assertTrue(rf.wantsField("[explain]"));
    assertFalse(rf.wantsField("id"));
    assertFalse(rf.wantsAllFields());
    assertEquals( "[explain]", rf.getTransformer().getName() );

    rf = new SolrReturnFields( req("fl", "[shard],id") );
    assertFalse( rf.wantsScore() );
    assertTrue(rf.wantsField("[shard]"));
    assertTrue(rf.wantsField("id"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertEquals( "[shard]", rf.getTransformer().getName() );

    rf = new SolrReturnFields( req("fl", "[docid]") );
    assertFalse( rf.wantsScore() );
    assertTrue(rf.wantsField("[docid]"));
    assertFalse( rf.wantsField( "id" ) );
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertEquals( "[docid]", rf.getTransformer().getName() );

    rf = new SolrReturnFields( req("fl", "mydocid:[docid]") );
    assertFalse( rf.wantsScore() );
    assertTrue(rf.wantsField("mydocid"));
    assertFalse( rf.wantsField( "id" ) );
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertEquals( "mydocid", rf.getTransformer().getName() );

    rf = new SolrReturnFields( req("fl", "[docid][shard]") );
    assertFalse( rf.wantsScore() );
    assertTrue(rf.wantsField("[docid]"));
    assertTrue(rf.wantsField("[shard]"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertTrue( rf.getTransformer() instanceof DocTransformers);
    assertEquals(2, ((DocTransformers)rf.getTransformer()).size());

    rf = new SolrReturnFields( req("fl", "[xxxxx]") );
    assertFalse( rf.wantsScore() );
    assertTrue(rf.wantsField("[xxxxx]"));
    assertFalse( rf.wantsField( "id" ) );
    assertFalse(rf.wantsAllFields());
    assertNull(rf.getTransformer());

    // Don't return 'store_rpt' just because it is required by the transformer
    rf = new SolrReturnFields( req("fl", "[geo f=store_rpt]") );
    assertFalse( rf.wantsScore() );
    assertTrue(rf.wantsField("[geo]"));
    assertFalse( rf.wantsField( "store_rpt" ) );
    assertFalse(rf.wantsAllFields());
    assertNotNull(rf.getTransformer());
  }

  @Test
  public void testAliases() {
    ReturnFields rf = new SolrReturnFields( req("fl", "newId:id newName:name newTest:test newSubject:subject") );
    assertTrue(rf.wantsField("id"));
    assertTrue(rf.wantsField("name"));
    assertTrue(rf.wantsField("test"));
    assertTrue(rf.wantsField("subject"));
    assertTrue(rf.wantsField("newId"));
    assertTrue(rf.wantsField("newName"));
    assertTrue(rf.wantsField("newTest"));
    assertTrue(rf.wantsField("newSubject"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());

    rf = new SolrReturnFields( req("fl", "newId:id newName:name newTest:test newSubject:subject score") );
    assertTrue(rf.wantsField("id"));
    assertTrue(rf.wantsField("name"));
    assertTrue(rf.wantsField("test"));
    assertTrue(rf.wantsField("subject"));
    assertTrue(rf.wantsField("newId"));
    assertTrue(rf.wantsField("newName"));
    assertTrue(rf.wantsField("newTest"));
    assertTrue(rf.wantsField("newSubject"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
    assertTrue( rf.getTransformer() instanceof DocTransformers);
    assertEquals(5, ((DocTransformers)rf.getTransformer()).size());  // 4 rename and score
  }

  // hyphens in field names are not supported in all contexts, but we wanted
  // the simplest case of fl=foo-bar to work
  @Test
  public void testHyphenInFieldName() {
    ReturnFields rf = new SolrReturnFields(req("fl", "id-test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id-test"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
  }

  @Test
  public void testTrailingDotInFieldName() {
    ReturnFields rf = new SolrReturnFields(req("fl", "id.test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id.test"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());

    rf = new SolrReturnFields(req("fl", "test:id.test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id.test"));
    assertTrue(rf.wantsField("test"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());

    rf = new SolrReturnFields(req("fl", "test.id:id.test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id.test"));
    assertTrue(rf.wantsField("test.id"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
  }

  @Test
  public void testTrailingDollarInFieldName() {
    ReturnFields rf = new SolrReturnFields(req("fl", "id$test"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id$test"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());
  }

  @Test
  public void testFunkyFieldNames() {
    ReturnFields rf = new SolrReturnFields(req("fl", "#foo_s", "fl", "id"));
    assertFalse(rf.wantsScore());
    assertTrue(rf.wantsField("id"));
    assertTrue(rf.wantsField("#foo_s"));
    assertFalse(rf.wantsField("xxx"));
    assertFalse(rf.wantsAllFields());

    assertQ(req("q","id:1", "fl","#foo_s", "fl","id")
            ,"//*[@numFound='1'] "
            ,"//str[@name='id'][.='1']"
            ,"//arr[@name='#foo_s']/str[.='how now brown cow']"
            );

  }

  /**
   * Whitebox verification that the conversion from lucene {@link Document} to {@link SolrDocument} respects
   * the {@link ReturnFields} and doesn't unneccessarily convert Fields that aren't needed.
   * <p>
   * This is important because {@link SolrDocumentFetcher} may return additional fields 
   * (lazy or otherwise) if the document has been cached.
   * </p>
   */
  public void testWhiteboxSolrDocumentConversion() {
    final IndexSchema schema = h.getCore().getLatestSchema();
    SolrDocument docOut = null;

    // a "mock" Document with a bunch of fields...
    //
    // (we can mock this with all StringField instances because convertLuceneDocToSolrDoc only
    // uses the schema for multivalued-ness)
    final Document docIn = new Document();
    final StringBuilder allFieldNames = new StringBuilder();
    docIn.add(new StringField("id","bar",Store.YES));
    allFieldNames.append("id");
    docIn.add(new StringField("store_rpt","42",Store.YES));
    allFieldNames.append(",store_rpt");
    docIn.add(new StringField("subword","bar",Store.YES)); // single value in multi-value field
    allFieldNames.append(",subword");
    docIn.add(new StringField("uniq","xxx",Store.YES)); 
    docIn.add(new StringField("uniq","yyy",Store.YES)); // multi-value in multi-valued field
    allFieldNames.append(",uniq");
    for (int i = 0; i < 20; i++) {
      final String foo = "foo_" + i + "_s1";
      allFieldNames.append(",").append(foo);
      docIn.add(new StringField(foo, "bar"+i, Store.YES));
    }

    // output should only have a single field
    docOut = convertLuceneDocToSolrDoc(docIn, schema, new SolrReturnFields(req("fl","id")));
    assertEquals(docOut.toString(), 1, docOut.size());
    assertEquals(docOut.toString(),
                 Collections.singleton("id"),
                 docOut.getFieldNames());
    assertTrue(docOut.toString(), docOut.get("id") instanceof StringField);

    // output should only have the few specified fields
    // behavior should be ultimately be consistent for all of these ReturnField instances
    // (aliasing, extra requested by transformer, or otherwise)
    for (ReturnFields rf : Arrays.asList
           (new SolrReturnFields(req("fl","id,subword,store_rpt,uniq,foo_2_s1")),
            new SolrReturnFields(req("fl","id,xxx:[geo f=store_rpt],uniq,foo_2_s1,subword")),
            new SolrReturnFields(req("fl","id,xxx:subword,uniq,yyy:foo_2_s1,[geo f=store_rpt]")))) {
      docOut = convertLuceneDocToSolrDoc(docIn, schema, rf);
      final String debug = rf.toString() + " => " +docOut.toString();
      assertEquals(debug, 5, docOut.size());
      assertEquals(debug,
                   new HashSet<String>(Arrays.asList("id","subword","uniq","foo_2_s1","store_rpt")),
                   docOut.getFieldNames());
      assertTrue(debug, docOut.get("id") instanceof StringField);
      assertTrue(debug, docOut.get("store_rpt") instanceof StringField);
      assertTrue(debug, docOut.get("foo_2_s1") instanceof StringField);
      assertTrue(debug, docOut.get("subword") instanceof List);
      assertTrue(debug, docOut.get("uniq") instanceof List);
    }
    
    // all source fields should be in the output
    // behavior should be ultimately be consistent for all of these ReturnField instances
    // (globbing or requesting more fields then doc has)
    for (ReturnFields rf : Arrays.asList
           (new SolrReturnFields(),
            new SolrReturnFields(req()),
            new SolrReturnFields(req("fl","*")),
            new SolrReturnFields(req("fl","*,score")),
            new SolrReturnFields(req("fl","id,subword,uniq,foo_*,store_*")),
            new SolrReturnFields(req("fl",allFieldNames+",bogus1,bogus2,bogus3")))) {
      
      docOut = convertLuceneDocToSolrDoc(docIn, schema, rf);
      final String debug = rf.toString() + " => " +docOut.toString();
      assertEquals(debug, 24, docOut.size());
      assertTrue(debug, docOut.get("id") instanceof StringField);
      assertTrue(debug, docOut.get("store_rpt") instanceof StringField);
      assertTrue(debug, docOut.get("subword") instanceof List);
      assertTrue(debug, docOut.get("uniq") instanceof List);
      for (int i = 0; i < 20; i++) {
        assertTrue(debug, docOut.get("foo_" + i + "_s1") instanceof StringField);
      }
    }
    
  }

  
  public void testWhitespace() {
    Random r = random();
    final int iters = atLeast(30);

    for (int i = 0; i < iters; i++) {
      final boolean aliasId = r.nextBoolean();
      final boolean aliasFoo = r.nextBoolean();

      final String id = randomWhitespace(r, 0, 3) +
        (aliasId ? "aliasId:" : "") +
        "id" + 
        randomWhitespace(r, 1, 3);
      final String foo_i = randomWhitespace(r, 0, 3) +
        (aliasFoo ? "aliasFoo:" : "") +
        "foo_i" + 
        randomWhitespace(r, 0, 3);

      final String fl = id + (r.nextBoolean() ? "" : ",") + foo_i;
      ReturnFields rf = new SolrReturnFields(req("fl", fl));

      assertFalse("score ("+fl+")", rf.wantsScore());

      assertTrue("id ("+fl+")", rf.wantsField("id"));
      assertTrue("foo_i ("+fl+")", rf.wantsField("foo_i"));

      assertEquals("aliasId ("+fl+")", aliasId, rf.wantsField("aliasId"));
      assertEquals("aliasFoo ("+fl+")", aliasFoo, rf.wantsField("aliasFoo"));

      assertFalse(rf.wantsField("xxx"));
      assertFalse(rf.wantsAllFields());
    }
  }

  /** List of characters that match {@link Character#isWhitespace} */
  private static final char[] WHITESPACE_CHARACTERS = new char[] {
    // :TODO: is this list exhaustive?
    '\u0009',
    '\n',    
    '\u000B',
    '\u000C',
    '\r',    
    '\u001C',
    '\u001D',
    '\u001E',
    '\u001F',
    '\u0020',
    // '\u0085', failed sanity check?
    '\u1680',
    // '\u180E', no longer whitespace in Unicode 7.0 (Java 9)!
    '\u2000',
    '\u2001',
    '\u2002',
    '\u2003',
    '\u2004',
    '\u2005',
    '\u2006',
    '\u2008',
    '\u2009',
    '\u200A',
    '\u2028',
    '\u2029',
    '\u205F',
    '\u3000',
  };

  static {
    // if the JVM/unicode can redefine whitespace once (LUCENE-6760), it might happen again
    // in the future.  if that happens, fail early with a clera msg, even if java asserts
    // (used in randomWhitespace) are disbled
    
    for (int offset = 0; offset < WHITESPACE_CHARACTERS.length; offset++) {
      char c = WHITESPACE_CHARACTERS[offset];
      if (! Character.isWhitespace(c) ) {
        fail(String.format(Locale.ENGLISH, "Not really whitespace? New JVM/Unicode definitions? WHITESPACE_CHARACTERS[%d] is '\\u%04X'", offset, (int) c));
      }
    }
  }
  
  /**
   * Returns a random string in the specified length range consisting 
   * entirely of whitespace characters 
   * @see #WHITESPACE_CHARACTERS
   */
  public static String randomWhitespace(Random r, int minLength, int maxLength) {
    final int end = TestUtil.nextInt(r, minLength, maxLength);
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < end; i++) {
      int offset = TestUtil.nextInt(r, 0, WHITESPACE_CHARACTERS.length-1);
      char c = WHITESPACE_CHARACTERS[offset];
      // sanity check
      assert Character.isWhitespace(c) : String.format(Locale.ENGLISH, "Not really whitespace? WHITESPACE_CHARACTERS[%d] is '\\u%04X'", offset, (int) c);
      out.append(c);
    }
    return out.toString();
  }

}
