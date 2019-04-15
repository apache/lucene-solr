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
package org.apache.solr.schema;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.TestUtil;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

import org.junit.Before;
import org.junit.BeforeClass;
import static org.hamcrest.CoreMatchers.instanceOf;

public class TestSortableTextField extends SolrTestCaseJ4 {

  protected static final String BIG_CONST
    = StringUtils.repeat("x", SortableTextField.DEFAULT_MAX_CHARS_FOR_DOC_VALUES);
  
  @BeforeClass
  public static void create() throws Exception {
    initCore("solrconfig-minimal.xml","schema-sorting-text.xml");
    
    // sanity check our fields & types...

    // these should all use docValues (either explicitly or implicitly)...
    for (String n : Arrays.asList("keyword_stxt", 
                                  "whitespace_stxt", "whitespace_f_stxt", "whitespace_l_stxt")) {
           
      FieldType ft = h.getCore().getLatestSchema().getFieldTypeByName(n);
      assertEquals("type " + ft.getTypeName() + " should have docvalues - schema got changed?",
                   true, ft.getNamedPropertyValues(true).get("docValues")) ;
    }
    for (String n : Arrays.asList("keyword_stxt", "keyword_dv_stxt",
                                  "whitespace_stxt", "whitespace_nois_stxt",
                                  "whitespace_f_stxt", "whitespace_l_stxt")) {
                                  
      SchemaField sf = h.getCore().getLatestSchema().getField(n);
      assertTrue("field " + sf.getName() + " should have docvalues - schema got changed?",
                 sf.hasDocValues()) ;
    }

    { // this field should *NOT* have docValues .. should behave like a plain old TextField
      SchemaField sf = h.getCore().getLatestSchema().getField("whitespace_nodv_stxt");
      assertFalse("field " + sf.getName() + " should not have docvalues - schema got changed?",
                  sf.hasDocValues()) ;
    }
    
  }
  
  @Before
  public void cleanup() throws Exception {
    clearIndex();
  }

  public void testSimple() throws Exception {
    assertU(adoc("id","1", "whitespace_stxt", "how now brown cow ?", "whitespace_f_stxt", "aaa bbb"));
    assertU(adoc("id","2", "whitespace_stxt", "how now brown dog ?", "whitespace_f_stxt", "bbb aaa"));
    assertU(adoc("id","3", "whitespace_stxt", "how now brown cat ?", "whitespace_f_stxt", "xxx yyy"));
    assertU(adoc("id","4", "whitespace_stxt", "dog and cat"          /* no val for whitespace_f_stxt */));
                 
    assertU(commit());

    // search & sort
    // NOTE: even if the field is indexed=false, should still be able to sort on it
    for (String sortf : Arrays.asList("whitespace_stxt", "whitespace_nois_stxt", "whitespace_plain_str")) {
      assertQ(req("q", "whitespace_stxt:cat", "sort", sortf + " asc")
              , "//*[@numFound='2']"
              , "//result/doc[1]/str[@name='id'][.=4]"
              , "//result/doc[2]/str[@name='id'][.=3]"
              );
      assertQ(req("q", "whitespace_stxt:cat", "sort", sortf + " desc")
              , "//*[@numFound='2']"
              , "//result/doc[1]/str[@name='id'][.=3]"
              , "//result/doc[2]/str[@name='id'][.=4]"
              );
      assertQ(req("q", "whitespace_stxt:brown", "sort", sortf + " asc")
              , "//*[@numFound='3']"
              , "//result/doc[1]/str[@name='id'][.=3]"
              , "//result/doc[2]/str[@name='id'][.=1]"
              , "//result/doc[3]/str[@name='id'][.=2]"
              );
      assertQ(req("q", "whitespace_stxt:brown", "sort", sortf + " desc")
              , "//*[@numFound='3']"
              , "//result/doc[1]/str[@name='id'][.=2]"
              , "//result/doc[2]/str[@name='id'][.=1]"
              , "//result/doc[3]/str[@name='id'][.=3]"
              );
      
      // we should still be able to search if docValues="false" (but sort on a diff field)
      assertQ(req("q","whitespace_nodv_stxt:cat", "sort", sortf + " asc")
              , "//*[@numFound='2']"
              , "//result/doc[1]/str[@name='id'][.=4]"
              , "//result/doc[2]/str[@name='id'][.=3]"
              );
    }
    
    // attempting to sort on docValues="false" field should give an error...
    assertQEx("attempting to sort on docValues=false field should give an error",
              "when docValues=\"false\"",
              req("q","*:*", "sort", "whitespace_nodv_stxt asc"),
              ErrorCode.BAD_REQUEST);

    // sortMissing - whitespace_f_stxt copyField to whitespace_l_stxt
    assertQ(req("q","*:*", "sort", "whitespace_f_stxt asc")
            , "//*[@numFound='4']"
            , "//result/doc[1]/str[@name='id'][.=4]"
            , "//result/doc[2]/str[@name='id'][.=1]"
            , "//result/doc[3]/str[@name='id'][.=2]"
            , "//result/doc[4]/str[@name='id'][.=3]"
            );    
    assertQ(req("q","*:*", "sort", "whitespace_f_stxt desc")
            , "//*[@numFound='4']"
            , "//result/doc[1]/str[@name='id'][.=4]"
            , "//result/doc[2]/str[@name='id'][.=3]"
            , "//result/doc[3]/str[@name='id'][.=2]"
            , "//result/doc[4]/str[@name='id'][.=1]"
            );    
    assertQ(req("q","*:*", "sort", "whitespace_l_stxt asc")
            , "//*[@numFound='4']"
            , "//result/doc[1]/str[@name='id'][.=1]"
            , "//result/doc[2]/str[@name='id'][.=2]"
            , "//result/doc[3]/str[@name='id'][.=3]"
            , "//result/doc[4]/str[@name='id'][.=4]"
            );    
    assertQ(req("q","*:*", "sort", "whitespace_l_stxt desc")
            , "//*[@numFound='4']"
            , "//result/doc[1]/str[@name='id'][.=3]"
            , "//result/doc[2]/str[@name='id'][.=2]"
            , "//result/doc[3]/str[@name='id'][.=1]"
            , "//result/doc[4]/str[@name='id'][.=4]"
            );
  }

  public void testSimpleSearchAndFacets() throws Exception {
    assertU(adoc("id","1", "whitespace_stxt", "how now brown cow ?"));
    assertU(adoc("id","2", "whitespace_stxt", "how now brown cow ?"));
    assertU(adoc("id","3", "whitespace_stxt", "holy cow !"));
    assertU(adoc("id","4", "whitespace_stxt", "dog and cat"));
    
    assertU(commit());

    // NOTE: even if the field is indexed=false, should still be able to facet on it
    for (String facet : Arrays.asList("whitespace_stxt", "whitespace_nois_stxt",
                                      "whitespace_m_stxt", "whitespace_plain_str")) {
      for (String search : Arrays.asList("whitespace_stxt", "whitespace_nodv_stxt",
                                         "whitespace_m_stxt", "whitespace_plain_txt")) {
        // facet.field
        final String fpre = "//lst[@name='facet_fields']/lst[@name='"+facet+"']/";
        assertQ(req("q", search + ":cow", "rows", "0", 
                    "facet.field", facet, "facet", "true")
                , "//*[@numFound='3']"
                , fpre + "int[@name='how now brown cow ?'][.=2]"
                , fpre + "int[@name='holy cow !'][.=1]"
                , fpre + "int[@name='dog and cat'][.=0]"
                );
        
        // json facet
        final String jpre = "//lst[@name='facets']/lst[@name='x']/arr[@name='buckets']/";
        assertQ(req("q", search + ":cow", "rows", "0", 
                    "json.facet", "{x:{ type: terms, field:'" + facet + "', mincount:0 }}")
                , "//*[@numFound='3']"
                , jpre + "lst[str[@name='val'][.='how now brown cow ?']][int[@name='count'][.=2]]"
                , jpre + "lst[str[@name='val'][.='holy cow !']][int[@name='count'][.=1]]"
                , jpre + "lst[str[@name='val'][.='dog and cat']][int[@name='count'][.=0]]"
                );
        
      }
    }
  }

  
  public void testWhiteboxIndexReader() throws Exception {
    assertU(adoc("id","1",
                 "whitespace_stxt", "how now brown cow ?",
                 "whitespace_m_stxt", "xxx",
                 "whitespace_m_stxt", "yyy",
                 "whitespace_f_stxt", "aaa bbb",
                 "keyword_stxt", "Blarggghhh!"));
    assertU(commit());

    final RefCounted<SolrIndexSearcher> searcher = h.getCore().getNewestSearcher(false);
    try {
      final LeafReader r = searcher.get().getSlowAtomicReader();

      // common cases...
      for (String field : Arrays.asList("keyword_stxt", "keyword_dv_stxt",
                                        "whitespace_stxt", "whitespace_f_stxt", "whitespace_l_stxt")) {
        assertNotNull("FieldInfos: " + field, r.getFieldInfos().fieldInfo(field));
        assertEquals("DocValuesType: " + field,
                     DocValuesType.SORTED, r.getFieldInfos().fieldInfo(field).getDocValuesType());
        assertNotNull("DocValues: " + field, r.getSortedDocValues(field));
        assertNotNull("Terms: " + field, r.terms(field));
                      
      }
      
      // special cases...
      assertNotNull(r.getFieldInfos().fieldInfo("whitespace_nodv_stxt"));
      assertEquals(DocValuesType.NONE,
                   r.getFieldInfos().fieldInfo("whitespace_nodv_stxt").getDocValuesType());
      assertNull(r.getSortedDocValues("whitespace_nodv_stxt"));
      assertNotNull(r.terms("whitespace_nodv_stxt"));
      // 
      assertNotNull(r.getFieldInfos().fieldInfo("whitespace_nois_stxt"));
      assertEquals(DocValuesType.SORTED,
                   r.getFieldInfos().fieldInfo("whitespace_nois_stxt").getDocValuesType());
      assertNotNull(r.getSortedDocValues("whitespace_nois_stxt"));
      assertNull(r.terms("whitespace_nois_stxt"));
      //
      assertNotNull(r.getFieldInfos().fieldInfo("whitespace_m_stxt"));
      assertEquals(DocValuesType.SORTED_SET,
                   r.getFieldInfos().fieldInfo("whitespace_m_stxt").getDocValuesType());
      assertNotNull(r.getSortedSetDocValues("whitespace_m_stxt"));
      assertNotNull(r.terms("whitespace_m_stxt"));
        
    } finally {
      if (null != searcher) {
        searcher.decref();
      }
    }
  }
  
  public void testWhiteboxCreateFields() throws Exception {
    List<IndexableField> values = null;

    // common case...
    for (String field : Arrays.asList("keyword_stxt", "keyword_dv_stxt",
                                      "whitespace_stxt", "whitespace_f_stxt", "whitespace_l_stxt")) {
      values = createIndexableFields(field);
      assertEquals(field, 2, values.size());
      assertThat(field, values.get(0), instanceOf(Field.class));
      assertThat(field, values.get(1), instanceOf(SortedDocValuesField.class));
    }
    
    // special cases...
    values = createIndexableFields("whitespace_nois_stxt");
    assertEquals(1, values.size());
    assertThat(values.get(0), instanceOf(SortedDocValuesField.class));
    //
    values = createIndexableFields("whitespace_nodv_stxt");
    assertEquals(1, values.size());
    assertThat(values.get(0), instanceOf(Field.class));
    //
    values = createIndexableFields("whitespace_m_stxt");
    assertEquals(2, values.size());
    assertThat(values.get(0), instanceOf(Field.class));
    assertThat(values.get(1), instanceOf(SortedSetDocValuesField.class));      
  }
  private List<IndexableField> createIndexableFields(String fieldName) {
    SchemaField sf = h.getCore().getLatestSchema().getField(fieldName);
    return sf.getType().createFields(sf, "dummy value");
  }

  public void testMaxCharsSort() throws Exception {
    assertU(adoc("id","1", "whitespace_stxt", "aaa bbb ccc ddd"));
    assertU(adoc("id","2", "whitespace_stxt", "aaa bbb xxx yyy"));
    assertU(adoc("id","3", "whitespace_stxt", "aaa bbb ccc xxx"));
    assertU(adoc("id","4", "whitespace_stxt", "aaa"));
    assertU(commit());

    // all terms should be searchable in all fields, even if the docvalues are limited
    for (String searchF : Arrays.asList("whitespace_stxt", "whitespace_plain_txt",
                                        "whitespace_max3_stxt", "whitespace_max6_stxt",
                                        "whitespace_max0_stxt", "whitespace_maxNeg_stxt")) {
      //  maxChars of 0 or neg should be equivalent to no max at all
      for (String sortF : Arrays.asList("whitespace_stxt", "whitespace_plain_str", 
                                        "whitespace_max0_stxt", "whitespace_maxNeg_stxt")) {
        
        assertQ(req("q", searchF + ":ccc", "sort", sortF + " desc, id asc")
                , "//*[@numFound='2']"
                , "//result/doc[1]/str[@name='id'][.=3]"
                , "//result/doc[2]/str[@name='id'][.=1]"
                );
        
        assertQ(req("q", searchF + ":ccc", "sort", sortF + " asc, id desc")
                , "//*[@numFound='2']"
                , "//result/doc[1]/str[@name='id'][.=1]"
                , "//result/doc[2]/str[@name='id'][.=3]"
                );
      }
    }
    
    // sorting on a maxChars limited fields should force tie breaker
    for (String dir : Arrays.asList("asc", "desc")) {
      // for max3, dir shouldn't matter - should always tie..
      assertQ(req("q", "*:*", "sort", "whitespace_max3_stxt "+dir+", id desc") // max3, id desc
              , "//*[@numFound='4']"
              , "//result/doc[1]/str[@name='id'][.=4]"
              , "//result/doc[2]/str[@name='id'][.=3]"
              , "//result/doc[3]/str[@name='id'][.=2]"
              , "//result/doc[4]/str[@name='id'][.=1]"
              );
      assertQ(req("q", "*:*", "sort", "whitespace_max3_stxt "+dir+", id asc") // max3, id desc
              , "//*[@numFound='4']"
              , "//result/doc[1]/str[@name='id'][.=1]"
              , "//result/doc[2]/str[@name='id'][.=2]"
              , "//result/doc[3]/str[@name='id'][.=3]"
              , "//result/doc[4]/str[@name='id'][.=4]"
              );
    }
    assertQ(req("q", "*:*", "sort", "whitespace_max6_stxt asc, id desc") // max6 asc, id desc
            , "//*[@numFound='4']"
            , "//result/doc[1]/str[@name='id'][.=4]" // no tiebreaker needed
            , "//result/doc[2]/str[@name='id'][.=3]"
            , "//result/doc[3]/str[@name='id'][.=2]"
            , "//result/doc[4]/str[@name='id'][.=1]"
            );
    assertQ(req("q", "*:*", "sort", "whitespace_max6_stxt asc, id asc") // max6 asc, id desc
            , "//*[@numFound='4']"
            , "//result/doc[1]/str[@name='id'][.=4]" // no tiebreaker needed
            , "//result/doc[2]/str[@name='id'][.=1]"
            , "//result/doc[3]/str[@name='id'][.=2]"
            , "//result/doc[4]/str[@name='id'][.=3]"
            );
    assertQ(req("q", "*:*", "sort", "whitespace_max6_stxt desc, id desc") // max6 desc, id desc
            , "//*[@numFound='4']"
            , "//result/doc[1]/str[@name='id'][.=3]"
            , "//result/doc[2]/str[@name='id'][.=2]"
            , "//result/doc[3]/str[@name='id'][.=1]"
            , "//result/doc[4]/str[@name='id'][.=4]" // no tiebreaker needed
            );
    assertQ(req("q", "*:*", "sort", "whitespace_max6_stxt desc, id asc") // max6 desc, id desc
            , "//*[@numFound='4']"
            , "//result/doc[1]/str[@name='id'][.=1]"
            , "//result/doc[2]/str[@name='id'][.=2]"
            , "//result/doc[3]/str[@name='id'][.=3]"
            , "//result/doc[4]/str[@name='id'][.=4]" // no tiebreaker needed
            );
    
    // sanity check that the default max is working....
    assertU(adoc("id","5", "whitespace_stxt", BIG_CONST + " aaa zzz"));
    assertU(adoc("id","6", "whitespace_stxt", BIG_CONST + " bbb zzz "));
    assertU(commit());
    // for these fields, the tie breaker should be the only thing that matters, regardless of direction...
    for (String sortF : Arrays.asList("whitespace_stxt", "whitespace_nois_stxt")) {
      for (String dir : Arrays.asList("asc", "desc")) {
        assertQ(req("q", "whitespace_stxt:zzz", "sort", sortF + " " + dir + ", id asc")
                , "//*[@numFound='2']"
                , "//result/doc[1]/str[@name='id'][.=5]"
                , "//result/doc[2]/str[@name='id'][.=6]"
                );
        assertQ(req("q", "whitespace_stxt:zzz", "sort", sortF + " " + dir + ", id desc")
                , "//*[@numFound='2']"
                , "//result/doc[1]/str[@name='id'][.=6]"
                , "//result/doc[2]/str[@name='id'][.=5]"
                );
      }
    }
  }

  /**
   * test how various permutations of useDocValuesAsStored and maxCharsForDocValues interact
   */
  public void testUseDocValuesAsStored() throws Exception {
    ignoreException("when useDocValuesAsStored=true \\(length=");
    
    // first things first...
    // unlike most field types, SortableTextField should default to useDocValuesAsStored==false
    // (check a handful that should have the default behavior)
    for (String n : Arrays.asList("keyword_stxt", "whitespace_max0_stxt", "whitespace_max6_stxt")) {
      {
        FieldType ft = h.getCore().getLatestSchema().getFieldTypeByName(n);
        assertEquals("type " + ft.getTypeName() + " should not default to useDocValuesAsStored",
                     false, ft.useDocValuesAsStored()) ;
      }
      {
        SchemaField sf = h.getCore().getLatestSchema().getField(n);
        assertEquals("field " + sf.getName() + " should not default to useDocValuesAsStored",
                     false, sf.useDocValuesAsStored()) ;
      }
    }
    
    // but it should be possible to set useDocValuesAsStored=true explicitly on types...
    int num_types_found = 0;
    for (Map.Entry<String,FieldType> entry : h.getCore().getLatestSchema().getFieldTypes().entrySet()) {
      if (entry.getKey().endsWith("_has_usedvs")) {
        num_types_found++;
        FieldType ft = entry.getValue();
        assertEquals("type " + ft.getTypeName() + " has unexpected useDocValuesAsStored value",
                     true, ft.useDocValuesAsStored()) ;
      }
    }
    assertEquals("sanity check: wrong number of *_has_usedvs types found -- schema changed?",
                 2, num_types_found);

    
    // ...and it should be possible to set/override useDocValuesAsStored=true on fields...
    int num_fields_found = 0;
    List<String> xpaths = new ArrayList<>(42);
    for (Map.Entry<String,SchemaField> entry : h.getCore().getLatestSchema().getFields().entrySet()) {
      if (entry.getKey().endsWith("_usedvs")) {
        num_fields_found++;
        final SchemaField sf = entry.getValue();
        final String name = sf.getName();
        
        // some sanity check before we move on with the rest of our testing...
        assertFalse("schema change? field should not be stored=true: " + name, sf.stored());
        final boolean usedvs = name.endsWith("_has_usedvs");
        assertTrue("schema change broke assumptions: field must be '*_has_usedvs' or '*_negates_usedvs': " +
                   name, usedvs ^ name.endsWith("_negates_usedvs"));
        final boolean max6 = name.startsWith("max6_");
        assertTrue("schema change broke assumptions: field must be 'max6_*' or 'max0_*': " +
                   name, max6 ^ name.startsWith("max0_"));
        
        assertEquals("Unexpected useDocValuesAsStored value for field: " + name,
                     usedvs, sf.useDocValuesAsStored()) ;
        
        final String docid = ""+num_fields_found;
        if (usedvs && max6) {
          // if useDocValuesAsStored==true and maxCharsForDocValues=N then longer values should fail
          
          final String doc = adoc("id", docid, name, "apple pear orange");
          SolrException ex = expectThrows(SolrException.class, () -> { assertU(doc); });
          for (String expect : Arrays.asList("field " + name,
                                             "length=17",
                                             "useDocValuesAsStored=true",
                                             "maxCharsForDocValues=6")) {
            assertTrue("exception must mention " + expect + ": " + ex.getMessage(),
                       ex.getMessage().contains(expect));
          }
        } else {
          // otherwise (useDocValuesAsStored==false *OR* maxCharsForDocValues=0) any value
          // should be fine when adding a doc and we should be able to search for it later...
          final String val = docid + " apple pear orange " + BIG_CONST;
          assertU(adoc("id", docid, name, val));
          String doc_xpath = "//result/doc[str[@name='id'][.='"+docid+"']]";
            
          if (usedvs) {
            // ...and if it *does* usedvs, then we should defnitely see our value when searching...
            doc_xpath = doc_xpath + "[str[@name='"+name+"'][.='"+val+"']]";
          } else {
            // ...but if not, then we should definitely not see any value for our field...
            doc_xpath = doc_xpath + "[not(str[@name='"+name+"'])]";
          }
          xpaths.add(doc_xpath);
        }
      }
    }
    assertEquals("sanity check: wrong number of *_usedvs fields found -- schema changed?",
                 6, num_fields_found);
    
    // check all our expected docs can be found (with the expected values)
    assertU(commit());
    xpaths.add("//*[@numFound='"+xpaths.size()+"']");
    assertQ(req("q", "*:*", "fl", "*"), xpaths.toArray(new String[xpaths.size()]));
  }
    

  
  /**
   * tests that a SortableTextField using KeywordTokenzier (w/docValues) behaves exactly the same as 
   * StrFields that it's copied to for quering and sorting
   */
  public void testRandomStrEquivalentBehavior() throws Exception {
    final List<String> test_fields = Arrays.asList("keyword_stxt", "keyword_dv_stxt",
                                                   "keyword_s_dv", "keyword_s");
    // we use embedded client instead of assertQ: we want to compare the responses from multiple requests
    @SuppressWarnings("resource") final SolrClient client = new EmbeddedSolrServer(h.getCore());
    
    final int numDocs = atLeast(100);
    final int magicIdx = TestUtil.nextInt(random(), 1, numDocs);
    String magic = null;
    for (int i = 1; i <= numDocs; i++) {

      // ideally we'd test all "realistic" unicode string, but EmbeddedSolrServer uses XML request writer
      // and has no option to change this so ctrl-characters break the request
      final String val = TestUtil.randomSimpleString(random(), 100);
      if (i == magicIdx) {
        magic = val;
      }
      assertEquals(0, client.add(sdoc("id", ""+i, "keyword_stxt", val)).getStatus());
      
    }
    assertNotNull(magic);
    
    assertEquals(0, client.commit().getStatus());

    // query for magic term should match same doc regardless of field (reminder: keyword tokenizer)
    // (we need the filter in the unlikely event that magic value with randomly picked twice)
    for (String f : test_fields) {
      
      final SolrDocumentList results = client.query(params("q", "{!field f="+f+" v=$v}",
                                                           "v", magic,
                                                           "fq", "id:" + magicIdx )).getResults();
      assertEquals(f + ": Query ("+magic+") filtered by id: " + magicIdx + " ==> " + results,
                   1L, results.getNumFound());
      final SolrDocument doc = results.get(0);
      assertEquals(f + ": Query ("+magic+") filtered by id: " + magicIdx + " ==> " + doc,
                   ""+magicIdx, doc.getFieldValue("id"));
      assertEquals(f + ": Query ("+magic+") filtered by id: " + magicIdx + " ==> " + doc,
                   magic, doc.getFieldValue(f));
    }

    // do some random id range queries using all 3 fields for sorting.  results should be identical
    final int numQ = atLeast(10);
    for (int i = 0; i < numQ; i++) {
      final int hi = TestUtil.nextInt(random(), 1, numDocs-1);
      final int lo = TestUtil.nextInt(random(), 1, hi);
      final boolean fwd = random().nextBoolean();
      
      SolrDocumentList previous = null;
      String prevField = null;
      for (String f : test_fields) {
        final SolrDocumentList results = client.query(params("q","id_i:["+lo+" TO "+hi+"]",
                                                             "sort", f + (fwd ? " asc" : " desc") +
                                                             // secondary on id for determinism
                                                             ", id asc")
                                                      ).getResults();
        assertEquals(results.toString(), (1L + hi - lo), results.getNumFound());
        if (null != previous) {
          assertEquals(prevField + " vs " + f,
                       previous.getNumFound(), results.getNumFound());
          for (int d = 0; d < results.size(); d++) {
            assertEquals(prevField + " vs " + f + ": " + d,
                         previous.get(d).getFieldValue("id"),
                         results.get(d).getFieldValue("id"));
            assertEquals(prevField + " vs " + f + ": " + d,
                         previous.get(d).getFieldValue(prevField),
                         results.get(d).getFieldValue(f));
            
          }
        }
        previous = results;
        prevField = f;
      }
    }
    
  }
}
