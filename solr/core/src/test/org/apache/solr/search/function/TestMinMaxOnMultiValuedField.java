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
package org.apache.solr.search.function;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.DoubleValueFieldType;
import org.apache.solr.schema.FloatValueFieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IntValueFieldType;
import org.apache.solr.schema.LongValueFieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Tests the behavior of <code>field(foo,min|max)</code> on numerious types of multivalued 'foo' fields,
 * as well as the beahvior of sorting on <code>foo asc|desc</code> to implicitly choose the min|max.
 */
@SuppressCodecs({"SimpleText"}) // see TestSortedSetSelector
public class TestMinMaxOnMultiValuedField extends SolrTestCaseJ4 {

  /** Initializes core and does some sanity checking of schema */
  @BeforeClass
  public static void beforeClass() throws Exception {

    initCore("solrconfig-functionquery.xml","schema11.xml");
    checkFields(new String[] {"i", "l", "f", "d"}, new String [] {"_p", "_ni_p"});
    checkFields(new String[] {"ti", "tl", "tf", "td"}, new String [] {"", "_dv", "_ni_dv"});
    checkFields(new String[] {"str", // no expectation on missing first/last
                              "str_missf_", "str_missl_",
                              "int_missf_", "int_missl_",
                              "long_missf_", "long_missl_",
                              "float_missf_", "float_missl_",
                              "double_missf_", "double_missl_",
                              "date_missf_", "date_missl_",
                              "enum_missf_", "enum_missl_",
                              "bool_missf_", "bool_missl_"  }, new String [] {"_dv"});
    checkFields(new String[] {"stxt_", // no expectation on missing first/last
                              "stxt_missf_", "stxt_missl_" }, new String [] { "_dv"});
    checkFields(new String [] { "stxt_" }, // no expectation on missing first/last
                new String [] { "_nodv", "_dv" });
    checkFields(new String [] { "stxt_missf_", "stxt_missl_" }, new String [] { "_dv"});
      
  }
  
  private static void checkFields(String[] types, String[] suffixes) {
    // sanity check the expected properties of our fields (ie: who broke the schema?)
    IndexSchema schema = h.getCore().getLatestSchema();
    for (String type : types) {
      for (String suffix : suffixes) {
        String f = "val_" + type + "s" + suffix;
        SchemaField sf = schema.getField(f);
        assertTrue(f + " is not multivalued", sf.multiValued());
        assertEquals(f + " doesn't have expected docValues status",
                     ((f.contains("dv") || f.endsWith("_p") || Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP))
                      && !f.contains("nodv")),
                     sf.hasDocValues());
        assertEquals(f + " doesn't have expected index status",
                     ! f.contains("ni"), sf.indexed());

        if (f.contains("miss")) {
          // if name contains "miss" assert that the missing first/last props match
          // but don't make any asserts about fields w/o that in name
          // (schema11.xml's strings has some preexisting silliness that don't affect us)
          assertEquals(f + " sortMissingFirst is wrong",
                       f.contains("missf"), sf.sortMissingFirst());
          assertEquals(f + " sortMissingLast is wrong",
                       f.contains("missl"), sf.sortMissingLast());
        }
      }
    }
  }
  
  /** Deletes all docs (which may be left over from a previous test */
  @Before
  public void before() throws Exception {
    clearIndex();
    assertU(commit());
  }
  
  public void testBasics() throws Exception {
    testBasics("val_tis_dv", "val_tls_dv", "val_tfs_dv", "val_tds_dv");
    testBasics("val_tis_ni_dv", "val_tls_ni_dv", "val_tfs_ni_dv", "val_tds_ni_dv");
    testBasics("val_is_p", "val_ls_p", "val_fs_p", "val_ds_p");
    testBasics("val_is_ni_p", "val_ls_ni_p", "val_fs_ni_p", "val_ds_ni_p");
  }

  private void testBasics(String intField, String longField, String floatField, String doubleField) throws Exception {
    assertTrue("Unexpected int field", h.getCore().getLatestSchema().getField(intField).getType() instanceof IntValueFieldType);
    assertTrue("Unexpected long field", h.getCore().getLatestSchema().getField(longField).getType() instanceof LongValueFieldType);
    assertTrue("Unexpected float field", h.getCore().getLatestSchema().getField(floatField).getType() instanceof FloatValueFieldType);
    assertTrue("Unexpected double field", h.getCore().getLatestSchema().getField(doubleField).getType() instanceof DoubleValueFieldType);

    clearIndex();
    assertU(adoc(sdoc("id", "1"
                      // int
                      ,intField, "42"
                      ,intField, "9"
                      ,intField, "-54"
                      // long
                      ,longField, "420"
                      ,longField, "90"
                      ,longField, "-540"
                      // float
                      ,floatField, "-42.5"
                      ,floatField, "-4.5"
                      ,floatField, "-13.5"
                      // double
                      ,doubleField, "-420.5"
                      ,doubleField, "-40.5"
                      ,doubleField, "-130.5"
                      )));
    assertU(commit());

    assertQ(req("q","id:1"
                // int
                ,"fl","exists_min_i:exists(field(" + intField + ",min))"
                ,"fl","exists_max_i:exists(field(" + intField + ",max))"
                ,"fl","min_i:field(" + intField + ",min)"
                ,"fl","max_i:field(" + intField + ",max)"
                // long
                ,"fl","exists_min_l:exists(field(" + longField + ",min))"
                ,"fl","exists_max_l:exists(field(" + longField + ",max))"
                ,"fl","min_l:field(" + longField + ",min)"
                ,"fl","max_l:field(" + longField + ",max)"
                // float
                ,"fl","exists_min_f:exists(field(" + floatField + ",min))"
                ,"fl","exists_max_f:exists(field(" + floatField + ",max))"
                ,"fl","min_f:field(" + floatField + ",min)"
                ,"fl","max_f:field(" + floatField + ",max)"
                // double
                ,"fl","exists_min_d:exists(field(" + doubleField + ",min))"
                ,"fl","exists_max_d:exists(field(" + doubleField + ",max))"
                ,"fl","min_d:field(" + doubleField + ",min)"
                ,"fl","max_d:field(" + doubleField + ",max)"
                
                )
            ,"//*[@numFound='1']"
            // int
            ,"//bool[@name='exists_min_i']='true'"
            ,"//bool[@name='exists_max_i']='true'"
            ,"//int[@name='min_i']='-54'"
            ,"//int[@name='max_i']='42'"
            // long
            ,"//bool[@name='exists_min_l']='true'"
            ,"//bool[@name='exists_max_l']='true'"
            ,"//long[@name='min_l']='-540'"
            ,"//long[@name='max_l']='420'"
            // float
            ,"//bool[@name='exists_min_f']='true'"
            ,"//bool[@name='exists_max_f']='true'"
            ,"//float[@name='min_f']='-42.5'"
            ,"//float[@name='max_f']='-4.5'"
            // double
            ,"//bool[@name='exists_min_d']='true'"
            ,"//bool[@name='exists_max_d']='true'"
            ,"//double[@name='min_d']='-420.5'"
            ,"//double[@name='max_d']='-40.5'"
            );
  }

  public void testBasicStrings() {
    checkBasicStrings("val_strs_dv");
  }
  public void testBasicSortableText() {
    checkBasicStrings("val_stxt_s_dv");
    checkBasicStrings("val_stxt_missf_s_dv");
    checkBasicStrings("val_stxt_missl_s_dv");
  }
  private void checkBasicStrings(final String field) {
    assertU(adoc(sdoc("id", "1",
                      field, "dog",
                      field, "xyz",
                      field, "cat")));
    assertU(adoc(sdoc("id", "2"))); // 2 has no values in tested field
    assertU(commit());

    // id=1: has values
    assertQ(req("q","id:1"
                ,"fl","exists_min_str:exists(field("+field+",min))"
                ,"fl","exists_max_str:exists(field("+field+",max))"
                ,"fl","min_str:field("+field+",min)"
                ,"fl","max_str:field("+field+",max)"
                
                )
            ,"//*[@numFound='1']"
            ,"//bool[@name='exists_min_str']='true'"
            ,"//bool[@name='exists_max_str']='true'"
            ,"//str[@name='min_str']='cat'"
            ,"//str[@name='max_str']='xyz'"
            );
    // id=2: no values
    assertQ(req("q","id:2"
                ,"fl","exists_min_str:exists(field("+field+",min))"
                ,"fl","exists_max_str:exists(field("+field+",max))"
                ,"fl","min_str:field("+field+",min)"
                ,"fl","max_str:field("+field+",max)"
                
                )
            ,"//*[@numFound='1']"
            ,"//bool[@name='exists_min_str']='false'"
            ,"//bool[@name='exists_max_str']='false'"
            ,"count(//*[@name='min_str'])=0"
            ,"count(//*[@name='max_str'])=0"
            );
  }

  public void testExpectedSortOrderingStrings() {
    testExpectedSortOrdering("val_strs_dv", false,
                             null, "a", "cat", "dog", "wako", "xyz", "zzzzz");
  }
  public void testExpectedSortOrderingSortableText() {
    testExpectedSortOrdering("val_stxt_s_dv", false,
                             null, "a", "cat", "dog", "wako", "xyz", "zzzzz");
  }

  public void testExpectedSortMissingOrderings() {

    // NOTE: we never test the "true" min/max value for a type, because
    // (in this simple test) we aren't using a secondary sort, so there is no way to disambiguate
    // docs that have those values from docs that have those *effective* sort values

    testSortMissingMinMax("val_str",  "a", "aaaaaa", "xxxxx", "zzzzzzzzzzzzzzzzzzz");
    testSortMissingMinMax("val_stxt", "a", "aaaaaa", "xxxxx", "zzzzzzzzzzzzzzzzzzz");
    
    testSortMissingMinMax("val_int",
                          Integer.MIN_VALUE+1L, -9999, 0, 99999, Integer.MAX_VALUE-1L);
    testSortMissingMinMax("val_long",
                          Long.MIN_VALUE+1L, -99999999L, 0, 9999999999L, Long.MAX_VALUE-1L);
    testSortMissingMinMax("val_float",
                          Math.nextAfter(Float.NEGATIVE_INFINITY, 0F), -99.99F,
                          0F, 99.99F, Math.nextAfter(Float.POSITIVE_INFINITY, 0F));
    testSortMissingMinMax("val_double",
                          Math.nextAfter(Double.NEGATIVE_INFINITY, 0D), -99.99D, 
                          0D, 99.99D, Math.nextAfter(Double.POSITIVE_INFINITY, 0F));
    testSortMissingMinMax("val_date",
                          "-1000000-01-01T00:00:00Z", "NOW-1YEAR", "NOW", "NOW+1YEAR", "+1000000-01-01T00:00:00Z");
    testSortMissingMinMax("val_bool", false, true);
    testSortMissingMinMax("val_enum", "Not Available", "Low", "High", "Critical");

  }
  

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6709")
  public void testIntFieldCache() {
    testSimpleInt("val_tis");
    testExpectedSortOrderingInt("val_tis", true);
  }
  
  public void testPointInt() {
    testSimpleInt("val_is_p");
    testSimpleInt("val_is_ni_p");
    
    testExpectedSortOrderingInt("val_is_p", false);
    testExpectedSortOrderingInt("val_is_ni_p", false);
  }
  
  public void testIntDocValues() {
    testSimpleInt("val_tis_dv");
    testSimpleInt("val_tis_ni_dv");
    
    testExpectedSortOrderingInt("val_tis_dv", true);
    testExpectedSortOrderingInt("val_tis_ni_dv", true);
  }

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6709")
  public void testLongFieldCache() {
    testSimpleLong("val_tls");
    testExpectedSortOrderingLong("val_tls", true);
  }
  
  public void testLongDocValues() {
    testSimpleLong("val_tls_dv");
    testSimpleLong("val_tls_ni_dv");
    
    testExpectedSortOrderingLong("val_tls_dv", true);
    testExpectedSortOrderingLong("val_tls_ni_dv", true);
  }
  
  public void testPointLong() {
    testSimpleLong("val_ls_p");
    testSimpleLong("val_ls_ni_p");
    
    testExpectedSortOrderingLong("val_ls_p", false);
    testExpectedSortOrderingLong("val_ls_ni_p", false);
  }


  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6709")
  public void testFloatFieldCache() {
    testSimpleFloat("val_tfs");
    testExpectedSortOrderingFloat("val_tfs", true);
  }
  
  public void testFloatDocValues() {
    testSimpleFloat("val_tfs_dv");
    testSimpleFloat("val_tfs_ni_dv");
    
    testExpectedSortOrderingFloat("val_tfs_dv", true);
    testExpectedSortOrderingFloat("val_tfs_ni_dv", true);
  }
  
  public void testPointFloat() {
    testSimpleFloat("val_fs_p");
    testSimpleFloat("val_fs_ni_p");
    
    testExpectedSortOrderingFloat("val_fs_p", false);
    testExpectedSortOrderingFloat("val_fs_ni_p", false);
  }
  
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6709")
  public void testDoubleFieldCache() {
    testSimpleDouble("val_tds");
    
    testExpectedSortOrderingDouble("val_tds", true);
  }
  
  public void testDoubleDocValues() {
    testSimpleDouble("val_tds_dv");
    testSimpleDouble("val_tds_ni_dv");
    
    testExpectedSortOrderingDouble("val_tds_dv", true);
    testExpectedSortOrderingDouble("val_tds_ni_dv", true);
  }

  public void testPointDouble() {
    testSimpleDouble("val_ds_p");
    testSimpleDouble("val_ds_ni_p");
    
    testExpectedSortOrderingDouble("val_ds_p", false);
    testExpectedSortOrderingDouble("val_ds_ni_p", false);
  }

  public void testBadRequests() {

    // useful error msg when bogus selector is requested (ie: not min or max)
    assertQEx("no error asking for bogus selector",
              "hoss",
              req("q","*:*", "fl", "field(val_tds_dv,'hoss')"),
              SolrException.ErrorCode.BAD_REQUEST);

    assertQEx("no error asking for bogus selector",
        "hoss",
        req("q","*:*", "fl", "field(val_ds_p,'hoss')"),
        SolrException.ErrorCode.BAD_REQUEST);
    
    // useful error until/unless LUCENE-6709
    assertFalse(h.getCore().getLatestSchema().getField("val_is_ndv_p").hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField("val_is_ndv_p").multiValued());
    assertQEx("no error asking for max on a non docVals field",
              "val_is_ndv_p",
              req("q","*:*", "fl", "field(val_is_ndv_p,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    assertQEx("no error asking for max on a non docVals field",
              "max",
              req("q","*:*", "fl", "field(val_is_ndv_p,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    assertQEx("no error asking for max on a non docVals field",
              "docValues",
              req("q","*:*", "fl", "field(val_is_ndv_p,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    
    // useful error if min/max is unsupported for fieldtype
    assertQEx("no error mentioning field name when asking for max on type that doesn't support it",
              "cat_length",
              req("q","*:*", "fl", "field(cat_length,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    assertQEx("no error mentioning type when asking for max on type that doesn't support it",
              "text_length",
              req("q","*:*", "fl", "field(cat_length,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    // type supports, but field doesn't have docValues
    assertQEx("no error mentioning field name when asking for max on a non-dv str field",
              "cat",
              req("q","*:*", "fl", "field(cat,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    assertQEx("no error mentioning 'docValues' when asking for max on a non-dv str field",
              "docValues",
              req("q","*:*", "fl", "field(cat,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    assertQEx("no error mentioning field name when asking for max on a non-dv sortable text field",
              "val_stxt_s_nodv",
              req("q","*:*", "fl", "field(val_stxt_s_nodv,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    assertQEx("no error mentioning 'docValues' when asking for max on a non-dv sortable field",
              "docValues",
              req("q","*:*", "fl", "field(val_stxt_s_nodv,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);

    
  }

  public void testRandom() throws Exception {

    @SuppressWarnings({"rawtypes"})
    Comparable[] vals = new Comparable[TestUtil.nextInt(random(), 1, 17)];

    // random ints
    for (int i = 0; i < vals.length; i++) {
      vals[i] = random().nextInt();
    }
    testSimpleValues("val_tis_dv", int.class, vals);
    testSimpleValues("val_is_p", int.class, vals);
    testSimpleValues("val_tis_ni_dv", int.class, vals);
    testSimpleValues("val_is_ni_p", int.class, vals);

    // random longs
    for (int i = 0; i < vals.length; i++) {
      vals[i] = random().nextLong();
    }
    testSimpleValues("val_tls_dv", long.class, vals);
    testSimpleValues("val_ls_p", long.class, vals);
    testSimpleValues("val_tls_ni_dv", long.class, vals);
    testSimpleValues("val_ls_ni_p", long.class, vals);
    
    // random floats
    for (int i = 0; i < vals.length; i++) {
      // Random.nextFloat is lame
      Float f = Float.NaN;
      while (f.isNaN()) {
        f = Float.intBitsToFloat(random().nextInt());
      }
      vals[i] = f;
    }
    testSimpleValues("val_tfs_dv", float.class, vals);
    testSimpleValues("val_fs_p", float.class, vals);
    testSimpleValues("val_tfs_ni_dv", float.class, vals);
    testSimpleValues("val_fs_ni_p", float.class, vals);
    
    // random doubles
    for (int i = 0; i < vals.length; i++) {
      // Random.nextDouble is lame
      Double d = Double.NaN;
      while (d.isNaN()) {
        d = Double.longBitsToDouble(random().nextLong());
      }
      vals[i] = d;
    }
    testSimpleValues("val_tds_dv", double.class, vals);
    testSimpleValues("val_ds_p", double.class, vals);
    testSimpleValues("val_tds_ni_dv", double.class, vals);
    testSimpleValues("val_ds_ni_p", double.class, vals);

  }

  
  /** @see #testSimpleValues */
  protected void testSimpleInt(final String fieldname) {
    // most basic case
    testSimpleValues(fieldname, int.class, 0);
    
    // order of values shouldn't matter
    testSimpleValues(fieldname, int.class, -42, 51, 3);
    testSimpleValues(fieldname, int.class, 51, 3, -42);

    // extreme's of the data type
    testSimpleValues(fieldname, int.class, Integer.MIN_VALUE, 42, -550);
    testSimpleValues(fieldname, int.class, Integer.MAX_VALUE, 0, Integer.MIN_VALUE);

    testSimpleSort(fieldname, -42, 666);
  }
  
  /** @see #testSimpleValues */
  protected void testSimpleLong(final String fieldname) {
    // most basic case
    testSimpleValues(fieldname, long.class, 0);
    
    // order of values shouldn't matter
    testSimpleValues(fieldname, long.class, -42L, 51L, 3L);
    testSimpleValues(fieldname, long.class, 51L, 3L, -42L);

    // extreme's of the data type
    testSimpleValues(fieldname, long.class, Long.MIN_VALUE, 42L, -550L);
    testSimpleValues(fieldname, long.class, Long.MAX_VALUE, 0L, Long.MIN_VALUE);
    
    testSimpleSort(fieldname, -42, 666);
  }
  
  /** @see #testSimpleValues */
  protected void testSimpleFloat(final String fieldname) {
    // most basic case
    testSimpleValues(fieldname, float.class, 0.0F);
    
    // order of values shouldn't matter
    testSimpleValues(fieldname, float.class, -42.5F, 51.3F, 3.1415F);
    testSimpleValues(fieldname, float.class, 51.3F, 3.1415F, -42.5F);

    // extreme's of the data type
    testSimpleValues(fieldname, float.class, Float.NEGATIVE_INFINITY, 42.5F, -550.4F);
    testSimpleValues(fieldname, float.class, Float.POSITIVE_INFINITY, 0.0F, Float.NEGATIVE_INFINITY);
    
    testSimpleSort(fieldname, -4.2, 6.66);
  }
  
  /** @see #testSimpleValues */
  protected void testSimpleDouble(final String fieldname) {
    // most basic case
    testSimpleValues(fieldname, double.class, 0.0D);
    
    // order of values shouldn't matter
    testSimpleValues(fieldname, double.class, -42.5D, 51.3D, 3.1415D);
    testSimpleValues(fieldname, double.class, 51.3D, 3.1415D, -42.5D);

    // extreme's of the data type
    testSimpleValues(fieldname, double.class, Double.NEGATIVE_INFINITY, 42.5D, -550.4D);
    testSimpleValues(fieldname, double.class, Double.POSITIVE_INFINITY, 0.0D, Double.NEGATIVE_INFINITY);
    
    testSimpleSort(fieldname, -4.2, 6.66);
  }
  
  /** Tests a single doc with a few explicit values, as well as testing exists with and w/o values */
  @SuppressWarnings({"unchecked"})
  protected void testSimpleValues(final String fieldname, final Class<?> clazz,
                                  @SuppressWarnings({"rawtypes"})final Comparable... vals) {
    clearIndex();
    
    assert 0 < vals.length;
    @SuppressWarnings({"rawtypes"})
    Comparable min = vals[0];
    @SuppressWarnings({"rawtypes"})
    Comparable max = vals[0];
    
    final String type = clazz.getName();
    final SolrInputDocument doc1 = sdoc("id", "1");
    for (@SuppressWarnings({"rawtypes"})Comparable v : vals) {
      doc1.addField(fieldname, v);
      if (0 < min.compareTo(v)) {
        min = v;
      }
      if (0 > max.compareTo(v)) {
        max = v;
      }
    }
    assertU(adoc(doc1));
    assertU(adoc(sdoc("id", "2"))); // fieldname doesn't exist
    assertU(commit());

    // doc with values
    assertQ(fieldname,
            req("q","id:1",
                "fl","exists_val_min:exists(field("+fieldname+",min))",
                "fl","exists_val_max:exists(field("+fieldname+",max))",
                "fl","val_min:field("+fieldname+",min)",
                "fl","val_max:field("+fieldname+",max)")
            ,"//*[@numFound='1']"
            ,"//bool[@name='exists_val_min']='true'"
            ,"//bool[@name='exists_val_max']='true'"
            ,"//"+type+"[@name='val_min']='"+min+"'"
            ,"//"+type+"[@name='val_max']='"+max+"'"
            );

    // doc w/o values
    assertQ(fieldname,
            req("q","id:2",
                "fl","exists_val_min:exists(field("+fieldname+",min))",
                "fl","exists_val_max:exists(field("+fieldname+",max))",
                "fl","val_min:field("+fieldname+",min)",
                "fl","val_max:field("+fieldname+",max)")
            ,"//*[@numFound='1']"
            ,"//bool[@name='exists_val_min']='false'"
            ,"//bool[@name='exists_val_max']='false'"
            ,"count(//"+type+"[@name='val_min'])=0"
            ,"count(//"+type+"[@name='val_max'])=0"
            );

    // sanity check no sort error when there are missing values
    for (String dir : new String[] { "asc", "desc" }) {
      for (String mm : new String[] { "min", "max" }) {
        for (String func : new String[] { "field("+fieldname+","+mm+")",
                                          "def(field("+fieldname+","+mm+"),42)",
                                          "sum(32,field("+fieldname+","+mm+"))"  }) {
          assertQ(fieldname,
                  req("q","*:*", 
                      "fl", "id",
                      "sort", func + " " + dir)
                  ,"//*[@numFound='2']"
                  // no assumptions about order for now, see bug: SOLR-8005
                  ,"//str[@name='id']='1'"
                  ,"//str[@name='id']='2'"
                  );
        }
      }
    }
  }

  /** 
   * Tests sort order of min/max realtive to other docs w/o any values.
   * @param fieldname The field to test
   * @param negative a "negative" value for this field (ie: in a function context, is less then the "0")
   * @param positive a "positive" value for this field (ie: in a function context, is more then the "0")
   */
  protected void testSimpleSort(final String fieldname,
                                @SuppressWarnings({"rawtypes"})final Comparable negative,
                                @SuppressWarnings({"rawtypes"})final Comparable positive) {
    clearIndex();

    int numDocsExpected = 1;
    for (int i = 1; i < 4; i++) { // pos docids
      if (random().nextBoolean()) {
        assertU(adoc(sdoc("id",i))); // fieldname doesn't exist
        numDocsExpected++;
      }
    }
    
    assertU(adoc(sdoc("id", "0",
                      fieldname, negative,
                      fieldname, positive)));
    
    for (int i = 1; i < 4; i++) { // neg docids
      if (random().nextBoolean()) {
        assertU(adoc(sdoc("id",-i))); // fieldname doesn't exist
        numDocsExpected++;
      }
    }
    assertU(commit());

    // need to wrap with "def" until SOLR-8005 is resolved
    assertDocWithValsIsFirst(numDocsExpected, "def(field("+fieldname+",min),0) asc");
    assertDocWithValsIsLast(numDocsExpected,  "def(field("+fieldname+",min),0) desc");
    
    assertDocWithValsIsFirst(numDocsExpected, "def(field("+fieldname+",max),0) desc");
    assertDocWithValsIsLast(numDocsExpected,  "def(field("+fieldname+",max),0) asc");

    // def wrapper shouldn't be needed since it's already part of another function
    assertDocWithValsIsFirst(numDocsExpected, "sum(32,field("+fieldname+",max)) desc");
    assertDocWithValsIsLast(numDocsExpected,  "sum(32,field("+fieldname+",max)) asc");
    
    assertDocWithValsIsFirst(numDocsExpected, "sum(32,field("+fieldname+",min)) asc");
    assertDocWithValsIsLast(numDocsExpected,  "sum(32,field("+fieldname+",min)) desc");
  }

  /** helper for testSimpleSort */
  private static void assertDocWithValsIsFirst(final int numDocs, final String sort) {
    assertQ(sort,
            req("q","*:*", "rows", ""+numDocs, "sort", sort)
            ,"//result[@numFound='"+numDocs+"']"
            ,"//result/doc[1]/str[@name='id']='0'"
            );
  }
  /** helper for testSimpleSort */
  private static void assertDocWithValsIsLast(final int numDocs, final String sort) {
    assertQ(sort,
            req("q","*:*", "rows", ""+numDocs, "sort", sort)
            ,"//result[@numFound='"+numDocs+"']"
            ,"//result/doc["+numDocs+"]/str[@name='id']='0'"
            );
  }

  /** @see #testExpectedSortOrdering */
  private void testExpectedSortOrderingInt(final String f, final boolean trieFieldHack) {
    // first a quick test where every doc has a value
    testExpectedSortOrdering(f, trieFieldHack,
                             Integer.MIN_VALUE, -9999, 0, 1000, Integer.MAX_VALUE);

    // now where one doc has no values
    testExpectedSortOrdering(f, trieFieldHack,
                             Integer.MIN_VALUE, -9999, -42, -15, -3,
                             null, 7, 53, 1000, 121212112, Integer.MAX_VALUE);
  }
  
  /** @see #testExpectedSortOrdering */
  private void testExpectedSortOrderingLong(final String f, final boolean trieFieldHack) {
    // first a quick test where every doc has a value
    testExpectedSortOrdering(f, trieFieldHack,
                             Long.MIN_VALUE, -4200L, 0, 121212112, Long.MAX_VALUE);

    // now where one doc has no values
    testExpectedSortOrdering(f, trieFieldHack,
                             Long.MIN_VALUE, ((long)Integer.MIN_VALUE)-1L, -4200L,
                             -150L, -3L, null, 70L, 530L, 121212112,
                             1L+(long)Integer.MAX_VALUE, Long.MAX_VALUE);
                                           
  }
  
  /** @see #testExpectedSortOrdering */
  private void testExpectedSortOrderingFloat(final String f, final boolean trieFieldHack) {
    // first a quick test where every doc has a value
    testExpectedSortOrdering(f, trieFieldHack,
                             Float.NEGATIVE_INFINITY, -15.0, 0F, 121212.112, Float.POSITIVE_INFINITY);

    // now where one doc has no values
    testExpectedSortOrdering(f, trieFieldHack,
                             Float.NEGATIVE_INFINITY, -9999.999, -42.3, -15.0, -0.3,
                             null, 0.7, 5.3, 1000, 121212.112, Float.POSITIVE_INFINITY);
                             
  }
  
  /** @see #testExpectedSortOrdering */
  private void testExpectedSortOrderingDouble(final String f, final boolean trieFieldHack) {
    // first a quick test where every doc has a value
    testExpectedSortOrdering(f, trieFieldHack,
                             Double.NEGATIVE_INFINITY, -9999.999D,
                             0D, 121212.112D, Double.POSITIVE_INFINITY);

    // now where one doc has no values
    testExpectedSortOrdering(f, trieFieldHack,
                             Double.NEGATIVE_INFINITY, -9999.999D, -42.3D, -15.0D, -0.3D,
                             null, 0.7D, 5.3D, 1000, 121212.112D, Double.POSITIVE_INFINITY);
  }

  /**
   * Given a <code>fieldPrefix</code> and a list of sorted values which may <em>not</em> contain null, this method tests that sortMissingLast and sortMissingFirst fields using those prefixes sort correctly when {@link #buildMultiValueSortedDocuments} is used to generate documents containing these values <em>and</em> an additional document with no values in the field.
   *
   * <p>
   * Permutations tested:
   * </p>
   * <ul>
   *  <li><code>fieldPrefix</code> + <code>"_missf_s_dv"</code> asc</li>
   *  <li><code>fieldPrefix</code> + <code>"_missf_s_dv"</code> desc</li>
   *  <li><code>fieldPrefix</code> + <code>"_missl_s_dv"</code> asc</li>
   *  <li><code>fieldPrefix</code> + <code>"_missl_s_dv"</code> desc</li>
   * </ul>
   *
   * @see #buildMultiValueSortedDocuments
   * @see #testExpectedSortOrdering(String,List)
   */
  private void testSortMissingMinMax(final String fieldPrefix,
                                     Object... sortedValues) {

    for (Object obj : sortedValues) { // sanity check
      assertNotNull("this helper method can't be used with 'null' values", obj);
    }
    
    for (String suffix : Arrays.asList("_missf_s_dv", "_missl_s_dv")) {

      final String f = fieldPrefix + suffix;
      final boolean first = f.contains("missf");
    
      final List<Object> asc_vals = new ArrayList<>(sortedValues.length + 1);
      Collections.addAll(asc_vals, sortedValues);
      final List<Object> desc_vals = new ArrayList<>(sortedValues.length + 1);
      Collections.addAll(desc_vals, sortedValues);
      Collections.reverse(desc_vals);
      
      asc_vals.add(first ? 0 : sortedValues.length, null);
      desc_vals.add(first ? 0 : sortedValues.length, null);
      
      testExpectedSortOrdering(f + " asc", buildMultiValueSortedDocuments(f, asc_vals));
      testExpectedSortOrdering(f + " desc", buildMultiValueSortedDocuments(f, desc_vals));
    }
  }

  /**
   * Given a (multivalued) field name and an (ascending) sorted list of values, this method uses {@link #buildMultiValueSortedDocuments} to generate and test multiple function &amp; sort permutations ...
   * <ul>
   *  <li><code>f asc</code> (implicitly min)</li>
   *  <li><code>field(f,min) asc</code></li>
   *  <li><code>field(f,min) desc</code></li>
   *  <li><code>f desc</code> (implicitly max)</li>
   *  <li><code>field(f,max) desc</code></li>
   *  <li><code>field(f,max) asc</code></li>
   * </ul>
   *
   * <p>
   * <b>NOTE:</b> if the sortedValues includes "null" then the field must <em>NOT</em> use <code>sortMissingFirst</code> or <code>sortMissingLast</code></b>
   * </p>
   *
   * @param f the field to test
   * @param trieFieldHack if this param and {@link #NUMERIC_POINTS_SYSPROP} are both true, then the <code>field(f,min|max)</code> functions will be wrapped in <code>def(...,0)</code> and the implicit <code>f asc|desc</code> syntax will not be tested -- see SOLR-8005 for the reason.
   * @param sortedValues the values to use when building the docs and validating the sort
   *
   * @see #buildMultiValueSortedDocuments
   * @see #testExpectedSortOrdering(String,List)
   * @see #clearIndex
   */
  private void testExpectedSortOrdering(final String f, boolean trieFieldHack,
                                        Object... sortedValues) {

    SchemaField sf = h.getCore().getLatestSchema().getField(f);
    assertFalse("this utility method does not work with fields that are sortMissingFirst|Last: " + f,
                sf.sortMissingFirst() || sf.sortMissingLast());
    
    // make a copy we can re-order later
    final List<Object> vals = new ArrayList<Object>(sortedValues.length);
    Collections.addAll(vals, sortedValues);
      
    String minFunc = "field("+f+",min)";
    String maxFunc = "field("+f+",max)";

    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) {
      // we don't need to mess with this hack at all if we're using all point numerics
      trieFieldHack = false;
    }

    if (trieFieldHack // SOLR-8005
        // if this line of code stops compiling, then trie fields have been removed from solr
        // and the entire trieFieldHack param should be removed from this method (and callers)
        && null != TrieField.class) {
      
      // the SOLR-8005 hack is only needed if/when a doc has no value...
      trieFieldHack = false; // assume we're safe
      for (Object val : vals) {
        if (null == val) { // we're not safe
          trieFieldHack = true;
          break;
        }
      }
    }
    if (trieFieldHack) {
      // if we've made it this far, and we still need the hack, we have to wrap our
      // functions with a default...
      minFunc = "def(" + minFunc + ",0)";
      maxFunc = "def(" + maxFunc + ",0)";
      // and we can't test implicit min/max default behavior...
    }
    
    // // // // min
    
    final List<SolrInputDocument> min_asc = buildMultiValueSortedDocuments(f, vals);
    
    // explicit min + asc
    testExpectedSortOrdering(minFunc + " asc", min_asc);
    // implicit: asc -> min
    if (!trieFieldHack) testExpectedSortOrdering(f + " asc", min_asc);
    
    final List<SolrInputDocument> min_desc = new ArrayList<>(min_asc);
    Collections.reverse(min_desc);
    
    // explicit min + desc
    testExpectedSortOrdering(minFunc + " desc", min_desc);

    // // // // max
    Collections.reverse(vals);
    
    final List<SolrInputDocument> max_desc = buildMultiValueSortedDocuments(f, vals);

    // explicit: max + desc
    testExpectedSortOrdering(maxFunc +" desc", max_desc);
    // implicit: desc -> max
    if (!trieFieldHack) testExpectedSortOrdering(f + " desc", max_desc); 
    
    final List<SolrInputDocument> max_asc = new ArrayList<>(max_desc);
    Collections.reverse(max_asc);
    
    // explicit max + asc
    testExpectedSortOrdering(maxFunc + " asc", max_asc);
  }
  
  /**
   * Given a sort clause, and a list of documents in sorted order, this method will clear the index 
   * and then add the documents in a random order (to ensure the index insertion order is not a factor) 
   * and then validate that a <code>*:*</code> query returns the documents in the original order.
   *
   * @see #buildMultiValueSortedDocuments
   * @see #clearIndex
   */   
  private void testExpectedSortOrdering(final String sort,
                                        final List<SolrInputDocument> sortedDocs) {
    clearIndex();

    // shuffle a copy of the doc list (to ensure index order isn't linked to uniqueKey order)
    List<SolrInputDocument> randOrderedDocs = new ArrayList<>(sortedDocs);
    Collections.shuffle(randOrderedDocs, random());

    for (SolrInputDocument doc : randOrderedDocs) {
      assertU(adoc(doc));
    }
    assertU(commit());

    // now use the original sorted docs to build up the expected sort order as a list of xpath
    List<String> xpaths = new ArrayList<>(sortedDocs.size() + 1);
    xpaths.add("//result[@numFound='"+sortedDocs.size()+"']");
    int seq = 0;
    for (SolrInputDocument doc : sortedDocs) {
      xpaths.add("//result/doc["+(++seq)+"]/str[@name='id']='"+doc.getFieldValue("id")+"'");
    }
    assertQ(req("q", "*:*", "rows", "" + sortedDocs.size(), "sort", sort),
            xpaths.toArray(new String[xpaths.size()]));
  }

  /**
   * Given a (multivalued) field name and an (ascending) sorted list of values, this method will generate a List of Solr Documents of the same size such that:
   * <ul>
   *  <li>For each non-null value in the original list, the corrisponding document in the result will have that value in the specified field.</li>
   *  <li>For each null value in the original list, the corrisponding document in teh result will have <em>NO</em> values in the specified field.</li>
   *  <li>If a document has a value in the field, then some random number of values that come <em>after</em> that value in the original list may also be included in the specified field.</li>
   *  <li>Every document in the result will have a randomly asssigned 'id', unique realitive to all other documents in the result.</li>
   * </ul>
   */
  private static final List<SolrInputDocument> buildMultiValueSortedDocuments(final String f,
                                                                              final List<Object> vals) {
    // build a list of docIds that we can shuffle (so the id order doesn't match the value order)
    List<Integer> ids = new ArrayList<>(vals.size());
    for (int i = 0; i < vals.size(); i++) {
      ids.add(i+1);
    }
    Collections.shuffle(ids, random());
    
    final List<SolrInputDocument> docs = new ArrayList<>(vals.size());
    for (int i = 0; i < vals.size(); i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", ids.get(i));
      Object primaryValue = vals.get(i);
      if (null != primaryValue) {
        doc.addField(f, primaryValue);
        final int extraValCount = random().nextInt(vals.size() - i);
        for (int j = 0; j < extraValCount; j++) {
          Object extraVal = vals.get(TestUtil.nextInt(random(), i+1, vals.size() - 1));
          if (null != extraVal) {
            doc.addField(f, extraVal);
          }
        }
      }
      docs.add(doc);
    }
    return docs;
  }
}
