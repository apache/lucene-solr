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
import org.junit.Before;
import org.junit.BeforeClass;

@SuppressCodecs({"Memory", "SimpleText"}) // see TestSortedSetSelector
public class TestMinMaxOnMultiValuedField extends SolrTestCaseJ4 {

  /** Initializes core and does some sanity checking of schema */
  @BeforeClass
  public static void beforeClass() throws Exception {

    initCore("solrconfig-functionquery.xml","schema11.xml");
    checkFields(new String[] {"i", "l", "f", "d"}, new String [] {"_p", "_ni_p"});
    checkFields(new String[] {"ti", "tl", "tf", "td"}, new String [] {"", "_dv", "_ni_dv"});
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
                     f.contains("dv") || f.endsWith("_p")
                     || Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP), sf.hasDocValues());
        assertEquals(f + " doesn't have expected index status",
                     ! f.contains("ni"), sf.indexed());
      }
    }
  }
  
  /** Deletes all docs (which may be left over from a previous test */
  @Before
  public void before() throws Exception {
    assertU(delQ("*:*"));
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

    assertU(delQ("*:*"));
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

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6709")
  public void testIntFieldCache() {
    testSimpleInt("val_tis");
  }
  
  public void testPointInt() {
    testSimpleInt("val_is_p");
    testSimpleInt("val_is_ni_p");
  }
  
  public void testIntDocValues() {
    testSimpleInt("val_tis_dv");
    testSimpleInt("val_tis_ni_dv");
  }

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6709")
  public void testLongFieldCache() {
    testSimpleLong("val_tls");
  }
  
  public void testLongDocValues() {
    testSimpleLong("val_tls_dv");
    testSimpleLong("val_tls_ni_dv");
  }
  
  public void testPointLong() {
    testSimpleLong("val_ls_p");
    testSimpleLong("val_ls_ni_p");
  }


  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6709")
  public void testFloatFieldCache() {
    testSimpleFloat("val_tfs");
  }
  
  public void testFloatDocValues() {
    testSimpleFloat("val_tfs_dv");
    testSimpleFloat("val_tfs_ni_dv");
  }
  
  public void testPointFloat() {
    testSimpleFloat("val_fs_p");
    testSimpleFloat("val_fs_ni_p");
  }
  
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6709")
  public void testDoubleFieldCache() {
    testSimpleDouble("val_tds");
  }
  
  public void testDoubleDocValues() {
    testSimpleDouble("val_tds_dv");
    testSimpleDouble("val_tds_ni_dv");
  }

  public void testPointDouble() {
    testSimpleDouble("val_ds_p");
    testSimpleDouble("val_ds_ni_p");
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
    assertQEx("no error asking for max on a str field",
              "cat_docValues",
              req("q","*:*", "fl", "field(cat_docValues,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    assertQEx("no error asking for max on a str field",
              "string",
              req("q","*:*", "fl", "field(cat_docValues,'max')"),
              SolrException.ErrorCode.BAD_REQUEST);
    
  }

  public void testRandom() throws Exception {

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
  protected void testSimpleValues(final String fieldname, final Class<?> clazz, final Comparable... vals) {
    clearIndex();
    
    assert 0 < vals.length;
    
    Comparable min = vals[0];
    Comparable max = vals[0];
    
    final String type = clazz.getName();
    final SolrInputDocument doc1 = sdoc("id", "1");
    for (Comparable v : vals) {
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
                                final Comparable negative, final Comparable positive) {
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
  
}
