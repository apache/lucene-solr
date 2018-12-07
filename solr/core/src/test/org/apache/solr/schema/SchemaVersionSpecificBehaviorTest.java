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

import org.apache.solr.SolrTestCaseJ4;


public class SchemaVersionSpecificBehaviorTest extends SolrTestCaseJ4 {

  public void testVersionBehavior() throws Exception {
    for (float v : new float[] { 1.0F, 1.1F, 1.2F, 1.3F, 1.4F, 1.5F, 1.6F }) {
      try {
        final IndexSchema schema = initCoreUsingSchemaVersion(v);
        final String ver = String.valueOf(v);

        // check defaults for fields where neither the field nor the field type
        // have any properties set on them
        for (String f : new String[] { "text", "xx_dyn_text",
                                       "bool", "xx_dyn_bool",
                                       "str", "xx_dyn_str",
                                       "int", "xx_dyn_int"}) {

          SchemaField field = schema.getField(f);

          // 1.1: multiValued default changed
          assertEquals(f + " field's multiValued is wrong for ver=" + ver,
                       (v < 1.1F), field.multiValued());

          // 1.2: omitTermFreqAndPositions default changed 
          // to true for non TextField
          assertEquals(f + " field's type has wrong omitTfP for ver=" + ver,
                       ( v < 1.2F ? false : 
                         ! (field.getType() instanceof TextField)), 
                       field.omitTermFreqAndPositions());

          // 1.4: autoGeneratePhraseQueries default changed to false
          if (field.getType() instanceof TextField) {
            TextField ft = (TextField) field.getType();
            assertEquals(f + " field's autoPhrase is wrong for ver=" + ver,
                         (v < 1.4F), ft.getAutoGeneratePhraseQueries());
          }

          // 1.5: omitNorms default changed to true for non TextField
          assertEquals(f + " field's type has wrong omitNorm for ver=" + ver,
                       ( v < 1.5F ? false : 
                         ! (field.getType() instanceof TextField)), 
                       field.omitNorms());
          
          // 1.6: useDocValuesAsStored defaults to true
          assertEquals(f + " field's type has wrong useDocValuesAsStored for ver=" + ver,
                       ( v < 1.6F ? false : true), 
                       field.useDocValuesAsStored());
          
          // uninvertable defaults to true (for now)
          assertEquals(f + " field's type has wrong uninvertable for ver=" + ver,
                       true,
                       field.isUninvertible());
        }

        // regardless of version, explicit multiValued values on field or type 
        // should be correct
        for (String f : new String[] { "multi_f", "multi_t", 
                                       "ft_multi_f", "ft_multi_t",
                                       "xx_dyn_str_multi_f",
                                       "xx_dyn_str_multi_t",
                                       "xx_dyn_str_ft_multi_f",
                                       "xx_dyn_str_ft_multi_t"  }) {

          boolean expected = f.endsWith("multi_t");
          SchemaField field = schema.getField(f);
          assertEquals(f + " field's multiValued is wrong for ver=" + ver,
                       expected, field.multiValued());

          FieldType ft = field.getType();
          if (f.contains("ft_multi")) {
            // sanity check that we really are inheriting from fieldtype
            assertEquals(f + " field's multiValued doesn't match type for ver=" + ver,
                         expected, ft.isMultiValued());
          } else {
            // for fields where the property is explicit, make sure
            // we aren't getting a false negative because someone changed the
            // schema and we're inheriting from fieldType
            assertEquals(f + " field's type has wrong multiValued is wrong for ver=" + ver,
                         (v < 1.1F), ft.isMultiValued());
          
          }
        }
        
        // regardless of version, explicit useDocValuesAsStored values on field or type 
        // should be correct
        for (String f : new String[] { "ft_intdvas_f", "ft_intdvas_t",
                                        "intdvas_f", "intdvas_t",
                                        "xx_dyn_ft_intdvas_f", "xx_dyn_ft_intdvas_f", 
                                        "xx_dyn_intdvas_f", "xx_dyn_intdvas_f"}) {

          boolean expected = f.endsWith("dvas_t");
          SchemaField field = schema.getField(f);
          assertEquals(f + " field's useDocValuesAsStored is wrong for ver=" + ver,
                       expected, field.useDocValuesAsStored());

          FieldType ft = field.getType();
          if (f.contains("ft_")) {
            // sanity check that we really are inheriting from fieldtype
            assertEquals(f + " field's omitTfP doesn't match type for ver=" + ver,
                         expected, ft.hasProperty(FieldType.USE_DOCVALUES_AS_STORED));
          } else {
            // for fields where the property is explicit, make sure
            // we aren't getting a false negative because someone changed the
            // schema and we're inheriting from fieldType
            assertEquals(f + " field's type has wrong useDocValuesAsStored for ver=" + ver,
                         ( v < 1.6F ? false : true), 
                         ft.hasProperty(FieldType.USE_DOCVALUES_AS_STORED));
          
          }
        }

        // regardless of version, explicit omitTfP values on field or type 
        // should be correct
        for (String f : new String[] { "strTfP_f", "strTfP_t", 
                                       "txtTfP_f", "txtTfP_t", 
                                       "ft_strTfP_f", "ft_strTfP_t",
                                       "ft_txtTfP_f", "ft_txtTfP_t",
                                       "xx_dyn_strTfP_f", "xx_dyn_strTfP_t",
                                       "xx_dyn_txtTfP_f", "xx_dyn_txtTfP_t",
                                       "xx_dyn_ft_strTfP_f", "xx_dyn_ft_strTfP_t",
                                       "xx_dyn_ft_txtTfP_f", "xx_dyn_ft_txtTfP_t" }) {

          boolean expected = f.endsWith("TfP_t");
          SchemaField field = schema.getField(f);
          assertEquals(f + " field's omitTfP is wrong for ver=" + ver,
                       expected, field.omitTermFreqAndPositions());

          FieldType ft = field.getType();
          if (f.contains("ft_")) {
            // sanity check that we really are inheriting from fieldtype
            assertEquals(f + " field's omitTfP doesn't match type for ver=" + ver,
                         expected, ft.hasProperty(FieldType.OMIT_TF_POSITIONS));
          } else {
            // for fields where the property is explicit, make sure
            // we aren't getting a false negative because someone changed the
            // schema and we're inheriting from fieldType
            assertEquals(f + " field's type has wrong omitTfP for ver=" + ver,
                         ( v < 1.2F ? false : 
                           ! (field.getType() instanceof TextField)), 
                         ft.hasProperty(FieldType.OMIT_TF_POSITIONS));
          
          }
        }

        // regardless of version, explicit autophrase values on type 
        // should be correct
        for (String f : new String[] { "ft_txt_phrase_f", "ft_txt_phrase_t",
                                       "xx_dyn_ft_txt_phrase_f", 
                                       "xx_dyn_ft_txt_phrase_t" }) {

          boolean expected = f.endsWith("phrase_t");
          FieldType ft = schema.getFieldType(f);
          assertTrue("broken test, assert only valid on text fields: " + f,
                     ft instanceof TextField);
          assertEquals(f + " field's autophrase is wrong for ver=" + ver,
                       expected, 
                       ((TextField)ft).getAutoGeneratePhraseQueries() );
        }
 
        // regardless of version, explicit multiValued values on field or type 
        // should be correct
        for (String f : new String[] { "strnorm_f", "strnorm_t", 
                                       "txtnorm_f", "txtnorm_t", 
                                       "ft_strnorm_f", "ft_strnorm_t",
                                       "ft_txtnorm_f", "ft_txtnorm_t",
                                       "xx_dyn_strnorm_f", "xx_dyn_strnorm_t",
                                       "xx_dyn_txtnorm_f", "xx_dyn_txtnorm_t",
                                       "xx_dyn_ft_strnorm_f", "xx_dyn_ft_strnorm_t",
                                       "xx_dyn_ft_txtnorm_f", "xx_dyn_ft_txtnorm_t" }) {

          boolean expected = f.endsWith("norm_t");
          SchemaField field = schema.getField(f);
          assertEquals(f + " field's omitNorm is wrong for ver=" + ver,
                       expected, field.omitNorms());

          FieldType ft = field.getType();
          if (f.contains("ft_")) {
            // sanity check that we really are inheriting from fieldtype
            assertEquals(f + " field's omitNorm doesn't match type for ver=" + ver,
                         expected, ft.hasProperty(FieldType.OMIT_NORMS));
          } else {
            // for fields where the property is explicit, make sure
            // we aren't getting a false negative because someone changed the
            // schema and we're inheriting from fieldType
            assertEquals(f + " field's type has wrong omitNorm for ver=" + ver,
                         ( v < 1.5F ? false : 
                           ! (field.getType() instanceof TextField)), 
                         ft.hasProperty(FieldType.OMIT_NORMS));
          
          }
        }
         
      } finally {
        deleteCore();
      }
    }
  }

  public IndexSchema initCoreUsingSchemaVersion(final float ver) 
    throws Exception {

    try {
      System.setProperty("solr.schema.test.ver", String.valueOf(ver));
      initCore( "solrconfig-basic.xml", "schema-behavior.xml" );
      IndexSchema s = h.getCore().getLatestSchema();
      assertEquals("Schema version not set correctly",
                   String.valueOf(ver),
                   String.valueOf(s.getVersion()));
      return s;
    } finally {
      System.clearProperty("solr.schema.test.ver");
    }
  }

}
