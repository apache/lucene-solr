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

package org.apache.solr.request;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.solr.request.SimpleFacets.FacetMethod;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TrieIntField;
import org.junit.Test;

public class TestFacetMethods {

  // TODO - make these public in FieldProperties?
  protected final static int MULTIVALUED         = 0x00000200;
  protected final static int DOC_VALUES          = 0x00008000;

  @Test
  public void testNumericSingleValuedDV() {

    SchemaField field = new SchemaField("field", new TrieIntField(), DOC_VALUES, null);

    // default is FCS, can't use ENUM due to trie-field terms, FC rewrites to FCS for efficiency

    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, 1));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 1));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 1));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 1));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 1));

  }

  @Test
  public void testNumericMultiValuedDV() {

    SchemaField field = new SchemaField("field", new TrieIntField(), DOC_VALUES ^ MULTIVALUED, null);

    // default is FC, can't use ENUM due to trie-field terms, can't use FCS because of multivalues

    // default value is FC
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 0));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 1));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 1));

  }

  @Test
  public void testNumericSingleValuedNoDV() {

    SchemaField field = new SchemaField("field", new TrieIntField(), 0, null);

    // only works with FCS for mincount = 0, UIF for count > 0 is fine

    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, 1));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 1));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 1));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 1));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 1));

  }

  @Test
  public void testNumericMultiValuedNoDV() {

    SchemaField field = new SchemaField("field", new TrieIntField(), MULTIVALUED, null);

    // only works with FC for mincount = 0, UIF for count > 1 is fine

    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 1));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 1));

  }

  @Test
  public void testTextSingleValuedDV() {

    SchemaField field = new SchemaField("field", new StrField(), DOC_VALUES, null);

    // default is FC, otherwise just uses the passed-in method

    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 1));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 1));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 1));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 1));

  }

  @Test
  public void testTextMultiValuedDV() {

    SchemaField field = new SchemaField("field", new StrField(), DOC_VALUES ^ MULTIVALUED, null);

    // default is FC, can't use FCS because of multivalues

    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 0));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 1));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 1));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 1));

  }

  @Test
  public void testTextSingleValuedNoDV() {

    SchemaField field = new SchemaField("field", new StrField(), 0, null);

    // default is FC, UIF rewrites to FCS for mincount = 0
    // TODO should it rewrite to FC instead?

    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 1));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 1));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 1));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 1));

  }

  @Test
  public void testTextMultiValuedNoDV() {

    SchemaField field = new SchemaField("field", new StrField(), MULTIVALUED, null);

    // default is FC, can't use FCS for multivalued fields, UIF rewrites to FC for mincount = 0

    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, 1));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.ENUM, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FCS, 1));
    assertEquals(SimpleFacets.FacetMethod.UIF, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.UIF, 1));
    assertEquals(SimpleFacets.FacetMethod.FC, SimpleFacets.selectFacetMethod(field, SimpleFacets.FacetMethod.FC, 1));

  }

  @Test
  public void testBooleanDefaults() {

    // BoolField defaults to ENUM

    SchemaField field = new SchemaField("field", new BoolField(), 0, null);
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, null, 1));

  }
  
  @Test
  public void testPointFields() {
    // Methods other than FCS are not currently supported for PointFields
    SchemaField field = new SchemaField("foo", new IntPointField());
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, 0));
    field = new SchemaField("fooMV", new IntPointField(), 0x00000200, "0"); //MultiValued
    assertTrue(field.multiValued());
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, 0));
    assertEquals(SimpleFacets.FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, 0));
  }

}
