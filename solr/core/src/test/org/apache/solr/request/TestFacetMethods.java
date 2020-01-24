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

import java.util.Arrays;

import org.apache.solr.SolrTestCase;
import org.apache.solr.request.SimpleFacets.FacetMethod;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TrieIntField;
import org.junit.Test;

public class TestFacetMethods extends SolrTestCase {

  // TODO - make these public in FieldProperties?
  protected final static int MULTIVALUED         = 0x00000200;
  protected final static int DOC_VALUES          = 0x00008000;
  protected final static int UNINVERTIBLE        = 0b10000000000000000000;

  protected static boolean propsMatch( int x, int y ) {
    return (x & y) != 0;
  }

  @Test
  public void testNumericSingleValuedDV() {

    for (int props : Arrays.asList(DOC_VALUES ^ UNINVERTIBLE,
                                   DOC_VALUES)) {
      SchemaField field = new SchemaField("field", new TrieIntField(), props, null);
      // default is FCS, can't use ENUM due to trie-field terms, FC rewrites to FCS for efficiency
      for (int mincount : Arrays.asList(0, 1)) {
        // behavior should be independent of mincount
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
        
        // UIF only allowed if field is UNINVERTIBLE
        assertEquals(propsMatch(props, UNINVERTIBLE) ? FacetMethod.UIF : FacetMethod.FCS,
                     SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 0));
      }
    }
  }

  @Test
  public void testNumericMultiValuedDV() {

    for (int props : Arrays.asList(DOC_VALUES ^ MULTIVALUED ^ UNINVERTIBLE,
                                   DOC_VALUES ^ MULTIVALUED)) {
      SchemaField field = new SchemaField("field", new TrieIntField(), props, null);
      // default value is FC
      for (int mincount : Arrays.asList(0, 1)) {
        // behavior should be independent of mincount
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        
        // UIF only allowed if field is UNINVERTIBLE
        assertEquals(propsMatch(props, UNINVERTIBLE) ? FacetMethod.UIF : FacetMethod.FC,
                     SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, mincount));
      }
    }
  }

  @Test
  public void testNumericSingleValuedNoDV() {

    for (int props : Arrays.asList(0 ^ UNINVERTIBLE,
                                   0)) {
      SchemaField field = new SchemaField("field", new TrieIntField(), props, null);
      // FCS is used by default for most requested methods other then UIF -- regardless of mincount
      for (int mincount : Arrays.asList(0, 1)) {
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
      }
      // UIF allowed only if UNINVERTIBLE *AND* mincount > 0
      assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 0));
      assertEquals(propsMatch(props, UNINVERTIBLE) ? FacetMethod.UIF : FacetMethod.FCS,
                   SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 1));
    }
  }

  @Test
  public void testNumericMultiValuedNoDV() {

    for (int props : Arrays.asList(MULTIVALUED ^ UNINVERTIBLE,
                                   MULTIVALUED)) {
      SchemaField field = new SchemaField("field", new TrieIntField(), props, null);
      // FC is used by default for most requested methods other then UIF -- regardless of mincount
      for (int mincount : Arrays.asList(0, 1)) {
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
      }
      // UIF allowed only if UNINVERTIBLE *AND* mincount > 0
      assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 0));
      assertEquals(propsMatch(props, UNINVERTIBLE) ? FacetMethod.UIF : FacetMethod.FC,
                   SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 1));
    }
  }

  @Test
  public void testStringSingleValuedDV() {

    for (int props : Arrays.asList(DOC_VALUES ^ UNINVERTIBLE,
                                   DOC_VALUES)) {
      SchemaField field = new SchemaField("field", new StrField(), props, null);
      // default is FC, otherwise just uses the passed-in method as is unless UIF...
      for (int mincount : Arrays.asList(0, 1)) {
        // behavior should be independent of mincount
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
        // UIF only allowed if field is UNINVERTIBLE
        assertEquals(propsMatch(props, UNINVERTIBLE) ? FacetMethod.UIF : FacetMethod.FCS,
                     SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, mincount));
      }
    }
  }

  @Test
  public void testStringMultiValuedDV() {

    for (int props : Arrays.asList(MULTIVALUED ^ DOC_VALUES ^ UNINVERTIBLE,
                                   MULTIVALUED ^ DOC_VALUES)) {
      SchemaField field = new SchemaField("field", new StrField(), props, null);
      // default is FC, can't use FCS because of multivalues...
      for (int mincount : Arrays.asList(0, 1)) {
        // behavior should be independent of mincount
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
        // UIF only allowed if field is UNINVERTIBLE
        assertEquals(propsMatch(props, UNINVERTIBLE) ? FacetMethod.UIF : FacetMethod.FC,
                     SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, mincount));
      }
    }
  }

  @Test
  public void testStringSingleValuedNoDV() {

    for (int props : Arrays.asList(0 ^ UNINVERTIBLE,
                                   0)) {
      SchemaField field = new SchemaField("field", new StrField(), props, null);
      // default is FC, otherwise just uses the passed-in method as is unless UIF...
      for (int mincount : Arrays.asList(0, 1)) {
        // behavior should be independent of mincount
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
      }
      // UIF allowed only if UNINVERTIBLE *AND* mincount > 0
      assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 0));
      assertEquals(propsMatch(props, UNINVERTIBLE) ? FacetMethod.UIF : FacetMethod.FCS,
                   SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 1));
    }
  }

  @Test
  public void testStringMultiValuedNoDV() {

    for (int props : Arrays.asList(MULTIVALUED ^ UNINVERTIBLE,
                                   MULTIVALUED)) {
      SchemaField field = new SchemaField("field", new StrField(), props, null);
      // default is FC, can't use FCS because of multivalues...
      for (int mincount : Arrays.asList(0, 1)) {
        // behavior should be independent of mincount
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
      }
      // UIF allowed only if UNINVERTIBLE *AND* mincount > 0
      assertEquals(FacetMethod.FC, SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 0));
      assertEquals(propsMatch(props, UNINVERTIBLE) ? FacetMethod.UIF : FacetMethod.FC,
                   SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, 1));
    }
  }

  @Test
  public void testBooleanDefaults() {

    // BoolField defaults to ENUM
    for (int props : Arrays.asList(0 ^ UNINVERTIBLE,
                                   0)) {
      SchemaField field = new SchemaField("field", new BoolField(), props, null);
      assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, null, 0));
      assertEquals(SimpleFacets.FacetMethod.ENUM, SimpleFacets.selectFacetMethod(field, null, 1));
    }
  }
  
  @Test
  public void testPointFields() {
    // Methods other than FCS are not currently supported for PointFields
    for (int props : Arrays.asList(MULTIVALUED ^ DOC_VALUES ^ UNINVERTIBLE,
                                   MULTIVALUED ^ DOC_VALUES,
                                   MULTIVALUED ^ UNINVERTIBLE,
                                   UNINVERTIBLE,
                                   MULTIVALUED,
                                   DOC_VALUES,
                                   0)) {
      SchemaField field = new SchemaField("foo", new IntPointField(), props, null);
      for (int mincount : Arrays.asList(0, 1)) {
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, null, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.ENUM, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FC, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.FCS, mincount));
        assertEquals(FacetMethod.FCS, SimpleFacets.selectFacetMethod(field, FacetMethod.UIF, mincount));
      }
    }
  }
}
