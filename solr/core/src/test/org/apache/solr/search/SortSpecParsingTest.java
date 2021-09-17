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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 *
 *
 **/
public class SortSpecParsingTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  private static SortSpec doParseSortSpec(String sortSpec, SolrQueryRequest req) {
    if (random().nextBoolean()) {
      return SortSpecParsing.parseSortSpec(sortSpec, req.getSchema());
    } else {
      return SortSpecParsing.parseSortSpec(sortSpec, req);
    }
  }

  @Test
  public void testSort() throws Exception {
    Sort sort;
    SortSpec spec;
    SolrQueryRequest req = req();

    sort = doParseSortSpec("score desc", req).getSort();
    assertNull("sort", sort);//only 1 thing in the list, no Sort specified

    spec = doParseSortSpec("score desc", req);
    assertNotNull("spec", spec);
    assertNull(spec.getSort());
    assertNotNull(spec.getSchemaFields());
    assertEquals(0, spec.getSchemaFields().size());

    // SOLR-4458 - using different case variations of asc and desc
    sort = doParseSortSpec("score aSc", req).getSort();
    SortField[] flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.SCORE);
    assertTrue(flds[0].getReverse());

    spec = doParseSortSpec("score aSc", req);
    flds = spec.getSort().getSort();
    assertEquals(1, flds.length);
    assertEquals(flds[0].getType(), SortField.Type.SCORE);
    assertTrue(flds[0].getReverse());
    assertEquals(1, spec.getSchemaFields().size());
    assertNull(spec.getSchemaFields().get(0));

    sort = doParseSortSpec("weight dEsC", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);

    spec = doParseSortSpec("weight dEsC", req);
    flds = spec.getSort().getSort();
    assertEquals(1, flds.length);
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(1, spec.getSchemaFields().size());
    assertNotNull(spec.getSchemaFields().get(0));
    assertEquals("weight", spec.getSchemaFields().get(0).getName());

    sort = doParseSortSpec("weight desc,bday ASC", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.Type.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);
    //order aliases
    sort = doParseSortSpec("weight top,bday asc", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.Type.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);
    sort = doParseSortSpec("weight top,bday bottom", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.Type.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);

    //test weird spacing
    sort = doParseSortSpec("weight         DESC,            bday         asc", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getType(), SortField.Type.LONG);
    //handles trailing commas
    sort = doParseSortSpec("weight desc,", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");

    //test functions
    sort = SortSpecParsing.parseSortSpec("pow(weight, 2) desc", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    //Not thrilled about the fragility of string matching here, but...
    //the value sources get wrapped, so the out field is different than the input
    assertEquals(flds[0].getField(), "pow(float(weight),const(2))");
    
    //test functions (more deep)
    sort = SortSpecParsing.parseSortSpec("sum(product(r_f1,sum(d_f1,t_f1,1.0)),a_f1) asc", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    assertEquals(flds[0].getField(), "sum(product(float(r_f1),sum(float(d_f1),float(t_f1),const(1.0))),float(a_f1))");

    sort = SortSpecParsing.parseSortSpec("pow(weight,                 2.0)         desc", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    //Not thrilled about the fragility of string matching here, but...
    //the value sources get wrapped, so the out field is different than the input
    assertEquals(flds[0].getField(), "pow(float(weight),const(2.0))");
                 

    spec = SortSpecParsing.parseSortSpec("pow(weight, 2.0) desc, weight    desc,   bday    asc", req);
    flds = spec.getSort().getSort();
    List<SchemaField> schemaFlds = spec.getSchemaFields();
    assertEquals(3, flds.length);
    assertEquals(3, schemaFlds.size());

    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    //Not thrilled about the fragility of string matching here, but...
    //the value sources get wrapped, so the out field is different than the input
    assertEquals(flds[0].getField(), "pow(float(weight),const(2.0))");
    assertNull(schemaFlds.get(0));

    assertEquals(flds[1].getType(), SortField.Type.FLOAT);
    assertEquals(flds[1].getField(), "weight");
    assertNotNull(schemaFlds.get(1));
    assertEquals("weight", schemaFlds.get(1).getName());

    assertEquals(flds[2].getField(), "bday");
    assertEquals(flds[2].getType(), SortField.Type.LONG);
    assertNotNull(schemaFlds.get(2));
    assertEquals("bday", schemaFlds.get(2).getName());
    
    //handles trailing commas
    sort = doParseSortSpec("weight desc,", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");

    //Test literals in functions
    sort = SortSpecParsing.parseSortSpec("strdist(foo_s1, \"junk\", jw) desc", req).getSort();
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    //the value sources get wrapped, so the out field is different than the input
    assertEquals(flds[0].getField(), "strdist(str(foo_s1),literal(junk), dist=org.apache.lucene.search.spell.JaroWinklerDistance)");

    sort = doParseSortSpec("", req).getSort();
    assertNull(sort);

    spec = doParseSortSpec("", req);
    assertNotNull(spec);
    assertNull(spec.getSort());

    // test includesScore and includesNonScoreDocField methods
    spec = doParseSortSpec("", req);
    assertTrue(spec.includesScore());
    assertFalse(spec.includesNonScoreOrDocField());

    spec = doParseSortSpec("score desc", req);
    assertTrue(spec.includesScore());
    assertFalse(spec.includesNonScoreOrDocField());

    spec = doParseSortSpec("score desc, _docid_ asc", req);
    assertTrue(spec.includesScore());
    assertFalse(spec.includesNonScoreOrDocField());

    spec = doParseSortSpec("_docid_ desc", req);
    assertFalse(spec.includesScore());
    assertFalse(spec.includesNonScoreOrDocField());

    spec = doParseSortSpec("weight desc", req);
    assertFalse(spec.includesScore());
    assertTrue(spec.includesNonScoreOrDocField());

    spec = doParseSortSpec("weight desc, score desc", req);
    assertTrue(spec.includesScore());
    assertTrue(spec.includesNonScoreOrDocField());

    spec = doParseSortSpec("weight desc, _docid_ desc", req);
    assertFalse(spec.includesScore());
    assertTrue(spec.includesNonScoreOrDocField());

    req.close();
  }

  @Test
  public void testBad() throws Exception {
    Sort sort;
    SolrQueryRequest req = req();

    //test some bad vals
    try {
      sort = doParseSortSpec("weight, desc", req).getSort();
      assertTrue(false);
    } catch (SolrException e) {
      //expected
    }
    try {
      sort = doParseSortSpec("w", req).getSort();
      assertTrue(false);
    } catch (SolrException e) {
      //expected
    }
    try {
      sort = doParseSortSpec("weight desc, bday", req).getSort();
      assertTrue(false);
    } catch (SolrException e) {
    }

    try {
      //bad number of commas
      sort = SortSpecParsing.parseSortSpec("pow(weight,,2) desc, bday asc", req).getSort();
      assertTrue(false);
    } catch (SolrException e) {
    }

    try {
      //bad function
      sort = SortSpecParsing.parseSortSpec("pow() desc, bday asc", req).getSort();
      assertTrue(false);
    } catch (SolrException e) {
    }

    try {
      //bad number of parens
      sort = SortSpecParsing.parseSortSpec("pow((weight,2) desc, bday asc", req).getSort();
      assertTrue(false);
    } catch (SolrException e) {
    }

    req.close();
  }
}
