package org.apache.solr.search;
/**
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

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.AbstractSolrTestCase;


/**
 *
 *
 **/
public class QueryParsingTest extends AbstractSolrTestCase {
  public String getSchemaFile() {
    return "schema.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }


  public void testSort() throws Exception {
    Sort sort;

    IndexSchema schema = h.getCore().getSchema();
    sort = QueryParsing.parseSort("score desc", schema);
    assertNull("sort", sort);//only 1 thing in the list, no Sort specified
    sort = QueryParsing.parseSort("weight desc", schema);
    SortField[] flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    sort = QueryParsing.parseSort("weight desc,bday asc", schema);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);
    //order aliases
    sort = QueryParsing.parseSort("weight top,bday asc", schema);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);
    sort = QueryParsing.parseSort("weight top,bday bottom", schema);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);

    //test weird spacing
    sort = QueryParsing.parseSort("weight         desc,            bday         asc", schema);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[1].getType(), SortField.LONG);
    assertEquals(flds[1].getField(), "bday");
    //handles trailing commas
    sort = QueryParsing.parseSort("weight desc,", schema);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    //test some bad vals
    try {
      sort = QueryParsing.parseSort("weight, desc", schema);
      assertTrue(false);
    } catch (SolrException e) {
      //expected
    }
    try {
      sort = QueryParsing.parseSort("weight desc, bday", schema);
      assertTrue(false);
    } catch (SolrException e) {
    }

  }

}
