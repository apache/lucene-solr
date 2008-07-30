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
package org.apache.solr.handler.dataimport;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Test for RegexTransformer
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestRegexTransformer {

  @Test
  public void commaSeparated() {
    List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
    fields.add(getField("col1", "string", null, "a", ","));
    Context context = AbstractDataImportHandlerTest.getContext(null, null,
            null, 0, fields, null);
    Map<String, Object> src = new HashMap<String, Object>();
    String s = "a,bb,cc,d";
    src.put("a", s);
    Map<String, Object> result = new RegexTransformer().transformRow(src,
            context);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(4, ((List) result.get("col1")).size());
  }

  @Test
  public void replaceWith() {
    List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
    Map<String, String> fld = getField("name", "string", "'", null, null);
    fld.put("replaceWith", "''");
    fields.add(fld);
    Context context = AbstractDataImportHandlerTest.getContext(null, null,
            null, 0, fields, null);
    Map<String, Object> src = new HashMap<String, Object>();
    String s = "D'souza";
    src.put("name", s);
    Map<String, Object> result = new RegexTransformer().transformRow(src,
            context);
    Assert.assertEquals("D''souza", result.get("name"));
  }

  @Test
  public void mileage() {
    Context context = AbstractDataImportHandlerTest.getContext(null, null,
            null, 0, getFields(), null);

    Map<String, Object> src = new HashMap<String, Object>();
    String s = "Fuel Economy Range: 26 mpg Hwy, 19 mpg City";
    src.put("rowdata", s);
    Map<String, Object> result = new RegexTransformer().transformRow(src,
            context);
    Assert.assertEquals(3, result.size());
    Assert.assertEquals(s, result.get("rowdata"));
    Assert.assertEquals("26", result.get("highway_mileage"));
    Assert.assertEquals("19", result.get("city_mileage"));

  }

  public static List<Map<String, String>> getFields() {
    List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
    fields.add(getField("city_mileage", "sint",
            "Fuel Economy Range:\\s*?\\d*?\\s*?mpg Hwy,\\s*?(\\d*?)\\s*?mpg City",
            "rowdata", null));
    fields.add(getField("highway_mileage", "sint",
            "Fuel Economy Range:\\s*?(\\d*?)\\s*?mpg Hwy,\\s*?\\d*?\\s*?mpg City",
            "rowdata", null));
    fields.add(getField("seating_capacity", "sint", "Seating capacity:(.*)",
            "rowdata", null));
    fields
            .add(getField("warranty", "string", "Warranty:(.*)", "rowdata", null));
    fields.add(getField("rowdata", "string", null, "rowdata", null));
    return fields;

  }

  public static Map<String, String> getField(String col, String type,
                                             String re, String srcCol, String splitBy) {
    HashMap<String, String> vals = new HashMap<String, String>();
    vals.put("column", col);
    vals.put("type", type);
    vals.put("regex", re);
    vals.put("sourceColName", srcCol);
    vals.put("splitBy", splitBy);
    return vals;
  }
}
