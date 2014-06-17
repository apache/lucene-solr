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
package org.apache.solr.handler.dataimport;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * <p>
 * Test for DateFormatTransformer
 * </p>
 *
 *
 * @since solr 1.3
 */
public class TestDateFormatTransformer extends AbstractDataImportHandlerTestCase {

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow_SingleRow() throws Exception {
    List<Map<String, String>> fields = new ArrayList<>();
    fields.add(createMap(DataImporter.COLUMN, "lastModified"));
    fields.add(createMap(DataImporter.COLUMN,
            "dateAdded", RegexTransformer.SRC_COL_NAME, "lastModified",
            DateFormatTransformer.DATE_TIME_FMT, "${xyz.myDateFormat}"));

    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy", Locale.ROOT);
    Date now = format.parse(format.format(new Date()));

    Map<String,Object> row = createMap("lastModified", format.format(now));

    VariableResolver resolver = new VariableResolver();
    resolver.addNamespace("e", row);
    resolver.addNamespace("xyz", createMap("myDateFormat", "MM/dd/yyyy"));

    Context context = getContext(null, resolver,
            null, Context.FULL_DUMP, fields, null);
    new DateFormatTransformer().transformRow(row, context);
    assertEquals(now, row.get("dateAdded"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow_MultipleRows() throws Exception {
    List<Map<String, String>> fields = new ArrayList<>();
    fields.add(createMap(DataImporter.COLUMN, "lastModified"));
    fields.add(createMap(DataImporter.COLUMN,
            "dateAdded", RegexTransformer.SRC_COL_NAME, "lastModified",
            DateFormatTransformer.DATE_TIME_FMT, "MM/dd/yyyy hh:mm:ss.SSS"));

    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss.SSS", Locale.ROOT);
    Date now1 = format.parse(format.format(new Date()));
    Date now2 = format.parse(format.format(new Date()));

    Map<String,Object> row = new HashMap<>();
    List<String> list = new ArrayList<>();
    list.add(format.format(now1));
    list.add(format.format(now2));
    row.put("lastModified", list);

    VariableResolver resolver = new VariableResolver();
    resolver.addNamespace("e", row);

    Context context = getContext(null, resolver,
            null, Context.FULL_DUMP, fields, null);
    new DateFormatTransformer().transformRow(row, context);
    List<Object> output = new ArrayList<>();
    output.add(now1);
    output.add(now2);
    assertEquals(output, row.get("dateAdded"));
  }

}
