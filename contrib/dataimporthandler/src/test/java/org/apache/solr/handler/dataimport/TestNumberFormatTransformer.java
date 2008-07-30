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
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Test for NumberFormatTransformer
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestNumberFormatTransformer {
  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow_SingleNumber() {
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTest.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER));
    Context c = AbstractDataImportHandlerTest.getContext(null, null, null, 0,
            l, null);
    Map m = AbstractDataImportHandlerTest.createMap("num", "123,567");
    new NumberFormatTransformer().transformRow(m, c);
    Assert.assertEquals(new Long(123567), m.get("num"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow_MultipleNumbers() throws Exception {
    List fields = new ArrayList();
    fields.add(AbstractDataImportHandlerTest.createMap(DataImporter.COLUMN,
            "inputs"));
    fields.add(AbstractDataImportHandlerTest.createMap(DataImporter.COLUMN,
            "outputs", RegexTransformer.SRC_COL_NAME, "inputs",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER));

    List inputs = new ArrayList();
    inputs.add("123,567");
    inputs.add("245,678");
    Map row = AbstractDataImportHandlerTest.createMap("inputs", inputs);

    VariableResolverImpl resolver = new VariableResolverImpl();
    resolver.addNamespace("e", row);

    Context context = AbstractDataImportHandlerTest.getContext(null, resolver,
            null, 0, fields, null);
    new NumberFormatTransformer().transformRow(row, context);

    List output = new ArrayList();
    output.add(new Long(123567));
    output.add(new Long(245678));
    Map outputRow = AbstractDataImportHandlerTest.createMap("inputs", inputs,
            "outputs", output);

    Assert.assertEquals(outputRow, row);
  }
}
