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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Assert;
import org.junit.Test;

import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * <p>
 * Test for NumberFormatTransformer
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestNumberFormatTransformer extends SolrTestCaseJ4 {
  private char GROUPING_SEP = new DecimalFormatSymbols().getGroupingSeparator();
  private char DECIMAL_SEP = new DecimalFormatSymbols().getDecimalSeparator();

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow_SingleNumber() {
    char GERMAN_GROUPING_SEP = new DecimalFormatSymbols(Locale.GERMANY).getGroupingSeparator();
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER));
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "localizedNum",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER, NumberFormatTransformer.LOCALE, "de-DE"));
    Context c = AbstractDataImportHandlerTestCase.getContext(null, null, null, Context.FULL_DUMP, l, null);
    Map m = AbstractDataImportHandlerTestCase.createMap("num", "123" + GROUPING_SEP + "567", "localizedNum", "123" + GERMAN_GROUPING_SEP + "567");
    new NumberFormatTransformer().transformRow(m, c);
    Assert.assertEquals(new Long(123567), m.get("num"));
    Assert.assertEquals(new Long(123567), m.get("localizedNum"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow_MultipleNumbers() throws Exception {
    List fields = new ArrayList();
    fields.add(AbstractDataImportHandlerTestCase.createMap(DataImporter.COLUMN,
            "inputs"));
    fields.add(AbstractDataImportHandlerTestCase.createMap(DataImporter.COLUMN,
            "outputs", RegexTransformer.SRC_COL_NAME, "inputs",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER));

    List inputs = new ArrayList();
    inputs.add("123" + GROUPING_SEP + "567");
    inputs.add("245" + GROUPING_SEP + "678");
    Map row = AbstractDataImportHandlerTestCase.createMap("inputs", inputs);

    VariableResolverImpl resolver = new VariableResolverImpl();
    resolver.addNamespace("e", row);

    Context context = AbstractDataImportHandlerTestCase.getContext(null, resolver, null, Context.FULL_DUMP, fields, null);
    new NumberFormatTransformer().transformRow(row, context);

    List output = new ArrayList();
    output.add(new Long(123567));
    output.add(new Long(245678));
    Map outputRow = AbstractDataImportHandlerTestCase.createMap("inputs", inputs,
            "outputs", output);

    Assert.assertEquals(outputRow, row);
  }

  @Test(expected = DataImportHandlerException.class)
  @SuppressWarnings("unchecked")
  public void testTransformRow_InvalidInput1_Number() {
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER));
    Context c = AbstractDataImportHandlerTestCase.getContext(null, null, null, Context.FULL_DUMP, l, null);
    Map m = AbstractDataImportHandlerTestCase.createMap("num", "123" + GROUPING_SEP + "5a67");
    new NumberFormatTransformer().transformRow(m, c);
  }

  @Test(expected = DataImportHandlerException.class)
  @SuppressWarnings("unchecked")
  public void testTransformRow_InvalidInput2_Number() {
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER));
    Context c = AbstractDataImportHandlerTestCase.getContext(null, null, null, Context.FULL_DUMP, l, null);
    Map m = AbstractDataImportHandlerTestCase.createMap("num", "123" + GROUPING_SEP + "567b");
    new NumberFormatTransformer().transformRow(m, c);
  }

  @Test(expected = DataImportHandlerException.class)
  @SuppressWarnings("unchecked")
  public void testTransformRow_InvalidInput2_Currency() {
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.CURRENCY));
    Context c = AbstractDataImportHandlerTestCase.getContext(null, null, null, Context.FULL_DUMP, l, null);
    Map m = AbstractDataImportHandlerTestCase.createMap("num", "123" + GROUPING_SEP + "567b");
    new NumberFormatTransformer().transformRow(m, c);
  }

  @Test(expected = DataImportHandlerException.class)
  @SuppressWarnings("unchecked")
  public void testTransformRow_InvalidInput1_Percent() {
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.PERCENT));
    Context c = AbstractDataImportHandlerTestCase.getContext(null, null, null, Context.FULL_DUMP, l, null);
    Map m = AbstractDataImportHandlerTestCase.createMap("num", "123" + GROUPING_SEP + "5a67");
    new NumberFormatTransformer().transformRow(m, c);
  }

  @Test(expected = DataImportHandlerException.class)
  @SuppressWarnings("unchecked")
  public void testTransformRow_InvalidInput3_Currency() {
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.CURRENCY));
    Context c = AbstractDataImportHandlerTestCase.getContext(null, null, null, Context.FULL_DUMP, l, null);
    Map m = AbstractDataImportHandlerTestCase.createMap(
            "num", "123" + DECIMAL_SEP + "456" + DECIMAL_SEP + "789");
    new NumberFormatTransformer().transformRow(m, c);
  }

  @Test(expected = DataImportHandlerException.class)
  @SuppressWarnings("unchecked")
  public void testTransformRow_InvalidInput3_Number() {
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER));
    Context c = AbstractDataImportHandlerTestCase.getContext(null, null, null, Context.FULL_DUMP, l, null);
    Map m = AbstractDataImportHandlerTestCase.createMap(
            "num", "123" + DECIMAL_SEP + "456" + DECIMAL_SEP + "789");
    new NumberFormatTransformer().transformRow(m, c);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformRow_MalformedInput_Number() {
    List l = new ArrayList();
    l.add(AbstractDataImportHandlerTestCase.createMap("column", "num",
            NumberFormatTransformer.FORMAT_STYLE, NumberFormatTransformer.NUMBER));
    Context c = AbstractDataImportHandlerTestCase.getContext(null, null, null, Context.FULL_DUMP, l, null);
    Map m = AbstractDataImportHandlerTestCase.createMap(
            "num", "123" + GROUPING_SEP + GROUPING_SEP + "789");
    new NumberFormatTransformer().transformRow(m, c);
    Assert.assertEquals(new Long(123789), m.get("num"));
  }
}
