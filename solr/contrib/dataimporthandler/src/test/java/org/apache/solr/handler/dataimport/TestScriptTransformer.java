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

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Test for ScriptTransformer
 * </p>
 * <p/>
 * All tests in this have been ignored because script support is only available
 * in Java 1.6+
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestScriptTransformer extends AbstractDataImportHandlerTestCase {

  @Test
  public void testBasic() {
    try {
      String script = "function f1(row,context){"
              + "row.put('name','Hello ' + row.get('name'));" + "return row;\n" + "}";
      Context context = getContext("f1", script);
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("name", "Scott");
      EntityProcessorWrapper sep = new EntityProcessorWrapper(new SqlEntityProcessor(), null);
      sep.init(context);
      sep.applyTransformer(map);
      assertEquals(map.get("name"), "Hello Scott");
    } catch (DataImportHandlerException e) {
      assumeFalse("JRE does not contain a JavaScript engine (OpenJDK)", "<script> can be used only in java 6 or above".equals(e.getMessage()));
      throw e;
    }
  }

  private Context getContext(String funcName, String script) {
    List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
    Map<String, String> entity = new HashMap<String, String>();
    entity.put("name", "hello");
    entity.put("transformer", "script:" + funcName);

    TestContext context = getContext(null, null, null,
            Context.FULL_DUMP, fields, entity);
    context.script = script;
    context.scriptlang = "JavaScript";
    return context;
  }

  @Test
  public void testOneparam() {
    try {
      String script = "function f1(row){"
              + "row.put('name','Hello ' + row.get('name'));" + "return row;\n" + "}";

      Context context = getContext("f1", script);
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("name", "Scott");
      EntityProcessorWrapper sep = new EntityProcessorWrapper(new SqlEntityProcessor(), null);
      sep.init(context);
      sep.applyTransformer(map);
      assertEquals(map.get("name"), "Hello Scott");
    } catch (DataImportHandlerException e) {
      assumeFalse("JRE does not contain a JavaScript engine (OpenJDK)", "<script> can be used only in java 6 or above".equals(e.getMessage()));
      throw e;
    }
  }

  @Test
  public void testReadScriptTag() throws Exception {
    try {
      DocumentBuilder builder = DocumentBuilderFactory.newInstance()
              .newDocumentBuilder();
      Document document = builder.parse(new InputSource(new StringReader(xml)));
      DataConfig config = new DataConfig();
      config.readFromXml((Element) document.getElementsByTagName("dataConfig")
              .item(0));
      assertTrue(config.script.text.indexOf("checkNextToken") > -1);
    } catch (DataImportHandlerException e) {
      assumeFalse("JRE does not contain a JavaScript engine (OpenJDK)", "<script> can be used only in java 6 or above".equals(e.getMessage()));
      throw e;
    }
  }

  @Test
  public void testCheckScript() throws Exception {
    try {
      DocumentBuilder builder = DocumentBuilderFactory.newInstance()
              .newDocumentBuilder();
      Document document = builder.parse(new InputSource(new StringReader(xml)));
      DataConfig config = new DataConfig();
      config.readFromXml((Element) document.getElementsByTagName("dataConfig")
              .item(0));

      Context c = getContext("checkNextToken", config.script.text);

      Map map = new HashMap();
      map.put("nextToken", "hello");
      EntityProcessorWrapper sep = new EntityProcessorWrapper(new SqlEntityProcessor(), null);
      sep.init(c);
      sep.applyTransformer(map);
      assertEquals("true", map.get("$hasMore"));
      map = new HashMap();
      map.put("nextToken", "");
      sep.applyTransformer(map);
      assertNull(map.get("$hasMore"));
    } catch (DataImportHandlerException e) {
      assumeFalse("JRE does not contain a JavaScript engine (OpenJDK)", "<script> can be used only in java 6 or above".equals(e.getMessage()));
      throw e;
    }
  }

  static String xml = "<dataConfig>\n"
          + "<script><![CDATA[\n"
          + "function checkNextToken(row)\t{\n"
          + " var nt = row.get('nextToken');"
          + " if (nt && nt !='' ){ "
          + "    row.put('$hasMore', 'true');}\n"
          + "    return row;\n"
          + "}]]></script>\t<document>\n"
          + "\t\t<entity name=\"mbx\" pk=\"articleNumber\" processor=\"XPathEntityProcessor\"\n"
          + "\t\t\turl=\"?boardId=${dataimporter.defaults.boardId}&amp;maxRecords=20&amp;includeBody=true&amp;startDate=${dataimporter.defaults.startDate}&amp;guid=:autosearch001&amp;reqId=1&amp;transactionId=stringfortracing&amp;listPos=${mbx.nextToken}\"\n"
          + "\t\t\tforEach=\"/mbmessage/articles/navigation | /mbmessage/articles/article\" transformer=\"script:checkNextToken\">\n"
          + "\n" + "\t\t\t<field column=\"nextToken\"\n"
          + "\t\t\t\txpath=\"/mbmessage/articles/navigation/nextToken\" />\n"
          + "\n" + "\t\t</entity>\n" + "\t</document>\n" + "</dataConfig>";
}
