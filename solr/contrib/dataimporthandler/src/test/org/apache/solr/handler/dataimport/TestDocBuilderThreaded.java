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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test DocBuilder with "threads"
 */
public class TestDocBuilderThreaded extends AbstractDataImportHandlerTestCase {

  //TODO: fix this test to not require FSDirectory.
  static String savedFactory;
  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "solr.MockFSDirectoryFactory");
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
    docs.add(createMap("id", "1", "worker", "one"));
    docs.add(createMap("id", "2", "worker", "two"));
    docs.add(createMap("id", "3", "worker", "three"));
    docs.add(createMap("id", "4", "worker", "four"));
    MockDataSource.setIterator("select * from y", docs.iterator());
    for (Map<String, Object> aDoc : docs) {
      String theWorker = (String) aDoc.get("worker");
      final List<Map<String, Object>> details = getDetails4Worker(theWorker);
      log.info("details: " + details);
      MockDataSource.setIterator(theWorker, details.iterator());
    }
  }

  @After
  @Override
  public void tearDown() throws Exception {
    DemoProcessor.entitiesInitied = 0;
    DemoEvaluator.evaluated = 0;
    MockDataSource.clearCache();
    assertU(delQ("*:*"));
    assertU(commit());
    super.tearDown();
  }

  @Test
  public void testProcessorThreaded2Entities() throws Exception {
    runFullImport(threaded2EntitiesWithProcessor);
    assertEquals("EntityProcessor.init() for child entity was called less times than the number of rows",
        4, DemoProcessor.entitiesInitied);
  }

  @Test
  public void testProcessor2EntitiesNoThreads() throws Exception {
    runFullImport(twoEntitiesWithProcessor);
    assertEquals("EntityProcessor.init() for child entity was called less times than the number of rows",
        4, DemoProcessor.entitiesInitied);
  }

  /*
  * This test fails in TestEnviroment, but works in real Live
  */
  @Test
  public void testEvaluator() throws Exception {
    runFullImport(twoEntitiesWithEvaluatorProcessor);
    assertEquals("Evaluator was invoked less times than the number of rows",
        4, DemoEvaluator.evaluated);
  }
  @Test 
  public void testContinue() throws Exception {
    runFullImport(twoEntitiesWithFailingProcessor);
    assertQ(req("*:*"), "//*[@numFound='0']"); // should rollback
  }
  
  @Test
  public void testContinueThreaded() throws Exception {
    runFullImport(twoThreadedEntitiesWithFailingProcessor);
    assertQ(req("*:*"), "//*[@numFound='0']"); // should rollback
  }

  @Test
  public void testFailingTransformerContinueThreaded() throws Exception {
    runFullImport(twoThreadedEntitiesWithFailingTransformer);
    assertQ(req("*:*"), "//*[@numFound='4']");
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getDetails4Worker(String aWorker) {
    List<Map<String, Object>> details4Worker = new ArrayList<Map<String, Object>>();
    details4Worker.add(createMap("author_s", "Author_" + aWorker, "title_s", "Title for " + aWorker, "text_s", " Text for " + aWorker));
    return details4Worker;
  }

  private final String threaded2EntitiesWithProcessor =

      "<dataConfig> <dataSource type=\"MockDataSource\"/>\n" +
          "<document>" +
          "<entity name=\"job\" query=\"select * from y\"" +
          " pk=\"id\" \n" +
          " threads='1'\n" +
          ">" +
          "<field column=\"id\" />\n" +
          "<entity name=\"details\" processor=\"TestDocBuilderThreaded$DemoProcessor\" \n" +
          "worker=\"${job.worker}\" \n" +
          "query=\"${job.worker}\" \n" +
          "transformer=\"TemplateTransformer\" " +
          " >" +
          "<field column=\"author_s\" />" +
          "<field column=\"title_s\" />" +
          " <field column=\"text_s\" />" +
          " <field column=\"generated_id_s\" template=\"generated_${job.id}\" />" +
          "</entity>" +
          "</entity>" +
          "</document>" +
          "</dataConfig>";
	private final String twoEntitiesWithProcessor =

      "<dataConfig> <dataSource type=\"MockDataSource\"/>\n" +
          "<document>" +
          "<entity name=\"job\" query=\"select * from y\"" +
          " pk=\"id\" \n" +
          ">" +
          "<field column=\"id\" />\n" +
          "<entity name=\"details\" processor=\"TestDocBuilderThreaded$DemoProcessor\" \n" +
          "worker=\"${job.worker}\" \n" +
          "query=\"${job.worker}\" \n" +
          "transformer=\"TemplateTransformer\" " +
          " >" +
          "<field column=\"author_s\" />" +
          "<field column=\"title_s\" />" +
          " <field column=\"text_s\" />" +
          " <field column=\"generated_id_s\" template=\"generated_${job.id}\" />" +
          "</entity>" +
          "</entity>" +
          "</document>" +
          "</dataConfig>";
          
  private final String twoEntitiesWithEvaluatorProcessor =

      "<dataConfig> <dataSource type=\"MockDataSource\"/>\n" +
          "<function name=\"concat\" class=\"TestDocBuilderThreaded$DemoEvaluator\" />" +
          "<document>" +
          "<entity name=\"job\" query=\"select * from y\"" +
          " pk=\"id\" \n" +
          " threads=\"1\" " +
          ">" +
          "<field column=\"id\" />\n" +
          "<entity name=\"details\" processor=\"TestDocBuilderThreaded$DemoProcessor\" \n" +
          "worker=\"${dataimporter.functions.concat(details.author_s, ':_:' , details.title_s, 9 )}\" \n" +
          "query=\"${job.worker}\" \n" +
          "transformer=\"TemplateTransformer\" " +
          " >" +
          "<field column=\"author_s\" />" +
          "<field column=\"title_s\" />" +
          " <field column=\"text_s\" />" +
          " <field column=\"generated_id_s\" template=\"generated_${job.id}\" />" +
          "</entity>" +
          "</entity>" +
          "</document>" +
          "</dataConfig>";


  private final String twoThreadedEntitiesWithFailingProcessor =
  
        "<dataConfig> <dataSource type=\"MockDataSource\"/>\n" +
            "<document>" +
            "<entity name=\"job\" processor=\"TestDocBuilderThreaded$DemoProcessor\" \n" +
            " threads=\"1\" " +
            " query=\"select * from y\"" +
            " pk=\"id\" \n" +
            " worker=\"id\" \n" +
            " onError=\"continue\" " +
            ">" +
            "<field column=\"id\" />\n" +
            "<entity name=\"details\" processor=\"TestDocBuilderThreaded$FailingProcessor\" \n" +
            "worker=\"${job.worker}\" \n" +
            "query=\"${job.worker}\" \n" +
            "transformer=\"TemplateTransformer\" " +
            "onError=\"continue\" " +
            "fail=\"yes\" " +
            " >" +
            "<field column=\"author_s\" />" +
            "<field column=\"title_s\" />" +
            " <field column=\"text_s\" />" +
            " <field column=\"generated_id_s\" template=\"generated_${job.id}\" />" +
            "</entity>" +
            "</entity>" +
            "</document>" +
            "</dataConfig>";
  
  private final String twoEntitiesWithFailingProcessor =
    
    "<dataConfig> <dataSource type=\"MockDataSource\"/>\n" +
        "<document>" +
        "<entity name=\"job\" processor=\"TestDocBuilderThreaded$DemoProcessor\" \n" +
        " query=\"select * from y\"" +
        " pk=\"id\" \n" +
        " worker=\"id\" \n" +
        " onError=\"continue\" " +
        ">" +
        "<field column=\"id\" />\n" +
        "<entity name=\"details\" processor=\"TestDocBuilderThreaded$FailingProcessor\" \n" +
        "worker=\"${job.worker}\" \n" +
        "query=\"${job.worker}\" \n" +
        "transformer=\"TemplateTransformer\" " +
        "onError=\"continue\" " +
        "fail=\"yes\" " +
        " >" +
        "<field column=\"author_s\" />" +
        "<field column=\"title_s\" />" +
        " <field column=\"text_s\" />" +
        " <field column=\"generated_id_s\" template=\"generated_${job.id}\" />" +
        "</entity>" +
        "</entity>" +
        "</document>" +
        "</dataConfig>";

  private final String twoThreadedEntitiesWithFailingTransformer =

    "<dataConfig> <dataSource type=\"MockDataSource\"/>\n" +
        "<document>" +
        "<entity name=\"job\" processor=\"TestDocBuilderThreaded$DemoProcessor\" \n" +
        " threads=\"1\" " +
        " query=\"select * from y\"" +
        " pk=\"id\" \n" +
        " worker=\"id\" \n" +
        " onError=\"continue\" " +
        ">" +
        "<field column=\"id\" />\n" +
        "<entity name=\"details\" \n" +
        "worker=\"${job.worker}\" \n" +
        "query=\"${job.worker}\" \n" +
        "transformer=\"TestDocBuilderThreaded$FailingTransformer\" " +
        "onError=\"continue\" " +
        " >" +
        "<field column=\"author_s\" />" +
        "<field column=\"title_s\" />" +
        " <field column=\"text_s\" />" +
        " <field column=\"generated_id_s\" template=\"generated_${job.id}\" />" +
        "</entity>" +
        "</entity>" +
        "</document>" +
        "</dataConfig>";


  public static class DemoProcessor extends SqlEntityProcessor {

    public static int entitiesInitied = 0;

    @Override
    public void init(Context context) {
      super.init(context);
      String result = context.getResolvedEntityAttribute("worker");
      if (result == null || result.trim().length() == 0) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "Could not resolve entity attribute");
      } else entitiesInitied++;
    }
  }
  public static class FailingProcessor extends SqlEntityProcessor {
    @Override
    public void init(Context context) {
      super.init(context);
      String fail = context.getResolvedEntityAttribute("fail");
      if (fail != null && fail.equalsIgnoreCase("yes")) {
        throw new NullPointerException("I was told to");
      }      
    }
  }

  public static class FailingTransformer extends Transformer  {
    @Override
    public Object transformRow(Map<String, Object> row, Context context) {
      throw new RuntimeException("Always fail");
    }
  }

  public static class DemoEvaluator extends Evaluator {
    public static int evaluated = 0;

    /* (non-Javadoc)
    * @see org.apache.solr.handler.dataimport.Evaluator#evaluate(java.lang.String, org.apache.solr.handler.dataimport.Context)
    */
    @Override
    @SuppressWarnings("unchecked")
    public String evaluate(String expression, Context context) {
      List allParams = EvaluatorBag.parseParams(expression, context.getVariableResolver());
      StringBuilder result = new StringBuilder();
      for (Object aVar : allParams) {
        result.append(aVar.toString());
      }
      evaluated++;
      return result.toString();
    }
  }
  
}
