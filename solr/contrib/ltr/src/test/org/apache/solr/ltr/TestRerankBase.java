package org.apache.solr.ltr;

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.ltr.feature.ModelMetadata;
import org.apache.solr.ltr.feature.impl.ValueFeature;
import org.apache.solr.ltr.feature.impl.ValueFeature.ValueFeatureWeight;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.ranking.LTRComponent.LTRParams;
import org.apache.solr.ltr.ranking.RankSVMModel;
import org.apache.solr.ltr.rest.ManagedFeatureStore;
import org.apache.solr.ltr.rest.ManagedModelStore;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.ModelException;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.noggit.ObjectBuilder;
import org.restlet.ext.servlet.ServerServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressSSL
public class TestRerankBase extends RestTestBase {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static File tmpSolrHome;
  protected static File tmpConfDir;

  public static final String FEATURE_ENDPOINT = LTRParams.FSTORE_END_POINT;
  public static final String MODEL_ENDPOINT = LTRParams.MSTORE_END_POINT;
  public static final String FEATURE_FILE_NAME = "_schema_fstore.json";
  public static final String MODEL_FILE_NAME = "_schema_mstore.json";
  public static final String PARENT_ENDPOINT = "/schema/*";

  protected static final String collection = "collection1";
  protected static final String confDir = collection + "/conf";

  protected static File fstorefile = null;
  protected static File mstorefile = null;

  public static void setuptest() throws Exception {
    setuptest("solrconfig-ltr.xml", "schema-ltr.xml");
    bulkIndex();
  }

  public static void setupPersistenttest() throws Exception {
    setupPersistentTest("solrconfig-ltr.xml", "schema-ltr.xml");
    bulkIndex();
  }

  // NOTE: this will return a new rest manager since the getCore() method
  // returns a new instance of a restManager.
  public static ManagedFeatureStore getNewManagedFeatureStore() {
    ManagedFeatureStore fs = (ManagedFeatureStore) h.getCore().getRestManager()
        .getManagedResource(FEATURE_ENDPOINT);
    return fs;
  }

  public static ManagedModelStore getNewManagedModelStore() {

    ManagedModelStore fs = (ManagedModelStore) h.getCore().getRestManager()
        .getManagedResource(MODEL_ENDPOINT);
    return fs;
  }

  public static void setuptest(String solrconfig, String schema)
      throws Exception {
    initCore(solrconfig, schema);

    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    tmpConfDir.deleteOnExit();
    FileUtils.copyDirectory(new File(TEST_HOME()),
        tmpSolrHome.getAbsoluteFile());
    File fstore = new File(tmpConfDir, FEATURE_FILE_NAME);
    File mstore = new File(tmpConfDir, MODEL_FILE_NAME);

    if (fstore.exists()) {
      logger.info("remove feature store config file in {}",
          fstore.getAbsolutePath());
      Files.delete(fstore.toPath());
    }
    if (mstore.exists()) {
      logger.info("remove model store config file in {}",
          mstore.getAbsolutePath());
      Files.delete(mstore.toPath());
    }
    if (!solrconfig.equals("solrconfig.xml")) FileUtils.copyFile(new File(
        tmpSolrHome.getAbsolutePath() + "/collection1/conf/" + solrconfig),
        new File(tmpSolrHome.getAbsolutePath()
            + "/collection1/conf/solrconfig.xml"));
    if (!schema.equals("schema.xml")) FileUtils
        .copyFile(new File(tmpSolrHome.getAbsolutePath() + "/collection1/conf/"
            + schema), new File(tmpSolrHome.getAbsolutePath()
            + "/collection1/conf/schema.xml"));

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi",
        ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application",
        "org.apache.solr.rest.SolrSchemaRestApi");
    solrRestApi.setInitParameter("storageIO",
        "org.apache.solr.rest.ManagedResourceStorage$InMemoryStorageIO");
    extraServlets.put(solrRestApi, PARENT_ENDPOINT);

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), solrconfig, schema,
        "/solr", true, extraServlets);
  }

  public static void setupPersistentTest(String solrconfig, String schema)
      throws Exception {
    initCore(solrconfig, schema);

    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    tmpConfDir.deleteOnExit();
    FileUtils.copyDirectory(new File(TEST_HOME()),
        tmpSolrHome.getAbsoluteFile());
    fstorefile = new File(tmpConfDir, FEATURE_FILE_NAME);
    mstorefile = new File(tmpConfDir, MODEL_FILE_NAME);

    if (fstorefile.exists()) {
      logger.info("remove feature store config file in {}",
          fstorefile.getAbsolutePath());
      Files.delete(fstorefile.toPath());
    }
    if (mstorefile.exists()) {
      logger.info("remove model store config file in {}",
          mstorefile.getAbsolutePath());
      Files.delete(mstorefile.toPath());
    }
    // clearModelStore();

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi",
        ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application",
        "org.apache.solr.rest.SolrSchemaRestApi");
    solrRestApi.setInitParameter("storageIO",
        "org.apache.solr.rest.ManagedResourceStorage$JsonStorageIO");

    extraServlets.put(solrRestApi, PARENT_ENDPOINT); // '/schema/*' matches
    // '/schema',
    // '/schema/', and
    // '/schema/whatever...'

    System.setProperty("managed.schema.mutable", "true");
    // System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), solrconfig, schema,
        "/solr", true, extraServlets);
  }

  protected static void aftertest() throws Exception {

    jetty.stop();
    jetty = null;
    FileUtils.deleteDirectory(tmpSolrHome);
    System.clearProperty("managed.schema.mutable");
    // System.clearProperty("enable.update.log");

    restTestHarness = null;
  }

  public static void makeRestTestHarnessNull() {
    restTestHarness = null;
  }

  /** produces a model encoded in json **/
  public static String getModelInJson(String name, String type,
      String[] features, String fstore, String params) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\":").append('"').append(name).append('"').append(",\n");
    sb.append("\"store\":").append('"').append(fstore).append('"')
        .append(",\n");
    sb.append("\"type\":").append('"').append(type).append('"').append(",\n");
    sb.append("\"features\":").append('[');
    for (String feature : features) {
      sb.append("\n\t{ ");
      sb.append("\"name\":").append('"').append(feature).append('"')
          .append("},");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append("\n]\n");
    if (params != null) {
      sb.append(",\n");
      sb.append("\"params\":").append(params);
    }
    sb.append("\n}\n");
    return sb.toString();
  }

  /** produces a model encoded in json **/
  public static String getFeatureInJson(String name, String type,
      String fstore, String params) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\":").append('"').append(name).append('"').append(",\n");
    sb.append("\"store\":").append('"').append(fstore).append('"')
        .append(",\n");
    sb.append("\"type\":").append('"').append(type).append('"');
    if (params != null) {
      sb.append(",\n");
      sb.append("\"params\":").append(params);
    }
    sb.append("\n}\n");
    return sb.toString();
  }

  protected static void loadFeature(String name, String type, String params)
      throws Exception {
    String feature = getFeatureInJson(name, type, "test", params);
    logger.info("loading feauture \n{} ", feature);
    assertJPut(FEATURE_ENDPOINT, feature, "/responseHeader/status==0");
  }

  protected static void loadFeature(String name, String type, String fstore,
      String params) throws Exception {
    String feature = getFeatureInJson(name, type, fstore, params);
    logger.info("loading feauture \n{} ", feature);
    assertJPut(FEATURE_ENDPOINT, feature, "/responseHeader/status==0");
  }

  protected static void loadModel(String name, String type, String[] features,
      String params) throws Exception {
    loadModel(name, type, features, "test", params);
  }

  protected static void loadModel(String name, String type, String[] features,
      String fstore, String params) throws Exception {
    String model = getModelInJson(name, type, features, fstore, params);
    logger.info("loading model \n{} ", model);
    assertJPut(MODEL_ENDPOINT, model, "/responseHeader/status==0");
  }

  public static void loadModels(String fileName) throws Exception {
    URL url = TestRerankBase.class.getResource("/modelExamples/" + fileName);
    String multipleModels = FileUtils.readFileToString(new File(url.toURI()), "UTF-8");

    assertJPut(MODEL_ENDPOINT, multipleModels, "/responseHeader/status==0");
  }

  public static void createModelFromFiles(String modelFileName,
      String featureFileName) throws ModelException, Exception {
    URL url = TestRerankBase.class.getResource("/modelExamples/"
        + modelFileName);
    String modelJson = FileUtils.readFileToString(new File(url.toURI()), "UTF-8");
    ManagedModelStore ms = getNewManagedModelStore();

    url = TestRerankBase.class.getResource("/featureExamples/"
        + featureFileName);
    String featureJson = FileUtils.readFileToString(new File(url.toURI()),"UTF-8");

    Object parsedFeatureJson = null;
    try {
      parsedFeatureJson = ObjectBuilder.fromJSON(featureJson);
    } catch (IOException ioExc) {
      throw new ModelException("ObjectBuilder failed parsing json", ioExc);
    }

    ManagedFeatureStore fs = getNewManagedFeatureStore();
    // fs.getFeatureStore(null).clear();
    fs.doDeleteChild(null, "*"); // is this safe??
    // based on my need to call this I dont think that
    // "getNewManagedFeatureStore()"
    // is actually returning a new feature store each time
    fs.applyUpdatesToManagedData(parsedFeatureJson);
    ms.init(fs);

    ModelMetadata meta = ms.makeModelMetaData(modelJson);
    ms.addMetadataModel(meta);
  }

  public static void loadFeatures(String fileName) throws Exception {
    URL url = TestRerankBase.class.getResource("/featureExamples/" + fileName);
    String multipleFeatures = FileUtils.readFileToString(new File(url.toURI()),"UTF-8");
    logger.info("send \n{}", multipleFeatures);

    assertJPut(FEATURE_ENDPOINT, multipleFeatures, "/responseHeader/status==0");
  }

  protected List<Feature> getFeatures(List<String> names)
      throws FeatureException {
    List<Feature> features = new ArrayList<>();
    int pos = 0;
    for (String name : names) {
      ValueFeature f = new ValueFeature();
      f.init(name, new NamedParams().add("value", 10), pos);
      features.add(f);
      ++pos;
    }
    return features;
  }

  protected List<Feature> getFeatures(String[] names) throws FeatureException {
    return getFeatures(Arrays.asList(names));
  }

  protected static void loadModelAndFeatures(String name, int allFeatureCount,
      int modelFeatureCount) throws Exception {
    String[] features = new String[modelFeatureCount];
    String[] weights = new String[modelFeatureCount];
    for (int i = 0; i < allFeatureCount; i++) {
      String featureName = "c" + i;
      if (i < modelFeatureCount) {
        features[i] = featureName;
        weights[i] = "\"" + featureName + "\":1.0";
      }
      loadFeature(featureName, ValueFeatureWeight.class.getCanonicalName(),
          "{\"value\":" + i + "}");
    }

    loadModel(name, RankSVMModel.class.getCanonicalName(), features,
        "{\"weights\":{" + StringUtils.join(weights, ",") + "}}");
  }

  protected static void bulkIndex() throws Exception {
    System.out.println("-----------index ---------------------");
    assertU(adoc("title", "bloomberg different bla", "description",
        "bloomberg", "id", "6", "popularity", "1"));
    assertU(adoc("title", "bloomberg bloomberg ", "description", "bloomberg",
        "id", "7", "popularity", "2"));
    assertU(adoc("title", "bloomberg bloomberg bloomberg", "description",
        "bloomberg", "id", "8", "popularity", "3"));
    assertU(adoc("title", "bloomberg bloomberg bloomberg bloomberg",
        "description", "bloomberg", "id", "9", "popularity", "5"));
    assertU(commit());
  }

  protected static void bulkIndex(String filePath) throws Exception {
    SolrQueryRequestBase req = lrf.makeRequest(CommonParams.STREAM_CONTENTTYPE,
        "application/xml");

    List<ContentStream> streams = new ArrayList<ContentStream>();
    File file = new File(filePath);
    streams.add(new ContentStreamBase.FileStream(file));
    req.setContentStreams(streams);

    try {
      SolrQueryResponse res = new SolrQueryResponse();
      h.updater.handleRequest(req, res);
    } catch (Throwable ex) {
      // Ignore. Just log the exception and go to the next file
      logger.error(ex.getMessage());
      ex.printStackTrace();
    }
    assertU(commit());

  }

  protected static void buildIndexUsingAdoc(String filepath)
      throws FileNotFoundException {
    Scanner scn = new Scanner(new File(filepath),"UTF-8");
    StringBuffer buff = new StringBuffer();
    scn.nextLine();
    scn.nextLine();
    scn.nextLine(); // Skip the first 3 lines then add everything else
    ArrayList<String> docsToAdd = new ArrayList<String>();
    while (scn.hasNext()) {
      String curLine = scn.nextLine();
      if (curLine.contains("</doc>")) {
        buff.append(curLine + "\n");
        docsToAdd.add(buff.toString().replace("</add>", "")
            .replace("<doc>", "<add>\n<doc>")
            .replace("</doc>", "</doc>\n</add>"));
        if (!scn.hasNext()) break;
        else curLine = scn.nextLine();
        buff = new StringBuffer();
      }
      buff.append(curLine + "\n");
    }
    for (String doc : docsToAdd) {
      assertU(doc.trim());
    }
    assertU(commit());
    scn.close();
  }

}
