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
package org.apache.solr.ltr;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FeatureException;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.ltr.model.ModelException;
import org.apache.solr.ltr.store.FeatureStore;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.ltr.store.rest.ManagedModelStore;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.ManagedResourceStorage;
import org.apache.solr.rest.SolrSchemaRestApi;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.restlet.ext.servlet.ServerServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRerankBase extends RestTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final SolrResourceLoader solrResourceLoader = new SolrResourceLoader();

  protected static File tmpSolrHome;
  protected static File tmpConfDir;

  public static final String FEATURE_FILE_NAME = "_schema_feature-store.json";
  public static final String MODEL_FILE_NAME = "_schema_model-store.json";
  public static final String PARENT_ENDPOINT = "/schema/*";

  protected static final String COLLECTION = "collection1";
  protected static final String CONF_DIR = COLLECTION + "/conf";

  protected static File fstorefile = null;
  protected static File mstorefile = null;

  final private static String SYSTEM_PROPERTY_SOLR_LTR_TRANSFORMER_FV_DEFAULTFORMAT = "solr.ltr.transformer.fv.defaultFormat";
  private static String defaultFeatureFormat;

  protected String chooseDefaultFeatureVector(String dense, String sparse) {
    if (defaultFeatureFormat == null) {
      // to match <code><str name="defaultFormat">${solr.ltr.transformer.fv.defaultFormat:dense}</str></code> snippet
      return dense;
    } else  if ("dense".equals(defaultFeatureFormat)) {
      return dense;
    } else  if ("sparse".equals(defaultFeatureFormat)) {
      return sparse;
    } else {
      fail("unexpected feature format choice: "+defaultFeatureFormat);
      return null;
    }
  }

  protected static void chooseDefaultFeatureFormat() throws Exception {
    switch (random().nextInt(3)) {
      case 0:
        defaultFeatureFormat = null;
        break;
      case 1:
        defaultFeatureFormat = "dense";
        break;
      case 2:
        defaultFeatureFormat = "sparse";
        break;
      default:
        fail("unexpected feature format choice");
        break;
    }
    if (defaultFeatureFormat != null) {
      System.setProperty(SYSTEM_PROPERTY_SOLR_LTR_TRANSFORMER_FV_DEFAULTFORMAT, defaultFeatureFormat);
    }
  }

  protected static void unchooseDefaultFeatureFormat() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_LTR_TRANSFORMER_FV_DEFAULTFORMAT);
  }

  protected static void setuptest(boolean bulkIndex) throws Exception {
    chooseDefaultFeatureFormat();
    setuptest("solrconfig-ltr.xml", "schema.xml");
    if (bulkIndex) bulkIndex();
  }

  protected static void setupPersistenttest(boolean bulkIndex) throws Exception {
    chooseDefaultFeatureFormat();
    setupPersistentTest("solrconfig-ltr.xml", "schema.xml");
    if (bulkIndex) bulkIndex();
  }

  public static ManagedFeatureStore getManagedFeatureStore() {
    try (SolrCore core = jetty.getCoreContainer().getCore(DEFAULT_TEST_CORENAME)) {
      return ManagedFeatureStore.getManagedFeatureStore(core);
    }
  }

  public static ManagedModelStore getManagedModelStore() {
    try (SolrCore core = jetty.getCoreContainer().getCore(DEFAULT_TEST_CORENAME)) {
      return ManagedModelStore.getManagedModelStore(core);
    }
  }

  protected static SortedMap<ServletHolder,String>  setupTestInit(
      String solrconfig, String schema,
      boolean isPersistent) throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, CONF_DIR);
    tmpConfDir.deleteOnExit();
    FileUtils.copyDirectory(new File(TEST_HOME()),
        tmpSolrHome.getAbsoluteFile());

    final File fstore = new File(tmpConfDir, FEATURE_FILE_NAME);
    final File mstore = new File(tmpConfDir, MODEL_FILE_NAME);

    if (isPersistent) {
      fstorefile = fstore;
      mstorefile = mstore;
    }

    if (fstore.exists()) {
      log.info("remove feature store config file in {}",
          fstore.getAbsolutePath());
      Files.delete(fstore.toPath());
    }
    if (mstore.exists()) {
      log.info("remove model store config file in {}",
          mstore.getAbsolutePath());
      Files.delete(mstore.toPath());
    }
    if (!solrconfig.equals("solrconfig.xml")) {
      FileUtils.copyFile(new File(tmpSolrHome.getAbsolutePath()
          + "/collection1/conf/" + solrconfig),
          new File(tmpSolrHome.getAbsolutePath()
              + "/collection1/conf/solrconfig.xml"));
    }
    if (!schema.equals("schema.xml")) {
      FileUtils.copyFile(new File(tmpSolrHome.getAbsolutePath()
          + "/collection1/conf/" + schema),
          new File(tmpSolrHome.getAbsolutePath()
              + "/collection1/conf/schema.xml"));
    }

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi",
        ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application",
        SolrSchemaRestApi.class.getCanonicalName());
    solrRestApi.setInitParameter("storageIO",
        ManagedResourceStorage.InMemoryStorageIO.class.getCanonicalName());
    extraServlets.put(solrRestApi, PARENT_ENDPOINT);

    System.setProperty("managed.schema.mutable", "true");

    return extraServlets;
  }

  public static void setuptest(String solrconfig, String schema)
      throws Exception {

    SortedMap<ServletHolder,String> extraServlets =
        setupTestInit(solrconfig,schema,false);
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), solrconfig, schema,
        "/solr", true, extraServlets);
  }

  public static void setupPersistentTest(String solrconfig, String schema)
      throws Exception {

    SortedMap<ServletHolder,String> extraServlets =
        setupTestInit(solrconfig,schema,true);

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), solrconfig, schema,
        "/solr", true, extraServlets);
  }

  protected static void aftertest() throws Exception {
    if (null != restTestHarness) {
      restTestHarness.close();
      restTestHarness = null;
    }
    if (null != jetty) {
      jetty.stop();
      jetty = null;
    }
    if (null != tmpSolrHome) {
      FileUtils.deleteDirectory(tmpSolrHome);
      tmpSolrHome = null;
    }
    System.clearProperty("managed.schema.mutable");
    // System.clearProperty("enable.update.log");
    unchooseDefaultFeatureFormat();
  }

  public static void makeRestTestHarnessNull() {
    restTestHarness = null;
  }

  /** produces a model encoded in json **/
  public static String getModelInJson(String name, String type,
      String[] features, String fstore, String params) {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\":").append('"').append(name).append('"').append(",\n");
    sb.append("\"store\":").append('"').append(fstore).append('"')
        .append(",\n");
    sb.append("\"class\":").append('"').append(type).append('"').append(",\n");
    sb.append("\"features\":").append('[');
    if (features.length > 0) {
      for (final String feature : features) {
        sb.append("\n\t{ ");
        sb.append("\"name\":").append('"').append(feature).append('"')
        .append("},");
      }
      sb.deleteCharAt(sb.length() - 1);
    }
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
    final StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\":").append('"').append(name).append('"').append(",\n");
    sb.append("\"store\":").append('"').append(fstore).append('"')
        .append(",\n");
    sb.append("\"class\":").append('"').append(type).append('"');
    if (params != null) {
      sb.append(",\n");
      sb.append("\"params\":").append(params);
    }
    sb.append("\n}\n");
    return sb.toString();
  }

  protected static void loadFeature(String name, String type, String params)
      throws Exception {
    final String feature = getFeatureInJson(name, type, "test", params);
    log.info("loading feauture \n{} ", feature);
    assertJPut(ManagedFeatureStore.REST_END_POINT, feature,
        "/responseHeader/status==0");
  }

  protected static void loadFeature(String name, String type, String fstore,
      String params) throws Exception {
    final String feature = getFeatureInJson(name, type, fstore, params);
    log.info("loading feauture \n{} ", feature);
    assertJPut(ManagedFeatureStore.REST_END_POINT, feature,
        "/responseHeader/status==0");
  }

  protected static void loadModel(String name, String type, String[] features,
      String params) throws Exception {
    loadModel(name, type, features, "test", params);
  }

  protected static void loadModel(String name, String type, String[] features,
      String fstore, String params) throws Exception {
    final String model = getModelInJson(name, type, features, fstore, params);
    log.info("loading model \n{} ", model);
    assertJPut(ManagedModelStore.REST_END_POINT, model,
        "/responseHeader/status==0");
  }

  public static void loadModels(String fileName) throws Exception {
    final URL url = TestRerankBase.class.getResource("/modelExamples/"
        + fileName);
    final String multipleModels = FileUtils.readFileToString(
        new File(url.toURI()), "UTF-8");

    assertJPut(ManagedModelStore.REST_END_POINT, multipleModels,
        "/responseHeader/status==0");
  }

  public static LTRScoringModel createModelFromFiles(String modelFileName,
      String featureFileName) throws ModelException, Exception {
    return createModelFromFiles(modelFileName, featureFileName,
        FeatureStore.DEFAULT_FEATURE_STORE_NAME);
  }

  public static LTRScoringModel createModelFromFiles(String modelFileName,
      String featureFileName, String featureStoreName) throws ModelException, Exception {
    URL url = TestRerankBase.class.getResource("/modelExamples/"
        + modelFileName);
    final String modelJson = FileUtils.readFileToString(new File(url.toURI()),
        "UTF-8");
    final ManagedModelStore ms = getManagedModelStore();

    url = TestRerankBase.class.getResource("/featureExamples/"
        + featureFileName);
    final String featureJson = FileUtils.readFileToString(
        new File(url.toURI()), "UTF-8");

    Object parsedFeatureJson = null;
    try {
      parsedFeatureJson = Utils.fromJSONString(featureJson);
    } catch (final Exception ioExc) {
      throw new ModelException("ObjectBuilder failed parsing json", ioExc);
    }

    final ManagedFeatureStore fs = getManagedFeatureStore();
    // fs.getFeatureStore(null).clear();
    fs.doDeleteChild(null, featureStoreName); // is this safe??
    // based on my need to call this I dont think that
    // "getNewManagedFeatureStore()"
    // is actually returning a new feature store each time
    fs.applyUpdatesToManagedData(parsedFeatureJson);
    ms.setManagedFeatureStore(fs); // can we skip this and just use fs directly below?

    final LTRScoringModel ltrScoringModel = ManagedModelStore.fromLTRScoringModelMap(
        solrResourceLoader, mapFromJson(modelJson), ms.getManagedFeatureStore());
    ms.addModel(ltrScoringModel);
    return ltrScoringModel;
  }

  @SuppressWarnings("unchecked")
  static private Map<String,Object> mapFromJson(String json) throws ModelException {
    Object parsedJson = null;
    try {
      parsedJson = Utils.fromJSONString(json);
    } catch (final Exception ioExc) {
      throw new ModelException("ObjectBuilder failed parsing json", ioExc);
    }
    return (Map<String,Object>) parsedJson;
  }

  public static void loadFeatures(String fileName) throws Exception {
    final URL url = TestRerankBase.class.getResource("/featureExamples/"
        + fileName);
    final String multipleFeatures = FileUtils.readFileToString(
        new File(url.toURI()), "UTF-8");
    log.info("send \n{}", multipleFeatures);

    assertJPut(ManagedFeatureStore.REST_END_POINT, multipleFeatures,
        "/responseHeader/status==0");
  }

  protected List<Feature> getFeatures(List<String> names)
      throws FeatureException {
    final List<Feature> features = new ArrayList<>();
    int pos = 0;
    for (final String name : names) {
      final Map<String,Object> params = new HashMap<String,Object>();
      params.put("value", 10);
      final Feature f = Feature.getInstance(solrResourceLoader,
          ValueFeature.class.getCanonicalName(),
          name, params);
      f.setIndex(pos);
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
    final String[] features = new String[modelFeatureCount];
    final String[] weights = new String[modelFeatureCount];
    for (int i = 0; i < allFeatureCount; i++) {
      final String featureName = "c" + i;
      if (i < modelFeatureCount) {
        features[i] = featureName;
        weights[i] = "\"" + featureName + "\":1.0";
      }
      loadFeature(featureName, ValueFeature.ValueFeatureWeight.class.getCanonicalName(),
          "{\"value\":" + i + "}");
    }

    loadModel(name, LinearModel.class.getCanonicalName(), features,
        "{\"weights\":{" + String.join(",", weights) + "}}");
  }

  protected static void bulkIndex() throws Exception {
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
    final SolrQueryRequestBase req = lrf.makeRequest(
        CommonParams.STREAM_CONTENTTYPE, "application/xml");

    final List<ContentStream> streams = new ArrayList<ContentStream>();
    final File file = new File(filePath);
    streams.add(new ContentStreamBase.FileStream(file));
    req.setContentStreams(streams);

    try {
      final SolrQueryResponse res = new SolrQueryResponse();
      h.updater.handleRequest(req, res);
    } catch (final Throwable ex) {
      // Ignore. Just log the exception and go to the next file
      log.error(ex.getMessage(), ex);
    }
    assertU(commit());

  }

  protected static void buildIndexUsingAdoc(String filepath)
      throws FileNotFoundException {
    final Scanner scn = new Scanner(new File(filepath), "UTF-8");
    StringBuffer buff = new StringBuffer();
    scn.nextLine();
    scn.nextLine();
    scn.nextLine(); // Skip the first 3 lines then add everything else
    final ArrayList<String> docsToAdd = new ArrayList<String>();
    while (scn.hasNext()) {
      String curLine = scn.nextLine();
      if (curLine.contains("</doc>")) {
        buff.append(curLine + "\n");
        docsToAdd.add(buff.toString().replace("</add>", "")
            .replace("<doc>", "<add>\n<doc>")
            .replace("</doc>", "</doc>\n</add>"));
        if (!scn.hasNext()) {
          break;
        } else {
          curLine = scn.nextLine();
        }
        buff = new StringBuffer();
      }
      buff.append(curLine + "\n");
    }
    for (final String doc : docsToAdd) {
      assertU(doc.trim());
    }
    assertU(commit());
    scn.close();
  }

  protected static void doTestParamsToMap(String featureClassName,
      LinkedHashMap<String,Object> featureParams) throws Exception {

    // start with default parameters
    final LinkedHashMap<String,Object> paramsA = new LinkedHashMap<String,Object>();
    final Object defaultValue;
    switch (random().nextInt(6)) {
      case 0:
        defaultValue = null;
        break;
      case 1:
        defaultValue = "1.2";
        break;
      case 2:
        defaultValue = Double.valueOf(3.4d);
        break;
      case 3:
        defaultValue = Float.valueOf(0.5f);
        break;
      case 4:
        defaultValue = Integer.valueOf(67);
        break;
      case 5:
        defaultValue = Long.valueOf(89);
        break;
      default:
        defaultValue = null;
        fail("unexpected defaultValue choice");
        break;
    }
    if (defaultValue != null) {
      paramsA.put("defaultValue", defaultValue);
    }

    // then add specific parameters
    paramsA.putAll(featureParams);

    // next choose a random feature name
    final String featureName = "randomFeatureName"+random().nextInt(10);

    // create a feature from the parameters
    final Feature featureA = Feature.getInstance(solrResourceLoader,
        featureClassName, featureName, paramsA);

    // turn the feature back into parameters
    final LinkedHashMap<String,Object> paramsB = featureA.paramsToMap();

    // create feature B from feature A's parameters
    final Feature featureB = Feature.getInstance(solrResourceLoader,
        featureClassName, featureName, paramsB);

    // check that feature A and feature B are identical
    assertEquals(featureA, featureB);
  }

}
