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

package org.apache.solr.ltr.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.ltr.store.rest.ManagedModelStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAdapterModel extends TestRerankBase {

  private static int numDocs = 0;
  private static float scoreValue;

  @Before
  public void setup() throws Exception {

    setuptest(false);

    numDocs = random().nextInt(10);
    for (int ii=1; ii <= numDocs; ++ii) {
      String id = Integer.toString(ii);
      assertU(adoc("id", id, "popularity", ii+"00"));
    }
    assertU(commit());

    loadFeature("popularity", FieldValueFeature.class.getName(), "test", "{\"field\":\"popularity\"}");

    scoreValue = random().nextFloat();
    final File scoreValueFile = new File(tmpConfDir, "scoreValue.txt");
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(scoreValueFile), StandardCharsets.UTF_8))) {
      writer.write(Float.toString(scoreValue));
    }
    scoreValueFile.deleteOnExit();

    final String modelJson = getModelInJson(
        "answerModel",
        CustomModel.class.getName(),
        new String[] { "popularity" },
        "test",
        "{\"answerFileName\":\"" + scoreValueFile.getName() + "\"}");
    assertJPut(ManagedModelStore.REST_END_POINT, modelJson, "/responseHeader/status==0");
  }
  @After
  public void cleanup() throws Exception {
    aftertest();
  }

  @Test
  public void test() throws Exception {
    final int rows = random().nextInt(numDocs+1); // 0..numDocs
    final SolrQuery query = new SolrQuery("*:*");
    query.setRows(rows);
    query.setFields("*,score");
    query.add("rq", "{!ltr model=answerModel}");
    final String[] tests = new String[rows];
    for (int ii=0; ii<rows; ++ii) {
      tests[ii] = "/response/docs/["+ii+"]/score=="+scoreValue;
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }

  public static class CustomModel extends AdapterModel {

    /**
     * answerFileName is part of the LTRScoringModel params map
     * and therefore here it does not individually
     * influence the class hashCode, equals, etc.
     */
    private String answerFileName;
    /**
     * answerValue is obtained from answerFileName
     * and therefore here it does not individually
     * influence the class hashCode, equals, etc.
     */
    private float answerValue;

    public CustomModel(String name, List<Feature> features, List<Normalizer> norms, String featureStoreName,
        List<Feature> allFeatures, Map<String,Object> params) {
      super(name, features, norms, featureStoreName, allFeatures, params);
    }

    public void setAnswerFileName(String answerFileName) {
      this.answerFileName = answerFileName;
    }

    @Override
    protected void validate() throws ModelException {
      super.validate();
      if (answerFileName == null) {
        throw new ModelException("no answerFileName configured for model "+name);
      }
    }

    public void init(SolrResourceLoader solrResourceLoader) throws ModelException {
      super.init(solrResourceLoader);
      try (
          InputStream is = solrResourceLoader.openResource(answerFileName);
          InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
          BufferedReader br = new BufferedReader(isr)
          ) {
        answerValue = Float.parseFloat(br.readLine());
      } catch (IOException e) {
        throw new ModelException("Failed to get the answerValue from the given answerFileName (" + answerFileName + ")", e);
      }
    }

    @Override
    public float score(float[] modelFeatureValuesNormalized) {
      return answerValue;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc, float finalScore,
        List<Explanation> featureExplanations) {
      return Explanation.match(finalScore, toString()
          + " model, always returns "+Float.toString(answerValue)+".");
    }

  }

}
