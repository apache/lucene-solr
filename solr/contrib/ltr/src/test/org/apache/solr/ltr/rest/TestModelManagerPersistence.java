package org.apache.solr.ltr.rest;

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

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.impl.ValueFeature;
import org.apache.solr.ltr.ranking.LTRComponent.LTRParams;
import org.apache.solr.ltr.ranking.RankSVMModel;
import org.junit.Before;
import org.junit.Test;

@SuppressSSL
public class TestModelManagerPersistence extends TestRerankBase {

  @Before
  public void init() throws Exception {
    setupPersistenttest();
  }

  // executed first
  @Test
  public void testFeaturePersistence() throws Exception {

    loadFeature("feature", ValueFeature.class.getCanonicalName(), "test",
        "{\"value\":2}");
    System.out.println(restTestHarness.query(LTRParams.FSTORE_END_POINT
        + "/test"));
    assertJQ(LTRParams.FSTORE_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    restTestHarness.reload();
    assertJQ(LTRParams.FSTORE_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    loadFeature("feature1", ValueFeature.class.getCanonicalName(), "test1",
        "{\"value\":2}");
    loadFeature("feature2", ValueFeature.class.getCanonicalName(), "test",
        "{\"value\":2}");
    loadFeature("feature3", ValueFeature.class.getCanonicalName(), "test2",
        "{\"value\":2}");
    assertJQ(LTRParams.FSTORE_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    assertJQ(LTRParams.FSTORE_END_POINT + "/test",
        "/features/[1]/name=='feature2'");
    assertJQ(LTRParams.FSTORE_END_POINT + "/test1",
        "/features/[0]/name=='feature1'");
    assertJQ(LTRParams.FSTORE_END_POINT + "/test2",
        "/features/[0]/name=='feature3'");
    restTestHarness.reload();
    assertJQ(LTRParams.FSTORE_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    assertJQ(LTRParams.FSTORE_END_POINT + "/test",
        "/features/[1]/name=='feature2'");
    assertJQ(LTRParams.FSTORE_END_POINT + "/test1",
        "/features/[0]/name=='feature1'");
    assertJQ(LTRParams.FSTORE_END_POINT + "/test2",
        "/features/[0]/name=='feature3'");
    loadModel("test-model", RankSVMModel.class.getCanonicalName(),
        new String[] {"feature"}, "test", "{\"weights\":{\"feature\":1.0}}");
    String fstorecontent = FileUtils.readFileToString(fstorefile,"UTF-8");
    String mstorecontent = FileUtils.readFileToString(mstorefile,"UTF-8");

    System.out.println("fstore:\n");
    System.out.println(fstorecontent);

    System.out.println("mstore:\n");
    System.out.println(mstorecontent);

  }

}
