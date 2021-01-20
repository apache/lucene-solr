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
package org.apache.solr.core;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;

/**
 * test that configs can override the RecoveryStrategy
 */
public class ConfigureRecoveryStrategyTest extends SolrTestCaseJ4 {

  private static final String solrConfigFileNameConfigure = "solrconfig-configurerecoverystrategy.xml";
  private static final String solrConfigFileNameCustom = "solrconfig-customrecoverystrategy.xml";

  private static String solrConfigFileName;

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrConfigFileName = (random().nextBoolean()
        ? solrConfigFileNameConfigure : solrConfigFileNameCustom);
    initCore(solrConfigFileName, "schema.xml");
  }

  public void testBuilder() throws Exception {
    final RecoveryStrategy.Builder recoveryStrategyBuilder =
        h.getCore().getSolrCoreState().getRecoveryStrategyBuilder();
    assertNotNull("recoveryStrategyBuilder is null", recoveryStrategyBuilder);

    final String expectedClassName;

    if (solrConfigFileName.equals(solrConfigFileNameConfigure)) {
      expectedClassName = RecoveryStrategy.Builder.class.getName();
    } else if (solrConfigFileName.equals(solrConfigFileNameCustom)) {
      assertTrue("recoveryStrategyBuilder is wrong class (instanceof)",
          recoveryStrategyBuilder instanceof CustomRecoveryStrategyBuilder);
      expectedClassName = ConfigureRecoveryStrategyTest.CustomRecoveryStrategyBuilder.class.getName();
    } else {
      expectedClassName = null;
    }

    assertEquals("recoveryStrategyBuilder is wrong class (name)",
        expectedClassName, recoveryStrategyBuilder.getClass().getName());
  }

  public void testAlmostAllMethodsAreFinal() throws Exception {
    for (Method m : RecoveryStrategy.class.getDeclaredMethods()) {
      if (Modifier.isStatic(m.getModifiers()) || Modifier.isPrivate(m.getModifiers())) continue;
      final String methodName = m.getName();
      if ("getReplicateLeaderUrl".equals(methodName)) {
        assertFalse(m.toString(), Modifier.isFinal(m.getModifiers()));
      } else {
        assertTrue(m.toString(), Modifier.isFinal(m.getModifiers()));
      }
    }
  }

  static public class CustomRecoveryStrategy extends RecoveryStrategy {

    private String alternativeBaseUrlProp;

    public String getAlternativeBaseUrlProp() {
      return alternativeBaseUrlProp;
    }

    public void setAlternativeBaseUrlProp(String alternativeBaseUrlProp) {
      this.alternativeBaseUrlProp = alternativeBaseUrlProp;
    }

    public CustomRecoveryStrategy(CoreContainer cc, CoreDescriptor cd,
        RecoveryStrategy.RecoveryListener recoveryListener) {
      super(cc, cd, recoveryListener);
    }

    @Override
    protected String getReplicateLeaderUrl(ZkNodeProps leaderprops) {
      return ZkCoreNodeProps.getCoreUrl(
          leaderprops.getStr(alternativeBaseUrlProp),
          leaderprops.getStr(ZkStateReader.CORE_NAME_PROP));
    }
  }

  static public class CustomRecoveryStrategyBuilder extends RecoveryStrategy.Builder {
    @Override
    protected RecoveryStrategy newRecoveryStrategy(CoreContainer cc, CoreDescriptor cd,
        RecoveryStrategy.RecoveryListener recoveryListener) {
      return new CustomRecoveryStrategy(cc, cd, recoveryListener);
    }
  }

}
