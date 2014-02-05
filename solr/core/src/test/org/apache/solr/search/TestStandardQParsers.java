package org.apache.solr.search;

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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Check standard query parsers for class loading problems during initialization (NAME field is final and static).
 * Because every query plugin extend {@link org.apache.solr.search.QParserPlugin} and contains own instance of {@link org.apache.solr.search.QParserPlugin#standardPlugins},
 * There are a cyclic dependencies of static fields between plugins and order of initialization can be wrong if NAME field is not final.
 * This leads to NPEs during Solr startup.
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-5526">SOLR-5526</a>
 * @see org.apache.solr.search.QParserPlugin#standardPlugins
 *
 */
public class TestStandardQParsers extends LuceneTestCase {
  /**
   * Field name of constant mandatory for query parser plugin.
   */
  public static final String FIELD_NAME = "NAME";

  /**
   * Test standard query parsers registered in {@link org.apache.solr.search.QParserPlugin#standardPlugins}
   * have NAME field which is final, static, and matches the registered name.
   */
  @Test
  public void testRegisteredName() throws Exception {
    Map<String, Class<QParserPlugin>> standardPlugins = getStandardQParsers();

    List<String> notStatic = new ArrayList<String>(standardPlugins.size());
    List<String> notFinal = new ArrayList<String>(standardPlugins.size());
    List<String> mismatch = new ArrayList<String>(standardPlugins.size());
 
    for (Map.Entry<String,Class<QParserPlugin>> pair : standardPlugins.entrySet()) {
      String regName = pair.getKey();
      Class<QParserPlugin> clazz = pair.getValue();

      Field nameField = clazz.getField(FIELD_NAME);
      int modifiers = nameField.getModifiers();
      if (!Modifier.isFinal(modifiers)) {
        notFinal.add(clazz.getName());
      }
      if (!Modifier.isStatic(modifiers)) {
        notStatic.add(clazz.getName());
      } else if (! regName.equals(nameField.get(null))) {
        mismatch.add(regName +" != "+ nameField.get(null) +"("+ clazz.getName() +")");
      }
    }

    assertTrue("All standard QParsers must have final NAME, broken: " + notFinal, 
               notFinal.isEmpty());
    assertTrue("All standard QParsers must have static NAME, broken: " + notStatic, 
               notStatic.isEmpty());
    assertTrue("All standard QParsers must be registered using NAME, broken: " + mismatch, 
               mismatch.isEmpty());

    assertTrue("DEFAULT_QTYPE is not in the standard set of registered names: " + 
               QParserPlugin.DEFAULT_QTYPE,
               standardPlugins.keySet().contains(QParserPlugin.DEFAULT_QTYPE));

  }

  /**
   * Get standard query parsers registered by default.
   *
   * @see org.apache.solr.search.QParserPlugin#standardPlugins
   * @return Map of classes extending QParserPlugin keyed by the registered name
   */
  private Map<String,Class<QParserPlugin>> getStandardQParsers() {
    Object[] standardPluginsValue = QParserPlugin.standardPlugins;

    Map<String, Class<QParserPlugin>> standardPlugins 
      = new HashMap<String, Class<QParserPlugin>>(standardPluginsValue.length / 2);

    for (int i = 0; i < standardPluginsValue.length; i += 2) {
      @SuppressWarnings("unchecked")
      String registeredName = (String) standardPluginsValue[i];
      @SuppressWarnings("unchecked")
      Class<QParserPlugin> clazz = (Class<QParserPlugin>) standardPluginsValue[i + 1];
      standardPlugins.put(registeredName, clazz);
    }
    return standardPlugins;
  }

}
