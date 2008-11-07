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

import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.util.AbstractSolrTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Abstract base class for DataImportHandler tests
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public abstract class AbstractDataImportHandlerTest extends
        AbstractSolrTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  protected String loadDataConfig(String dataConfigFileName) {
    try {
      SolrCore core = h.getCore();
      return SolrWriter.getResourceAsString(core.getResourceLoader()
              .openResource(dataConfigFileName));
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  protected void runFullImport(String dataConfig) throws Exception {
    LocalSolrQueryRequest request = lrf.makeRequest("command", "full-import",
            "debug", "on", "clean", "true", "commit", "true", "dataConfig",
            dataConfig);
    h.query("/dataimport", request);
  }

  protected void runDeltaImport(String dataConfig) throws Exception {
    LocalSolrQueryRequest request = lrf.makeRequest("command", "delta-import",
            "debug", "on", "clean", "true", "commit", "true", "dataConfig",
            dataConfig);
    h.query("/dataimport", request);
  }

  /**
   * Helper for creating a Context instance. Useful for testing Transformers
   */
  @SuppressWarnings("unchecked")
  public static Context getContext(DataConfig.Entity parentEntity,
                                   VariableResolverImpl resolver, DataSource parentDataSource,
                                   int currProcess, final List<Map<String, String>> entityFields,
                                   final Map<String, String> entityAttrs) {
    final Context delegate = new ContextImpl(parentEntity, resolver,
            parentDataSource, currProcess,
            new HashMap<String, Object>(), null, null);
    return new Context() {
      public String getEntityAttribute(String name) {
        return entityAttrs == null ? delegate.getEntityAttribute(name)
                : entityAttrs.get(name);
      }

      public List<Map<String, String>> getAllEntityFields() {
        return entityFields == null ? delegate.getAllEntityFields()
                : entityFields;
      }

      public VariableResolver getVariableResolver() {
        return delegate.getVariableResolver();
      }

      public DataSource getDataSource() {
        return delegate.getDataSource();
      }

      public boolean isRootEntity() {
        return false;
      }

      public int currentProcess() {
        return delegate.currentProcess();
      }

      public Map<String, Object> getRequestParameters() {
        return delegate.getRequestParameters();
      }

      public EntityProcessor getEntityProcessor() {
        return null;
      }

      public void setSessionAttribute(String name, Object val, String scope) {
        delegate.setSessionAttribute(name, val, scope);
      }

      public Object getSessionAttribute(String name, String scope) {
        return delegate.getSessionAttribute(name, scope);
      }

      public Context getParentContext() {
        return delegate.getParentContext();
      }

      public DataSource getDataSource(String name) {
        return delegate.getDataSource(name);
      }

      public SolrCore getSolrCore() {
        return delegate.getSolrCore();
      }
    };
  }

  /**
   * Strings at even index are keys, odd-index strings are values in the
   * returned map
   */
  @SuppressWarnings("unchecked")
  public static Map createMap(Object... args) {
    Map result = new HashMap();

    if (args == null || args.length == 0)
      return result;

    for (int i = 0; i < args.length - 1; i += 2)
      result.put(args[i], args[i + 1]);

    return result;
  }
}
