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
package org.apache.solr.handler.dataimport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.junit.Before;

/**
 * <p>
 * Abstract base class for DataImportHandler tests
 * </p>
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 *
 * @since solr 1.3
 */
public abstract class AbstractDataImportHandlerTestCase extends
        SolrTestCaseJ4 {

  // note, a little twisted that we shadow this static method
  public static void initCore(String config, String schema) throws Exception {
    File testHome = createTempDir("core-home").toFile();
    FileUtils.copyDirectory(getFile("dih/solr"), testHome);
    initCore(config, schema, testHome.getAbsolutePath());
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    File home = createTempDir("dih-properties").toFile();
    System.setProperty("solr.solr.home", home.getAbsolutePath());    
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
            "debug", "on", "clean", "false", "commit", "true", "dataConfig",
            dataConfig);
    h.query("/dataimport", request);
  }

  /**
   * Redirect {@link SimplePropertiesWriter#filename} to a temporary location 
   * and return it.
   */
  protected File redirectTempProperties(DataImporter di) {
    try {
      File tempFile = createTempFile().toFile();
      di.getConfig().getPropertyWriter().getParameters()
        .put(SimplePropertiesWriter.FILENAME, tempFile.getAbsolutePath());
      return tempFile;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Runs a full-import using the given dataConfig and the provided request parameters.
   *
   * By default, debug=on, clean=true and commit=true are passed which can be overridden.
   *
   * @param dataConfig the data-config xml as a string
   * @param extraParams any extra request parameters needed to be passed to DataImportHandler
   * @throws Exception in case of any error
   */
  protected void runFullImport(String dataConfig, Map<String, String> extraParams) throws Exception {
    HashMap<String, String> params = new HashMap<>();
    params.put("command", "full-import");
    params.put("debug", "on");
    params.put("dataConfig", dataConfig);
    params.put("clean", "true");
    params.put("commit", "true");
    params.putAll(extraParams);
    NamedList l = new NamedList();
    for (Map.Entry<String, String> e : params.entrySet()) {
      l.add(e.getKey(),e.getValue());
    }
    LocalSolrQueryRequest request = new LocalSolrQueryRequest(h.getCore(), l);  
    h.query("/dataimport", request);
  }

  /**
   * Helper for creating a Context instance. Useful for testing Transformers
   */
  @SuppressWarnings("unchecked")
  public static TestContext getContext(EntityProcessorWrapper parent,
                                   VariableResolver resolver, DataSource parentDataSource,
                                   String currProcess, final List<Map<String, String>> entityFields,
                                   final Map<String, String> entityAttrs) {
    if (resolver == null) resolver = new VariableResolver();
    final Context delegate = new ContextImpl(parent, resolver,
            parentDataSource, currProcess,
        new HashMap<>(), null, null);
    return new TestContext(entityAttrs, delegate, entityFields, parent == null);
  }

  /**
   * Strings at even index are keys, odd-index strings are values in the
   * returned map
   */
  @SuppressWarnings("unchecked")
  public static Map createMap(Object... args) {
   return Utils.makeMap(args);
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to set modified time for a file")
  public static File createFile(File tmpdir, String name, byte[] content,
                                boolean changeModifiedTime) throws IOException {
    File file = new File(tmpdir.getAbsolutePath() + File.separator + name);
    file.deleteOnExit();
    FileOutputStream f = new FileOutputStream(file);
    f.write(content);
    f.close();
    if (changeModifiedTime)
      file.setLastModified(System.currentTimeMillis() - 3600000);
    return file;
  }
  
  public static Map<String, String> getField(String col, String type,
                                             String re, String srcCol, String splitBy) {
    HashMap<String, String> vals = new HashMap<>();
    vals.put("column", col);
    vals.put("type", type);
    vals.put("regex", re);
    vals.put("sourceColName", srcCol);
    vals.put("splitBy", splitBy);
    return vals;
  }
  
  static class TestContext extends Context {
    private final Map<String, String> entityAttrs;
    private final Context delegate;
    private final List<Map<String, String>> entityFields;
    private final boolean root;
    String script,scriptlang;

    public TestContext(Map<String, String> entityAttrs, Context delegate,
                       List<Map<String, String>> entityFields, boolean root) {
      this.entityAttrs = entityAttrs;
      this.delegate = delegate;
      this.entityFields = entityFields;
      this.root = root;
    }

    @Override
    public String getEntityAttribute(String name) {
      return entityAttrs == null ? delegate.getEntityAttribute(name) : entityAttrs.get(name);
    }

    @Override
    public String getResolvedEntityAttribute(String name) {
      return entityAttrs == null ? delegate.getResolvedEntityAttribute(name) :
              delegate.getVariableResolver().replaceTokens(entityAttrs.get(name));
    }

    @Override
    public List<Map<String, String>> getAllEntityFields() {
      return entityFields == null ? delegate.getAllEntityFields()
              : entityFields;
    }

    @Override
    public VariableResolver getVariableResolver() {
      return delegate.getVariableResolver();
    }

    @Override
    public DataSource getDataSource() {
      return delegate.getDataSource();
    }

    @Override
    public boolean isRootEntity() {
      return root;
    }

    @Override
    public String currentProcess() {
      return delegate.currentProcess();
    }

    @Override
    public Map<String, Object> getRequestParameters() {
      return delegate.getRequestParameters();
    }

    @Override
    public EntityProcessor getEntityProcessor() {
      return null;
    }

    @Override
    public void setSessionAttribute(String name, Object val, String scope) {
      delegate.setSessionAttribute(name, val, scope);
    }

    @Override
    public Object getSessionAttribute(String name, String scope) {
      return delegate.getSessionAttribute(name, scope);
    }

    @Override
    public Context getParentContext() {
      return delegate.getParentContext();
    }

    @Override
    public DataSource getDataSource(String name) {
      return delegate.getDataSource(name);
    }

    @Override
    public SolrCore getSolrCore() {
      return delegate.getSolrCore();
    }

    @Override
    public Map<String, Object> getStats() {
      return delegate.getStats();
    }


    @Override
    public String getScript() {
      return script == null ? delegate.getScript() : script;
    }

    @Override
    public String getScriptLanguage() {
      return scriptlang == null ? delegate.getScriptLanguage() : scriptlang;
    }

    @Override
    public void deleteDoc(String id) {

    }

    @Override
    public void deleteDocByQuery(String query) {

    }

    @Override
    public Object resolve(String var) {
      return delegate.resolve(var);
    }

    @Override
    public String replaceTokens(String template) {
      return delegate.replaceTokens(template);
    }
  }

  public static class TestUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req,
        SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return new TestUpdateRequestProcessor(next);
    }
    
  }
  
  public static class TestUpdateRequestProcessor extends UpdateRequestProcessor {
  
    public static boolean finishCalled = false;
    public static boolean processAddCalled = false;
    public static boolean processCommitCalled = false;
    public static boolean processDeleteCalled = false;
    public static boolean mergeIndexesCalled = false;
    public static boolean rollbackCalled = false;
  
    public static void reset() {
      finishCalled = false;
      processAddCalled = false;
      processCommitCalled = false;
      processDeleteCalled = false;
      mergeIndexesCalled = false;
      rollbackCalled = false;
    }
    
    public TestUpdateRequestProcessor(UpdateRequestProcessor next) {
      super(next);
      reset();
    }

    @Override
    public void finish() throws IOException {
      finishCalled = true;
      super.finish();
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      processAddCalled = true;
      super.processAdd(cmd);
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      processCommitCalled = true;
      super.processCommit(cmd);
    }

    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      processDeleteCalled = true;
      super.processDelete(cmd);
    }

    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      mergeIndexesCalled = true;
      super.processMergeIndexes(cmd);
    }

    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      rollbackCalled = true;
      super.processRollback(cmd);
    }
    
  }
}
