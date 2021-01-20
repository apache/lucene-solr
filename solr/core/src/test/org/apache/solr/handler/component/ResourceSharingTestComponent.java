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

package org.apache.solr.handler.component;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.BlobRepository;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class ResourceSharingTestComponent extends SearchComponent implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrCore core;
  private volatile BlobRepository.BlobContent<TestObject> blob;

  @SuppressWarnings("SynchronizeOnNonFinalField")
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    ModifiableSolrParams mParams = new ModifiableSolrParams(params);
    String q = "text:" + getTestObj().getLastCollection();
    mParams.set("q", q); // search for the last collection name.
    // This should cause the param to show up in the response...
    rb.req.setParams(mParams);
    getTestObj().setLastCollection(core.getCoreDescriptor().getCollectionName());
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {}

  @Override
  public String getDescription() {
    return "ResourceSharingTestComponent";
  }

  @SuppressWarnings("unchecked")
  TestObject getTestObj() {
    return this.blob.get();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void inform(SolrCore core) {
    log.info("Informing test component...");
    this.core = core;
    this.blob =  core.loadDecodeAndCacheBlob(getKey(), new DumbCsvDecoder()).blob;
    log.info("Test component informed!");
  }

  private String getKey() {
    return getResourceName() + "/" + getResourceVersion();
  }

  public String getResourceName() {
    return "testResource";
  }

  public String getResourceVersion() {
    return "1";
  }

  class DumbCsvDecoder implements BlobRepository.Decoder<Object> {
    private final Map<String, String> dict = new HashMap<>();
    
    public DumbCsvDecoder() {}
    
    void processSimpleCsvRow(String string) {
      String[] row = string.split(","); // dumbest csv parser ever... :)
      getDict().put(row[0], row[1]);
    }

    public Map<String, String> getDict() {
      return dict;
    }

    @Override
    public TestObject decode(InputStream inputStream) {
      // loading a tiny csv like:
      // 
      // foo,bar
      // baz,bam

      try (Stream<String> lines = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8"))).lines()) {
          lines.forEach(this::processSimpleCsvRow);
      } catch (Exception e) {
        log.error("failed to read dictionary {}", getResourceName() );
        throw new RuntimeException("Cannot load  dictionary " , e);
      }
      
      assertEquals("bar", dict.get("foo"));
      assertEquals("bam", dict.get("baz"));
      if (log.isInfoEnabled()) {
        log.info("Loaded {}  using {}", getDict().size(), this.getClass().getClassLoader());
      }
      
      // if we get here we have seen the data from the blob and all we need is to test that two collections
      // are able to see the same object..
      return new TestObject();
    }
  }

  
  public static class TestObject {
    public static final String NEVER_UPDATED = "never updated";
    private volatile String lastCollection = NEVER_UPDATED;

    public String getLastCollection() {
      return this.lastCollection;
    }

    public void setLastCollection(String lastCollection) {
      this.lastCollection = lastCollection;
    }
  }
  
}
