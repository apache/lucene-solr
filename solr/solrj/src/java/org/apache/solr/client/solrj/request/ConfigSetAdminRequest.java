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
package org.apache.solr.client.solrj.request;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Map;
import java.util.Properties;
import java.util.Collection;
import java.util.Collections;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase.FileStream;

import static org.apache.solr.common.params.CommonParams.NAME;

import org.apache.commons.io.IOUtils;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 5.4
 */
public abstract class ConfigSetAdminRequest
      <Q extends ConfigSetAdminRequest<Q,R>, R extends ConfigSetAdminResponse>
      extends SolrRequest<R> {

  protected ConfigSetAction action = null;

  @SuppressWarnings({"rawtypes"})
  protected ConfigSetAdminRequest setAction(ConfigSetAction action) {
    this.action = action;
    return this;
  }

  public ConfigSetAdminRequest() {
    super(METHOD.GET, "/admin/configs");
  }

  public ConfigSetAdminRequest(String path) {
    super (METHOD.GET, path);
  }

  protected abstract Q getThis();

  @Override
  public SolrParams getParams() {
    if (action == null) {
      throw new RuntimeException( "no action specified!" );
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(ConfigSetParams.ACTION, action.toString());
    return params;
  }


  @Override
  protected abstract R createResponse(SolrClient client);

  protected abstract static class ConfigSetSpecificAdminRequest
       <T extends ConfigSetAdminRequest<T,ConfigSetAdminResponse>>
       extends ConfigSetAdminRequest<T,ConfigSetAdminResponse> {
    protected String configSetName = null;

    public final T setConfigSetName(String configSetName) {
      this.configSetName = configSetName;
      return getThis();
    }

    public final String getConfigSetName() {
      return configSetName;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (configSetName == null) {
        throw new RuntimeException( "no ConfigSet specified!" );
      }
      params.set(NAME, configSetName);
      return params;
    }

    @Override
    protected ConfigSetAdminResponse createResponse(SolrClient client) {
      return new ConfigSetAdminResponse();
    }
  }

  /**
   * Uploads files to create a new configset, or modify an existing config set.
   *
   * When creating a new configset, the file to be uploaded must be a ZIP file containing the entire configset being uploaded.
   * When modifing an existing configset, the file to be uploaded should either be a ZIP file containing the entire configset 
   * being uploaded, or an individual file to upload if {@link #setFilePath} is being used.
   */
  public static class Upload extends ConfigSetSpecificAdminRequest<Upload> {
    private static final String NO_STREAM_ERROR = "There must be a ContentStream or File to Upload";
    
    protected ContentStream stream;
    protected String filePath;
    
    protected Boolean overwrite;
    protected Boolean cleanup;
    
    public Upload() {
      action = ConfigSetAction.UPLOAD;
      setMethod(SolrRequest.METHOD.POST);
    }

    @Override
    protected Upload getThis() {
      return this;
    }

    /** Optional {@link ConfigSetParams#FILE_PATH} to indicate a single file is being uploaded into an existing configset */
    public final Upload setFilePath(final String filePath) {
      this.filePath = filePath;
      return getThis();
    }

    /** @see #setFilePath */
    public final String getFilePath() {
      return filePath;
    }

    /** 
     * A convinience method for specifying an existing File to use as the upload data.
     *
     * This should either be a ZIP file containing the entire configset being uploaded, or
     * an individual file to upload into an existing configset if {@link #setFilePath} is being used.
     *
     * @see #setUploadStream
     */
    public final Upload setUploadFile(final File file, final String contentType) {
      final FileStream fileStream = new FileStream(file);
      fileStream.setContentType(contentType);
      return setUploadStream(fileStream);
    }

    /** @see ConfigSetParams#OVERWRITE */
    public final Upload setOverwrite(final Boolean overwrite) {
      this.overwrite = overwrite;
      return getThis();
    }

    /** @see #setOverwrite */
    public final Boolean getOverwrite() {
      return overwrite;
    }
    
    /** @see ConfigSetParams#CLEANUP */
    public final Upload setCleanup(final Boolean cleanup) {
      this.cleanup = cleanup;
      return getThis();
    }
    
    /** @see #setCleanup */
    public final Boolean getCleanup() {
      return cleanup;
    }
    
    /** 
     * Specify the ContentStream to upload.
     *
     * This should either be a ZIP file containing the entire configset being uploaded, or
     * an individual file to upload into an existing configset if {@link #setFilePath} is being used.
     *
     * @see #setUploadStream
     */
    public final Upload setUploadStream(final ContentStream stream) {
      this.stream = stream;
      return getThis();
    }
    
    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
      return Collections.singletonList(stream);
    }
    
    @Override
    public RequestWriter.ContentWriter getContentWriter(String expectedType) {
      if (null == stream) {
        throw new NullPointerException(NO_STREAM_ERROR);
      }
      return new RequestWriter.ContentWriter() {
        @Override
        public void write(OutputStream os) throws IOException {
          try(InputStream inStream = stream.getStream()) {
            IOUtils.copy(inStream, os);
          }
        }
        
        @Override
        public String getContentType() {
          return stream.getContentType();
        }
      };
    }
    
    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());

      if (null == stream) {
        throw new NullPointerException(NO_STREAM_ERROR);
      }
      
      params.setNonNull(ConfigSetParams.FILE_PATH, filePath);
      params.setNonNull(ConfigSetParams.CLEANUP, cleanup);
      params.setNonNull(ConfigSetParams.OVERWRITE, overwrite);
          
      return params;
    }
  }

  /**
   * Creates a new config set by cloning an existing "base" configset.
   * To create a new configset from scratch using a ZIP file you wish to upload, use the {@link Upload} command instead
   */
  public static class Create extends ConfigSetSpecificAdminRequest<Create> {
    protected static String PROPERTY_PREFIX = "configSetProp";
    protected String baseConfigSetName;
    protected Properties properties;

    public Create() {
      action = ConfigSetAction.CREATE;
    }

    @Override
    protected Create getThis() {
      return this;
    }

    public final Create setBaseConfigSetName(String baseConfigSetName) {
      this.baseConfigSetName = baseConfigSetName;
      return getThis();
    }

    public final String getBaseConfigSetName() {
      return baseConfigSetName;
    }

    public final Create setNewConfigSetProperties(Properties properties) {
      this.properties = properties;
      return getThis();
    }

    public final Properties getNewConfigSetProperties() {
      return properties;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (baseConfigSetName != null) {
        params.set("baseConfigSet", baseConfigSetName);
      }
      if (properties != null) {
        for (@SuppressWarnings({"rawtypes"})Map.Entry entry : properties.entrySet()) {
          params.set(PROPERTY_PREFIX + "." + entry.getKey().toString(),
              entry.getValue().toString());
        }
      }
      return params;
    }
  }

  // DELETE request
  public static class Delete extends ConfigSetSpecificAdminRequest<Delete> {
    public Delete() {
      action = ConfigSetAction.DELETE;
    }

    @Override
    protected Delete getThis() {
      return this;
    }
  }

  // LIST request
  public static class List extends ConfigSetAdminRequest<List, ConfigSetAdminResponse.List> {
    public List() {
      action = ConfigSetAction.LIST;
    }

    @Override
    protected List getThis() {
      return this;
    }

    @Override
    protected ConfigSetAdminResponse.List createResponse(SolrClient client) {
      return new ConfigSetAdminResponse.List();
    }
  }
}
