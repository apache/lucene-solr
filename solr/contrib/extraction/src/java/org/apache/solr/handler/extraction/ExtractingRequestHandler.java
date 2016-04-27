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
package org.apache.solr.handler.extraction;


import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.mime.MimeTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler for rich documents like PDF or Word or any other file format that Tika handles that need the text to be extracted
 * first from the document.
 */
public class ExtractingRequestHandler extends ContentStreamHandlerBase implements SolrCoreAware , PermissionNameProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PARSE_CONTEXT_CONFIG = "parseContext.config";
  public static final String CONFIG_LOCATION = "tika.config";
  public static final String DATE_FORMATS = "date.formats";

  protected TikaConfig config;
  protected ParseContextConfig parseContextConfig;


  protected Collection<String> dateFormats = ExtractionDateUtil.DEFAULT_DATE_FORMATS;
  protected SolrContentHandlerFactory factory;


  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  @Override
  public void init(NamedList args) {
    super.init(args);
  }

  @Override
  public void inform(SolrCore core) {
    if (initArgs != null) {
      //if relative,then relative to config dir, otherwise, absolute path
      String tikaConfigLoc = (String) initArgs.get(CONFIG_LOCATION);
      if (tikaConfigLoc != null) {
        File configFile = new File(tikaConfigLoc);
        if (configFile.isAbsolute() == false) {
          configFile = new File(core.getResourceLoader().getConfigDir(), configFile.getPath());
        }
        try {
          config = new TikaConfig(configFile);
        } catch (Exception e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }

      String parseContextConfigLoc = (String) initArgs.get(PARSE_CONTEXT_CONFIG);
      if (parseContextConfigLoc != null) {
        try {
          parseContextConfig = new ParseContextConfig(core.getResourceLoader(), parseContextConfigLoc);
        } catch (Exception e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }

      NamedList configDateFormats = (NamedList) initArgs.get(DATE_FORMATS);
      if (configDateFormats != null && configDateFormats.size() > 0) {
        dateFormats = new HashSet<>();
        Iterator<Map.Entry> it = configDateFormats.iterator();
        while (it.hasNext()) {
          String format = (String) it.next().getValue();
          log.info("Adding Date Format: " + format);
          dateFormats.add(format);
        }
      }
    }
    if (config == null) {
      try {
        config = getDefaultConfig(core.getResourceLoader().getClassLoader());
      } catch (MimeTypeException | IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }
    if (parseContextConfig == null) {
      parseContextConfig = new ParseContextConfig();
    }
    factory = createFactory();
  }

  private TikaConfig getDefaultConfig(ClassLoader classLoader) throws MimeTypeException, IOException {
    return new TikaConfig(classLoader);
  }

  protected SolrContentHandlerFactory createFactory() {
    return new SolrContentHandlerFactory(dateFormats);
  }


  @Override
  protected ContentStreamLoader newLoader(SolrQueryRequest req, UpdateRequestProcessor processor) {
    return new ExtractingDocumentLoader(req, processor, config, parseContextConfig, factory);
  }

  // ////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getDescription() {
    return "Add/Update Rich document";
  }
}


