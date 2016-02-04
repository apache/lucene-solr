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
package org.apache.solr.rest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.rest.schema.CopyFieldCollectionResource;
import org.apache.solr.rest.schema.DynamicFieldCollectionResource;
import org.apache.solr.rest.schema.DynamicFieldResource;
import org.apache.solr.rest.schema.FieldCollectionResource;
import org.apache.solr.rest.schema.FieldResource;
import org.apache.solr.rest.schema.FieldTypeCollectionResource;
import org.apache.solr.rest.schema.FieldTypeResource;
import org.apache.solr.schema.IndexSchema;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Restlet servlet handling /&lt;context&gt;/&lt;collection&gt;/schema/* URL paths
 */
public class SolrSchemaRestApi extends Application {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String FIELDS_PATH = "/" + IndexSchema.FIELDS;
  
  public static final String DYNAMIC_FIELDS = IndexSchema.DYNAMIC_FIELDS.toLowerCase(Locale.ROOT);
  public static final String DYNAMIC_FIELDS_PATH = "/" + DYNAMIC_FIELDS;
  
  public static final String FIELDTYPES = IndexSchema.FIELD_TYPES.toLowerCase(Locale.ROOT);
  public static final String FIELDTYPES_PATH = "/" + FIELDTYPES;

  public static final String NAME_SEGMENT = "/{" + IndexSchema.NAME.toLowerCase(Locale.ROOT) + "}";
  
  public static final String COPY_FIELDS = IndexSchema.COPY_FIELDS.toLowerCase(Locale.ROOT);
  public static final String COPY_FIELDS_PATH = "/" + COPY_FIELDS;
  
  /**
   * Returns reserved endpoints under /schema
   */
  public static Set<String> getReservedEndpoints() {
    Set<String> reservedEndpoints = new HashSet<>();
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + FIELDS_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + DYNAMIC_FIELDS_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + FIELDTYPES_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + COPY_FIELDS_PATH);
    return Collections.unmodifiableSet(reservedEndpoints);
  }

  private Router router;

  public SolrSchemaRestApi() {
    router = new Router(getContext());
  }

  @Override
  public void stop() throws Exception {
    if (null != router) {
      router.stop();
    }
  }

  /**
   * Bind URL paths to the appropriate ServerResource subclass. 
   */
  @Override
  public synchronized Restlet createInboundRoot() {

    log.info("createInboundRoot started for /schema");


    router.attach(FIELDS_PATH, FieldCollectionResource.class);
    // Allow a trailing slash on collection requests
    router.attach(FIELDS_PATH + "/", FieldCollectionResource.class);
    router.attach(FIELDS_PATH + NAME_SEGMENT, FieldResource.class);

    router.attach(DYNAMIC_FIELDS_PATH, DynamicFieldCollectionResource.class);
    // Allow a trailing slash on collection requests
    router.attach(DYNAMIC_FIELDS_PATH + "/", DynamicFieldCollectionResource.class);
    router.attach(DYNAMIC_FIELDS_PATH + NAME_SEGMENT, DynamicFieldResource.class);

    router.attach(FIELDTYPES_PATH, FieldTypeCollectionResource.class);
    // Allow a trailing slash on collection requests
    router.attach(FIELDTYPES_PATH + "/", FieldTypeCollectionResource.class);
    router.attach(FIELDTYPES_PATH + NAME_SEGMENT, FieldTypeResource.class);

    router.attach(COPY_FIELDS_PATH, CopyFieldCollectionResource.class);
    // Allow a trailing slash on collection requests
    router.attach(COPY_FIELDS_PATH + "/", CopyFieldCollectionResource.class);

    router.attachDefault(RestManager.ManagedEndpoint.class);
    
    // attach all the dynamically registered schema resources
    RestManager.getRestManager(SolrRequestInfo.getRequestInfo())
        .attachManagedResources(RestManager.SCHEMA_BASE_PATH, router);

    log.info("createInboundRoot complete for /schema");

    return router;
  }  
}
