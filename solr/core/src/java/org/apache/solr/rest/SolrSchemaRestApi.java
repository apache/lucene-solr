package org.apache.solr.rest;
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

import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.rest.schema.CopyFieldCollectionResource;
import org.apache.solr.rest.schema.SchemaResource;
import org.apache.solr.rest.schema.DefaultSearchFieldResource;
import org.apache.solr.rest.schema.DynamicFieldCollectionResource;
import org.apache.solr.rest.schema.DynamicFieldResource;
import org.apache.solr.rest.schema.FieldCollectionResource;
import org.apache.solr.rest.schema.FieldResource;
import org.apache.solr.rest.schema.FieldTypeCollectionResource;
import org.apache.solr.rest.schema.FieldTypeResource;
import org.apache.solr.rest.schema.SchemaNameResource;
import org.apache.solr.rest.schema.SchemaSimilarityResource;
import org.apache.solr.rest.schema.SchemaVersionResource;
import org.apache.solr.rest.schema.SchemaZkVersionResource;
import org.apache.solr.rest.schema.SolrQueryParserDefaultOperatorResource;
import org.apache.solr.rest.schema.SolrQueryParserResource;
import org.apache.solr.rest.schema.UniqueKeyFieldResource;
import org.apache.solr.schema.IndexSchema;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Restlet servlet handling /&lt;context&gt;/&lt;collection&gt;/schema/* URL paths
 */
public class SolrSchemaRestApi extends Application {
  public static final Logger log = LoggerFactory.getLogger(SolrSchemaRestApi.class);
  public static final String FIELDS_PATH = "/" + IndexSchema.FIELDS;
  
  public static final String DYNAMIC_FIELDS = IndexSchema.DYNAMIC_FIELDS.toLowerCase(Locale.ROOT);
  public static final String DYNAMIC_FIELDS_PATH = "/" + DYNAMIC_FIELDS;
  
  public static final String FIELDTYPES = IndexSchema.FIELD_TYPES.toLowerCase(Locale.ROOT);
  public static final String FIELDTYPES_PATH = "/" + FIELDTYPES;

  public static final String NAME_PATH = "/" + IndexSchema.NAME.toLowerCase(Locale.ROOT);
  public static final String NAME_SEGMENT = "/{" + IndexSchema.NAME.toLowerCase(Locale.ROOT) + "}";
  
  public static final String COPY_FIELDS = IndexSchema.COPY_FIELDS.toLowerCase(Locale.ROOT);
  public static final String COPY_FIELDS_PATH = "/" + COPY_FIELDS;
  
  public static final String VERSION_PATH = "/" + IndexSchema.VERSION.toLowerCase(Locale.ROOT);
  
  public static final String DEFAULT_SEARCH_FIELD = IndexSchema.DEFAULT_SEARCH_FIELD.toLowerCase(Locale.ROOT);
  public static final String DEFAULT_SEARCH_FIELD_PATH = "/" + DEFAULT_SEARCH_FIELD;
  
  public static final String SIMILARITY_PATH = "/" + IndexSchema.SIMILARITY.toLowerCase(Locale.ROOT);
  
  public static final String SOLR_QUERY_PARSER = IndexSchema.SOLR_QUERY_PARSER.toLowerCase(Locale.ROOT);
  public static final String SOLR_QUERY_PARSER_PATH = "/" + SOLR_QUERY_PARSER;
  
  public static final String DEFAULT_OPERATOR = IndexSchema.DEFAULT_OPERATOR.toLowerCase(Locale.ROOT);
  public static final String DEFAULT_OPERATOR_PATH = SOLR_QUERY_PARSER_PATH + "/" + DEFAULT_OPERATOR;
  
  public static final String UNIQUE_KEY_FIELD = IndexSchema.UNIQUE_KEY.toLowerCase(Locale.ROOT);
  public static final String UNIQUE_KEY_FIELD_PATH = "/" + UNIQUE_KEY_FIELD;
  
  public static final String ZK_VERSION_PATH = "/zkversion";  

  /**
   * Returns reserved endpoints under /schema
   */
  public static Set<String> getReservedEndpoints() {
    Set<String> reservedEndpoints = new HashSet<>();
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + FIELDS_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + DYNAMIC_FIELDS_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + FIELDTYPES_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + NAME_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + COPY_FIELDS_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + VERSION_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + DEFAULT_SEARCH_FIELD_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + SIMILARITY_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + SOLR_QUERY_PARSER_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + DEFAULT_OPERATOR_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + UNIQUE_KEY_FIELD_PATH);
    reservedEndpoints.add(RestManager.SCHEMA_BASE_PATH + ZK_VERSION_PATH);
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

    router.attach("", SchemaResource.class);
    // Allow a trailing slash on full-schema requests
    router.attach("/", SchemaResource.class);
    
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
    
    router.attach(NAME_PATH, SchemaNameResource.class);
    
    router.attach(VERSION_PATH, SchemaVersionResource.class);
    
    router.attach(UNIQUE_KEY_FIELD_PATH, UniqueKeyFieldResource.class);

    router.attach(DEFAULT_SEARCH_FIELD_PATH, DefaultSearchFieldResource.class);
    
    router.attach(SIMILARITY_PATH, SchemaSimilarityResource.class);

    // At present solrQueryParser only contains defaultOperator, but there may be more children in the future
    router.attach(SOLR_QUERY_PARSER_PATH, SolrQueryParserResource.class);
    router.attach(DEFAULT_OPERATOR_PATH, SolrQueryParserDefaultOperatorResource.class);

    router.attach(ZK_VERSION_PATH, SchemaZkVersionResource.class);
    
    router.attachDefault(RestManager.ManagedEndpoint.class);
    
    // attach all the dynamically registered schema resources
    RestManager.getRestManager(SolrRequestInfo.getRequestInfo())
        .attachManagedResources(RestManager.SCHEMA_BASE_PATH, router);

    log.info("createInboundRoot complete for /schema");

    return router;
  }  
}
