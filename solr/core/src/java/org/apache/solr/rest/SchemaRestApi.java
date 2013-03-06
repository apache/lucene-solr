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

import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaRestApi extends Application {
  public static final Logger log = LoggerFactory.getLogger(SchemaRestApi.class);
  public static final String FIELDS = "fields";
  public static final String FIELDS_PATH = "/" + FIELDS;
  public static final String DYNAMIC_FIELDS = "dynamicfields";
  public static final String DYNAMIC_FIELDS_PATH = "/" + DYNAMIC_FIELDS;
  public static final String FIELDTYPES = "fieldtypes";
  public static final String FIELDTYPES_PATH = "/" + FIELDTYPES;
  public static final String NAME_VARIABLE = "name";
  public static final String NAME_SEGMENT = "/{" + NAME_VARIABLE + "}";
  public static final String COPY_FIELDS = "copyfields";
  public static final String COPY_FIELDS_PATH = "/" + COPY_FIELDS;

  private Router router;

  public SchemaRestApi() {
    router = new Router(getContext());
  }

  @Override
  public void stop() throws Exception {
    if (router != null) {
      router.stop();
    }
  }

  /**
   * Bind URL paths to the appropriate ServerResource subclass. 
   */
  @Override
  public synchronized Restlet createInboundRoot() {

    log.info("createInboundRoot started");
    
    router.attachDefault(DefaultSchemaResource.class);
    
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

    log.info("createInboundRoot complete");

    return router;
  }
}
