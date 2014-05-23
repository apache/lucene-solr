package org.apache.solr.rest.schema;
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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.rest.GETable;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class responds to requests at /solr/(corename)/schema/dynamicfields
 * <p/>
 * To restrict the set of dynamic fields in the response, specify a comma
 * and/or space separated list of dynamic field patterns in the "fl" query
 * parameter. 
 */
public class DynamicFieldCollectionResource extends BaseFieldResource implements GETable {
  private static final Logger log = LoggerFactory.getLogger(DynamicFieldCollectionResource.class);

  public DynamicFieldCollectionResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
  }

  @Override
  public Representation get() {
    
    try {
      List<SimpleOrderedMap<Object>> props = new ArrayList<>();
      if (null == getRequestedFields()) {
        for (IndexSchema.DynamicField dynamicField : getSchema().getDynamicFields()) {
          if ( ! dynamicField.getRegex().startsWith(IndexSchema.INTERNAL_POLY_FIELD_PREFIX)) { // omit internal polyfields
            props.add(getFieldProperties(dynamicField.getPrototype()));
          }
        }
      } else {
        if (0 == getRequestedFields().size()) {
          String message = "Empty " + CommonParams.FL + " parameter value";
          throw new SolrException(ErrorCode.BAD_REQUEST, message);
        }
        Map<String,SchemaField> dynamicFieldsByName = new HashMap<>();
        for (IndexSchema.DynamicField dynamicField : getSchema().getDynamicFields()) {
          dynamicFieldsByName.put(dynamicField.getRegex(), dynamicField.getPrototype());
        }
        // Use the same order as the fl parameter
        for (String dynamicFieldName : getRequestedFields()) {
          final SchemaField dynamicSchemaField = dynamicFieldsByName.get(dynamicFieldName);
          if (null == dynamicSchemaField) {
            log.info("Requested dynamic field '" + dynamicFieldName + "' not found.");
          } else {
            props.add(getFieldProperties(dynamicSchemaField));
          }
        }
      }
      getSolrResponse().add(IndexSchema.DYNAMIC_FIELDS, props);
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
