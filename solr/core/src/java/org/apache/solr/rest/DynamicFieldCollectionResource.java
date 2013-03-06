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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
  private final static String INTERNAL_POLY_FIELD_PREFIX = "*" + FieldType.POLY_FIELD_SEPARATOR;

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
      SchemaField[] dynamicFields = getSchema().getDynamicFieldPrototypes();
      List<SimpleOrderedMap<Object>> props = new ArrayList<SimpleOrderedMap<Object>>(dynamicFields.length);
      if (null != getRequestedFields()) {
        if (0 == getRequestedFields().size()) {
          String message = "Empty " + CommonParams.FL + " parameter value";
          throw new SolrException(ErrorCode.BAD_REQUEST, message);
        }
        for (SchemaField prototype : dynamicFields) {
          if (getRequestedFields().containsKey(prototype.getName())) {
            getRequestedFields().put(prototype.getName(), getFieldProperties(prototype));
          }
        }
        // Use the same order as the fl parameter
        for (Map.Entry<String,SimpleOrderedMap<Object>> requestedField : getRequestedFields().entrySet()) {
          SimpleOrderedMap<Object> fieldProperties = requestedField.getValue();
          // Should there be some form of error condition
          // if one or more of the requested fields were not found?
          if (null != fieldProperties) {
            props.add(fieldProperties);
          }
        }
      } else {
        for (SchemaField prototype : dynamicFields) {
          // omit internal polyfields
          if ( ! prototype.getName().startsWith(INTERNAL_POLY_FIELD_PREFIX)) {
            props.add(getFieldProperties(prototype));
          }
        }
      }
      getSolrResponse().add(SchemaRestApi.DYNAMIC_FIELDS, props);
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
