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
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class responds to requests at /solr/(corename)/schema/fields
 * <p/>
 * One query parameters are supported:
 * <ul>
 *   <li>
 *     "fl": a comma- and/or space-separated list of fields to send properties
 *     for in the response, rather than the default: all of them.
 *   </li>
 * </ul>
 */
public class FieldCollectionResource extends BaseFieldResource implements GETable {
  private static final Logger log = LoggerFactory.getLogger(FieldCollectionResource.class);
  
  public FieldCollectionResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
  }

  @Override
  public Representation get() {
    try {
      // Get all explicitly defined fields from the schema
      Set<String> fieldNames = new HashSet<String>(getSchema().getFields().keySet());

      final List<SimpleOrderedMap<Object>> fieldCollectionProperties = new ArrayList<SimpleOrderedMap<Object>>(fieldNames.size());

      if (null == getRequestedFields()) {
        for (String fieldName : fieldNames) {
          fieldCollectionProperties.add(getFieldProperties(getSchema().getFieldOrNull(fieldName)));
        }
      } else {
        if (0 == getRequestedFields().size()) {
          String message = "Empty " + CommonParams.FL + " parameter value";
          throw new SolrException(ErrorCode.BAD_REQUEST, message);
        }
        for (String field : fieldNames) {
          if (getRequestedFields().containsKey(field)) {
            getRequestedFields().put(field, getFieldProperties(getSchema().getFieldOrNull(field)));
          }
        }
        // Use the same order as the fl parameter
        for (SimpleOrderedMap<Object> fieldProperties : getRequestedFields().values()) {
          // Should there be some form of error condition
          // if one or more of the requested fields were not found?
          if (null != fieldProperties) {
            fieldCollectionProperties.add(fieldProperties);
          }
        }
      }
      getSolrResponse().add(SchemaRestApi.FIELDS, fieldCollectionProperties);
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
