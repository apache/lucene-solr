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
import org.apache.solr.schema.SchemaField;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class responds to requests at /solr/(corename)/schema/fields
 * <p/>
 * Two query parameters are supported:
 * <ul>
 *   <li>
 *     "fl": a comma- and/or space-separated list of fields to send properties
 *     for in the response, rather than the default: all of them.
 *   </li>
 *   <li>
 *     "includeDynamic": if the "fl" parameter is specified, matching dynamic
 *     fields are included in the response and identified with the "dynamicBase"
 *     property.  If the "fl" parameter is not specified, the "includeDynamic"
 *     query parameter is ignored.
 *   </li>
 * </ul>
 */
public class FieldCollectionResource extends BaseFieldResource implements GETable {
  private static final Logger log = LoggerFactory.getLogger(FieldCollectionResource.class);
  private boolean includeDynamic;
  
  public FieldCollectionResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
    includeDynamic = getSolrRequest().getParams().getBool(INCLUDE_DYNAMIC_PARAM, false);
  }

  @Override
  public Representation get() {
    try {
      final List<SimpleOrderedMap<Object>> props = new ArrayList<SimpleOrderedMap<Object>>();
      if (null == getRequestedFields()) {
        SortedSet<String> fieldNames = new TreeSet<String>(getSchema().getFields().keySet());
        for (String fieldName : fieldNames) {
          props.add(getFieldProperties(getSchema().getFields().get(fieldName)));
        }
      } else {
        if (0 == getRequestedFields().size()) {
          String message = "Empty " + CommonParams.FL + " parameter value";
          throw new SolrException(ErrorCode.BAD_REQUEST, message);
        }
        // Use the same order as the fl parameter
        for (String fieldName : getRequestedFields()) {
          final SchemaField field;
          if (includeDynamic) {
            field = getSchema().getFieldOrNull(fieldName);
          } else {
            field = getSchema().getFields().get(fieldName);
          }
          if (null == field) {
            log.info("Requested field '" + fieldName + "' not found.");
          } else {
            props.add(getFieldProperties(field));
          }
        }
      }
      getSolrResponse().add(SchemaRestApi.FIELDS, props);
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
