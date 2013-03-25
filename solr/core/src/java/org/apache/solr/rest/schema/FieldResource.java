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
import org.apache.solr.rest.GETable;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * This class responds to requests at /solr/(corename)/schema/fields/(fieldname)
 * where "fieldname" is the name of a field.
 * <p/>
 * The GET method returns properties for the given fieldname.
 * The "includeDynamic" query parameter, if specified, will cause the
 * dynamic field matching the given fieldname to be returned if fieldname
 * is not explicitly declared in the schema.
 */
public class FieldResource extends BaseFieldResource implements GETable {
  private static final Logger log = LoggerFactory.getLogger(FieldResource.class);
  
  private boolean includeDynamic;
  private String fieldName;

  public FieldResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
    if (isExisting()) {
      includeDynamic = getSolrRequest().getParams().getBool(INCLUDE_DYNAMIC_PARAM, false);
      fieldName = (String)getRequestAttributes().get(IndexSchema.NAME);
      try {
        fieldName = null == fieldName ? "" : urlDecode(fieldName.trim()).trim();
      } catch (UnsupportedEncodingException e) {
        throw new ResourceException(e);
      }
    }
  }

  @Override
  public Representation get() {
    try {
      if (fieldName.isEmpty()) {
        final String message = "Field name is missing";
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      } else {
        final SchemaField field;
        if (includeDynamic) {
          field = getSchema().getFieldOrNull(fieldName);
        } else {
          field = getSchema().getFields().get(fieldName);
        }
        if (null == field) {
          final String message = "Field '" + fieldName + "' not found.";
          throw new SolrException(ErrorCode.NOT_FOUND, message);
        } else {
          getSolrResponse().add(IndexSchema.FIELD, getFieldProperties(field));
        }
      }
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
