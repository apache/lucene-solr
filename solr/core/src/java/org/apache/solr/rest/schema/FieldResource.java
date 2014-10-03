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

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.GETable;
import org.apache.solr.rest.PUTable;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.noggit.ObjectBuilder;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * This class responds to requests at /solr/(corename)/schema/fields/(fieldname)
 * where "fieldname" is the name of a field.
 * <p/>
 * The GET method returns properties for the given fieldname.
 * The "includeDynamic" query parameter, if specified, will cause the
 * dynamic field matching the given fieldname to be returned if fieldname
 * is not explicitly declared in the schema.
 * <p/>
 * The PUT method accepts field addition requests in JSON format.
 */
public class FieldResource extends BaseFieldResource implements GETable, PUTable {
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
      fieldName = (String) getRequestAttributes().get(IndexSchema.NAME);
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

  /**
   * Accepts JSON add field request, to URL
   */
  @Override
  public Representation put(Representation entity) {
    try {
      if (!getSchema().isMutable()) {
        final String message = "This IndexSchema is not mutable.";
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      } else {
        if (null == entity.getMediaType()) {
          entity.setMediaType(MediaType.APPLICATION_JSON);
        }
        if (!entity.getMediaType().equals(MediaType.APPLICATION_JSON, true)) {
          String message = "Only media type " + MediaType.APPLICATION_JSON.toString() + " is accepted."
              + "  Request has media type " + entity.getMediaType().toString() + ".";
          log.error(message);
          throw new SolrException(ErrorCode.BAD_REQUEST, message);
        } else {
          Object object = ObjectBuilder.fromJSON(entity.getText());
          if (!(object instanceof Map)) {
            String message = "Invalid JSON type " + object.getClass().getName() + ", expected Map of the form"
                + " (ignore the backslashes): {\"type\":\"text_general\", ...}, either with or"
                + " without a \"name\" mapping.  If the \"name\" is specified, it must match the"
                + " name given in the request URL: /schema/fields/(name)";
            log.error(message);
            throw new SolrException(ErrorCode.BAD_REQUEST, message);
          } else {
            Map<String, Object> map = (Map<String, Object>) object;
            if (1 == map.size() && map.containsKey(IndexSchema.FIELD)) {
              map = (Map<String, Object>) map.get(IndexSchema.FIELD);
            }
            String bodyFieldName;
            if (null != (bodyFieldName = (String) map.remove(IndexSchema.NAME)) && !fieldName.equals(bodyFieldName)) {
              String message = "Field name in the request body '" + bodyFieldName
                  + "' doesn't match field name in the request URL '" + fieldName + "'";
              log.error(message);
              throw new SolrException(ErrorCode.BAD_REQUEST, message);
            } else {
              String fieldType;
              if (null == (fieldType = (String) map.remove(IndexSchema.TYPE))) {
                String message = "Missing '" + IndexSchema.TYPE + "' mapping.";
                log.error(message);
                throw new SolrException(ErrorCode.BAD_REQUEST, message);
              } else {
                ManagedIndexSchema oldSchema = (ManagedIndexSchema) getSchema();
                Object copies = map.get(IndexSchema.COPY_FIELDS);
                List<String> copyFieldNames = null;
                if (copies != null) {
                  if (copies instanceof List) {
                    copyFieldNames = (List<String>) copies;
                  } else if (copies instanceof String) {
                    copyFieldNames = Collections.singletonList(copies.toString());
                  } else {
                    String message = "Invalid '" + IndexSchema.COPY_FIELDS + "' type.";
                    log.error(message);
                    throw new SolrException(ErrorCode.BAD_REQUEST, message);
                  }
                }
                if (copyFieldNames != null) {
                  map.remove(IndexSchema.COPY_FIELDS);
                }

                IndexSchema newSchema = null;
                boolean success = false;
                while (!success) {
                  try {
                    SchemaField newField = oldSchema.newField(fieldName, fieldType, map);
                    synchronized (oldSchema.getSchemaUpdateLock()) {
                      newSchema = oldSchema.addField(newField, copyFieldNames);
                      if (null != newSchema) {
                        getSolrCore().setLatestSchema(newSchema);
                        success = true;
                      } else {
                        throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to add field.");
                      }
                    }
                  } catch (ManagedIndexSchema.SchemaChangedInZkException e) {
                    log.debug("Schema changed while processing request, retrying");
                    oldSchema = (ManagedIndexSchema)getSolrCore().getLatestSchema();
                  }
                }
                waitForSchemaUpdateToPropagate(newSchema);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
