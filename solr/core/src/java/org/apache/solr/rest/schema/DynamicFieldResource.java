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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * This class responds to requests at /solr/(corename)/schema/dynamicfields/(pattern)
 * where pattern is a field name pattern (with an asterisk at the beginning or the end).
 */
public class DynamicFieldResource extends BaseFieldResource implements GETable, PUTable {
  private static final Logger log = LoggerFactory.getLogger(DynamicFieldResource.class);

  private String fieldNamePattern;

  public DynamicFieldResource() {
    super();
  }

  /**
   * Gets the field name pattern from the request attribute where it's stored by Restlet. 
   */
  @Override
  public void doInit() throws ResourceException {
    super.doInit();
    if (isExisting()) {
      fieldNamePattern = (String)getRequestAttributes().get(IndexSchema.NAME);
      try {
        fieldNamePattern = null == fieldNamePattern ? "" : urlDecode(fieldNamePattern.trim()).trim();
      } catch (UnsupportedEncodingException e) {
        throw new ResourceException(e);
      }
    }
  }

  @Override
  public Representation get() {
    try {
      if (fieldNamePattern.isEmpty()) {
        final String message = "Dynamic field name is missing";
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      } else {
        SchemaField field = null;
        for (SchemaField prototype : getSchema().getDynamicFieldPrototypes()) {
          if (prototype.getName().equals(fieldNamePattern)) {
            field = prototype;
            break;
          }
        }
        if (null == field) {
          final String message = "Dynamic field '" + fieldNamePattern + "' not found.";
          throw new SolrException(ErrorCode.NOT_FOUND, message);
        } else {
          getSolrResponse().add(IndexSchema.DYNAMIC_FIELD, getFieldProperties(field));
        }
      }
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }

  /**
   * Accepts JSON add dynamic field request
   */
  @Override
  public Representation put(Representation entity) {
    try {
      if ( ! getSchema().isMutable()) {
        final String message = "This IndexSchema is not mutable.";
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      } else {
        if (null == entity.getMediaType()) {
          entity.setMediaType(MediaType.APPLICATION_JSON);
        }
        if ( ! entity.getMediaType().equals(MediaType.APPLICATION_JSON, true)) {
          String message = "Only media type " + MediaType.APPLICATION_JSON.toString() + " is accepted."
                         + "  Request has media type " + entity.getMediaType().toString() + ".";
          log.error(message);
          throw new SolrException(ErrorCode.BAD_REQUEST, message);
        } else {
          Object object = ObjectBuilder.fromJSON(entity.getText());
          if ( ! (object instanceof Map)) {
            String message = "Invalid JSON type " + object.getClass().getName() + ", expected Map of the form"
                           + " (ignore the backslashes): {\"type\":\"text_general\", ...}, either with or"
                           + " without a \"name\" mapping.  If the \"name\" is specified, it must match the"
                           + " name given in the request URL: /schema/dynamicfields/(name)";
            log.error(message);
            throw new SolrException(ErrorCode.BAD_REQUEST, message);
          } else {
            Map<String,Object> map = (Map<String,Object>)object;
            if (1 == map.size() && map.containsKey(IndexSchema.DYNAMIC_FIELD)) {
              map = (Map<String,Object>)map.get(IndexSchema.DYNAMIC_FIELD);
            }
            String bodyFieldName;
            if (null != (bodyFieldName = (String)map.remove(IndexSchema.NAME))
                && ! fieldNamePattern.equals(bodyFieldName)) {
              String message = "Dynamic field name in the request body '" + bodyFieldName
                  + "' doesn't match dynamic field name in the request URL '" + fieldNamePattern + "'";
              log.error(message);
              throw new SolrException(ErrorCode.BAD_REQUEST, message);
            } else {
              String fieldType;
              if (null == (fieldType = (String) map.remove(IndexSchema.TYPE))) {
                String message = "Missing '" + IndexSchema.TYPE + "' mapping.";
                log.error(message);
                throw new SolrException(ErrorCode.BAD_REQUEST, message);
              } else {
                ManagedIndexSchema oldSchema = (ManagedIndexSchema)getSchema();
                Object copies = map.get(IndexSchema.COPY_FIELDS);
                Collection<String> copyFieldNames = null;
                if (copies != null) {
                  if (copies instanceof List) {
                    copyFieldNames = (List<String>)copies;
                  } else if (copies instanceof String) {
                    copyFieldNames = singletonList(copies.toString());
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
                while ( ! success) {
                  try {
                    SchemaField newDynamicField = oldSchema.newDynamicField(fieldNamePattern, fieldType, map);
                    synchronized (oldSchema.getSchemaUpdateLock()) {
                      newSchema = oldSchema.addDynamicFields(singletonList(newDynamicField), singletonMap(newDynamicField.getName(), copyFieldNames), true);
                      if (null != newSchema) {
                        getSolrCore().setLatestSchema(newSchema);
                        success = true;
                      } else {
                        throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to add dynamic field.");
                      }
                    }
                  } catch (ManagedIndexSchema.SchemaChangedInZkException e) {
                    log.debug("Schema changed while processing request, retrying");
                    oldSchema = (ManagedIndexSchema)getSolrCore().getLatestSchema();
                  }
                }
                // if in cloud mode, wait for schema updates to propagate to all replicas
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
