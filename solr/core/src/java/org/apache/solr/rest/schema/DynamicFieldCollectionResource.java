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
import org.apache.solr.rest.POSTable;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.noggit.ObjectBuilder;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
public class DynamicFieldCollectionResource extends BaseFieldResource implements GETable, POSTable {
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

  @Override
  public Representation post(Representation entity) {
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
          if ( ! (object instanceof List)) {
            String message = "Invalid JSON type " + object.getClass().getName() + ", expected List of the form"
                + " (ignore the backslashes): [{\"name\":\"*_foo\",\"type\":\"text_general\", ...}, {...}, ...]";
            log.error(message);
            throw new SolrException(ErrorCode.BAD_REQUEST, message);
          } else {
            List<Map<String,Object>> list = (List<Map<String,Object>>)object;
            List<SchemaField> newDynamicFields = new ArrayList<>();
            List<NewFieldArguments> newDynamicFieldArguments = new ArrayList<>();
            ManagedIndexSchema oldSchema = (ManagedIndexSchema)getSchema();
            Map<String,Collection<String>> copyFields = new HashMap<>();
            for (Map<String,Object> map : list) {
              String fieldNamePattern = (String)map.remove(IndexSchema.NAME);
              if (null == fieldNamePattern) {
                String message = "Missing '" + IndexSchema.NAME + "' mapping.";
                log.error(message);
                throw new SolrException(ErrorCode.BAD_REQUEST, message);
              }
              String fieldType = (String)map.remove(IndexSchema.TYPE);
              if (null == fieldType) {
                String message = "Missing '" + IndexSchema.TYPE + "' mapping.";
                log.error(message);
                throw new SolrException(ErrorCode.BAD_REQUEST, message);
              }
              // copyFields:"comma separated list of destination fields"
              Object copies = map.get(IndexSchema.COPY_FIELDS);
              List<String> copyTo = null;
              if (copies != null) {
                if (copies instanceof List){
                  copyTo = (List<String>)copies;
                } else if (copies instanceof String){
                  copyTo = Collections.singletonList(copies.toString());
                } else {
                  String message = "Invalid '" + IndexSchema.COPY_FIELDS + "' type.";
                  log.error(message);
                  throw new SolrException(ErrorCode.BAD_REQUEST, message);
                }
              }
              if (copyTo != null) {
                map.remove(IndexSchema.COPY_FIELDS);
                copyFields.put(fieldNamePattern, copyTo);
              }
              newDynamicFields.add(oldSchema.newDynamicField(fieldNamePattern, fieldType, map));
              newDynamicFieldArguments.add(new NewFieldArguments(fieldNamePattern, fieldType, map));
            }
            IndexSchema newSchema = null;
            boolean firstAttempt = true;
            boolean success = false;
            while ( ! success) {
              try {
                if ( ! firstAttempt) {
                  // If this isn't the first attempt, we must have failed due to
                  // the schema changing in Zk during optimistic concurrency control.
                  // In that case, rerun creating the new fields, because they may
                  // fail now due to changes in the schema.  This behavior is consistent
                  // with what would happen if we locked the schema and the other schema
                  // change went first.
                  newDynamicFields.clear();
                  for (NewFieldArguments args : newDynamicFieldArguments) {
                    newDynamicFields.add(oldSchema.newDynamicField(args.getName(), args.getType(), args.getMap()));
                  }
                }
                firstAttempt = false;
                synchronized (oldSchema.getSchemaUpdateLock()) {
                  newSchema = oldSchema.addDynamicFields(newDynamicFields, copyFields, true);
                  if (null != newSchema) {
                    getSolrCore().setLatestSchema(newSchema);
                    success = true;
                  } else {
                    throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to add dynamic fields.");
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
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
