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
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.rest.GETable;
import org.apache.solr.rest.POSTable;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.noggit.ObjectBuilder;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.solr.common.SolrException.ErrorCode;

/**
 * This class responds to requests at /solr/(corename)/schema/copyfields
 * <p/>
 *
 * To restrict the set of copyFields in the response, specify one or both
 * of the following as query parameters, with values as space and/or comma
 * separated dynamic or explicit field names:
 *
 * <ul>
 *   <li>dest.fl: include copyFields that have one of these as a destination</li>
 *   <li>source.fl: include copyFields that have one of these as a source</li>
 * </ul>
 *
 * If both dest.fl and source.fl are given as query parameters, the copyfields
 * in the response will be restricted to those that match any of the destinations
 * in dest.fl and also match any of the sources in source.fl.
 */
public class CopyFieldCollectionResource extends BaseFieldResource implements GETable, POSTable {
  private static final Logger log = LoggerFactory.getLogger(CopyFieldCollectionResource.class);
  private static final String SOURCE_FIELD_LIST = IndexSchema.SOURCE + "." + CommonParams.FL;
  private static final String DESTINATION_FIELD_LIST = IndexSchema.DESTINATION + "." + CommonParams.FL;

  private Set<String> requestedSourceFields;
  private Set<String> requestedDestinationFields;

  public CopyFieldCollectionResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
    if (isExisting()) {
      String sourceFieldListParam = getSolrRequest().getParams().get(SOURCE_FIELD_LIST);
      if (null != sourceFieldListParam) {
        String[] fields = sourceFieldListParam.trim().split("[,\\s]+");
        if (fields.length > 0) {
          requestedSourceFields = new HashSet<>(Arrays.asList(fields));
          requestedSourceFields.remove(""); // Remove empty values, if any
        }
      }
      String destinationFieldListParam = getSolrRequest().getParams().get(DESTINATION_FIELD_LIST);
      if (null != destinationFieldListParam) {
        String[] fields = destinationFieldListParam.trim().split("[,\\s]+");
        if (fields.length > 0) {
          requestedDestinationFields = new HashSet<>(Arrays.asList(fields));
          requestedDestinationFields.remove(""); // Remove empty values, if any
        }
      }
    }
  }

  @Override
  public Representation get() {
    try {
      getSolrResponse().add(IndexSchema.COPY_FIELDS,
          getSchema().getCopyFieldProperties(true, requestedSourceFields, requestedDestinationFields));
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }

  @Override
  public Representation post(Representation entity) throws ResourceException {
    try {
      if (!getSchema().isMutable()) {
        final String message = "This IndexSchema is not mutable.";
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      } else {
        if (!entity.getMediaType().equals(MediaType.APPLICATION_JSON, true)) {
          String message = "Only media type " + MediaType.APPLICATION_JSON.toString() + " is accepted."
              + "  Request has media type " + entity.getMediaType().toString() + ".";
          log.error(message);
          throw new SolrException(ErrorCode.BAD_REQUEST, message);
        } else {
          Object object = ObjectBuilder.fromJSON(entity.getText());

          if (!(object instanceof List)) {
            String message = "Invalid JSON type " + object.getClass().getName() + ", expected List of the form"
                + " (ignore the backslashes): [{\"source\":\"foo\",\"dest\":\"comma-separated list of targets\"}, {...}, ...]";
            log.error(message);
            throw new SolrException(ErrorCode.BAD_REQUEST, message);
          } else {
            List<Map<String, Object>> list = (List<Map<String, Object>>) object;
            Map<String, Collection<String>> fieldsToCopy = new HashMap<>();
            ManagedIndexSchema oldSchema = (ManagedIndexSchema) getSchema();
            Set<String> malformed = new HashSet<>();
            for (Map<String,Object> map : list) {
              String fieldName = (String)map.get(IndexSchema.SOURCE);
              if (null == fieldName) {
                String message = "Missing '" + IndexSchema.SOURCE + "' mapping.";
                log.error(message);
                throw new SolrException(ErrorCode.BAD_REQUEST, message);
              }
              Object dest = map.get(IndexSchema.DESTINATION);
              List<String> destinations = null;
              if (dest != null) {
                if (dest instanceof List){
                  destinations = (List<String>)dest;
                } else if (dest instanceof String){
                  destinations = Collections.singletonList(dest.toString());
                } else {
                  String message = "Invalid '" + IndexSchema.DESTINATION + "' type.";
                  log.error(message);
                  throw new SolrException(ErrorCode.BAD_REQUEST, message);
                }
              }
              if (destinations == null) {
                malformed.add(fieldName);
              } else {
                fieldsToCopy.put(fieldName, destinations);
              }
            }
            if (malformed.size() > 0){
              StringBuilder message = new StringBuilder("Malformed destination(s) for: ");
              for (String s : malformed) {
                message.append(s).append(", ");
              }
              if (message.length() > 2) {
                message.setLength(message.length() - 2);//drop the last ,
              }
              log.error(message.toString().trim());
              throw new SolrException(ErrorCode.BAD_REQUEST, message.toString().trim());
            }
            IndexSchema newSchema = null;
            boolean success = false;
            while (!success) {
              try {
                synchronized (oldSchema.getSchemaUpdateLock()) {
                  newSchema = oldSchema.addCopyFields(fieldsToCopy);
                  if (null != newSchema) {
                    getSolrCore().setLatestSchema(newSchema);
                    success = true;
                  } else {
                    throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to add fields.");
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
