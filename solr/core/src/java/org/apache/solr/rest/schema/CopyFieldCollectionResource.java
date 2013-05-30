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


import org.apache.solr.common.params.CommonParams;
import org.apache.solr.rest.GETable;
import org.apache.solr.schema.IndexSchema;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This class responds to requests at /solr/(corename)/schema/copyfields
 * 
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
public class CopyFieldCollectionResource extends BaseFieldResource implements GETable {
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
          requestedSourceFields = new HashSet<String>(Arrays.asList(fields));
          requestedSourceFields.remove(""); // Remove empty values, if any
        }
      }
      String destinationFieldListParam = getSolrRequest().getParams().get(DESTINATION_FIELD_LIST);
      if (null != destinationFieldListParam) {
        String[] fields = destinationFieldListParam.trim().split("[,\\s]+");
        if (fields.length > 0) {
          requestedDestinationFields = new HashSet<String>(Arrays.asList(fields));
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
}
