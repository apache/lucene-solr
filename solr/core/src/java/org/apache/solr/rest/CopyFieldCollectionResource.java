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


import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.IndexSchema;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

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
  private static final String SOURCE = "source";
  private static final String DESTINATION = "dest";
  private static final String SOURCE_FIELD_LIST = SOURCE + "." + CommonParams.FL;
  private static final String DESTINATION_FIELD_LIST = DESTINATION + "." + CommonParams.FL;
  private static final String MAX_CHARS = "maxChars";
  private static final String SOURCE_DYNAMIC_BASE = "sourceDynamicBase";
  private static final String DESTINATION_DYNAMIC_BASE = "destDynamicBase";

  private Set<String> sourceFields;
  private Set<String> destinationFields;

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
          sourceFields = new HashSet<String>(Arrays.asList(fields));
          sourceFields.remove(""); // Remove empty values, if any
        }
      }
      String destinationFieldListParam = getSolrRequest().getParams().get(DESTINATION_FIELD_LIST);
      if (null != destinationFieldListParam) {
        String[] fields = destinationFieldListParam.trim().split("[,\\s]+");
        if (fields.length > 0) {
          destinationFields = new HashSet<String>(Arrays.asList(fields));
          destinationFields.remove(""); // Remove empty values, if any
        }
      }
    }
  }

  @Override
  public Representation get() {
    try {
      final List<SimpleOrderedMap<Object>> props = new ArrayList<SimpleOrderedMap<Object>>();
      SortedMap<String,List<CopyField>> sortedCopyFields
          = new TreeMap<String, List<CopyField>>(getSchema().getCopyFieldsMap());
      for (List<CopyField> copyFields : sortedCopyFields.values()) {
        Collections.sort(copyFields, new Comparator<CopyField>() {
          @Override
          public int compare(CopyField cf1, CopyField cf2) {
            // source should all be the same => already sorted
            return cf1.getDestination().getName().compareTo(cf2.getDestination().getName());
          }
        });
        for (CopyField copyField : copyFields) {
          final String source = copyField.getSource().getName();
          final String destination = copyField.getDestination().getName();
          if (   (null == sourceFields      || sourceFields.contains(source))
              && (null == destinationFields || destinationFields.contains(destination))) {
            SimpleOrderedMap<Object> copyFieldProps = new SimpleOrderedMap<Object>();
            copyFieldProps.add(SOURCE, source);
            copyFieldProps.add(DESTINATION, destination);
            if (0 != copyField.getMaxChars()) {
              copyFieldProps.add(MAX_CHARS, copyField.getMaxChars());
            }
            props.add(copyFieldProps);
          }
        }
      }
      for (IndexSchema.DynamicCopy dynamicCopy : getSchema().getDynamicCopyFields()) {
        final String source = dynamicCopy.getRegex();
        final String destination = dynamicCopy.getDestFieldName();
        if (   (null == sourceFields      || sourceFields.contains(source))
            && (null == destinationFields || destinationFields.contains(destination))) {
          SimpleOrderedMap<Object> dynamicCopyProps = new SimpleOrderedMap<Object>();
          
          dynamicCopyProps.add(SOURCE, dynamicCopy.getRegex());
          IndexSchema.DynamicField sourceDynamicBase = dynamicCopy.getSourceDynamicBase();
          if (null != sourceDynamicBase) {
            dynamicCopyProps.add(SOURCE_DYNAMIC_BASE, sourceDynamicBase.getRegex());
          }
          
          dynamicCopyProps.add(DESTINATION, dynamicCopy.getDestFieldName());
          IndexSchema.DynamicField destDynamicBase = dynamicCopy.getDestDynamicBase();
          if (null != destDynamicBase) {
            dynamicCopyProps.add(DESTINATION_DYNAMIC_BASE, destDynamicBase.getRegex());
          }
          
          if (0 != dynamicCopy.getMaxChars()) {
            dynamicCopyProps.add(MAX_CHARS, dynamicCopy.getMaxChars());
          }

          props.add(dynamicCopyProps);
        }
      }
      getSolrResponse().add(SchemaRestApi.COPY_FIELDS, props);
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
