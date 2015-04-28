package org.apache.solr.update.processor;

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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @lucene.experimental
 */
public class AtomicUpdateDocumentMerger {
  
  private final static Logger log = LoggerFactory.getLogger(AtomicUpdateDocumentMerger.class);
  
  protected final IndexSchema schema;
  protected final SchemaField idField;
  
  public AtomicUpdateDocumentMerger(SolrQueryRequest queryReq) {
    schema = queryReq.getSchema();
    idField = schema.getUniqueKeyField();
  }
  
  /**
   * Utility method that examines the SolrInputDocument in an AddUpdateCommand
   * and returns true if the documents contains atomic update instructions.
   */
  public static boolean isAtomicUpdate(final AddUpdateCommand cmd) {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    for (SolrInputField sif : sdoc.values()) {
      if (sif.getValue() instanceof Map) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Merges the fromDoc into the toDoc using the atomic update syntax.
   * 
   * @param fromDoc SolrInputDocument which will merged into the toDoc
   * @param toDoc the final SolrInputDocument that will be mutated with the values from the fromDoc atomic commands
   * @return toDoc with mutated values
   */
  public SolrInputDocument merge(final SolrInputDocument fromDoc, SolrInputDocument toDoc) {
    for (SolrInputField sif : fromDoc.values()) {
     Object val = sif.getValue();
      if (val instanceof Map) {
        for (Entry<String,Object> entry : ((Map<String,Object>) val).entrySet()) {
          String key = entry.getKey();
          Object fieldVal = entry.getValue();
          boolean updateField = false;
          switch (key) {
            case "add":
              updateField = true;
              doAdd(toDoc, sif, fieldVal);
              break;
            case "set":
              updateField = true;
              doSet(toDoc, sif, fieldVal);
              break;
            case "remove":
              updateField = true;
              doRemove(toDoc, sif, fieldVal);
              break;
            case "removeregex":
              updateField = true;
              doRemoveRegex(toDoc, sif, fieldVal);
              break;
            case "inc":
              updateField = true;
              doInc(toDoc, sif, fieldVal);
              break;
            default:
              //Perhaps throw an error here instead?
              log.warn("Unknown operation for the an atomic update, operation ignored: " + key);
              break;
          }
          // validate that the field being modified is not the id field.
          if (updateField && idField.getName().equals(sif.getName())) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid update of id field: " + sif);
          }

        }
      } else {
        // normal fields are treated as a "set"
        toDoc.put(sif.getName(), sif);
      }
    }
    
    return toDoc;
  }
  
  protected void doSet(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    toDoc.setField(sif.getName(), fieldVal, sif.getBoost());
  }

  protected void doAdd(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    toDoc.addField(sif.getName(), fieldVal, sif.getBoost());
  }

  protected void doInc(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    SolrInputField numericField = toDoc.get(sif.getName());
    if (numericField == null) {
      toDoc.setField(sif.getName(),  fieldVal, sif.getBoost());
    } else {
      // TODO: fieldtype needs externalToObject?
      String oldValS = numericField.getFirstValue().toString();
      SchemaField sf = schema.getField(sif.getName());
      BytesRefBuilder term = new BytesRefBuilder();
      sf.getType().readableToIndexed(oldValS, term);
      Object oldVal = sf.getType().toObject(sf, term.get());

      String fieldValS = fieldVal.toString();
      Number result;
      if (oldVal instanceof Long) {
        result = ((Long) oldVal).longValue() + Long.parseLong(fieldValS);
      } else if (oldVal instanceof Float) {
        result = ((Float) oldVal).floatValue() + Float.parseFloat(fieldValS);
      } else if (oldVal instanceof Double) {
        result = ((Double) oldVal).doubleValue() + Double.parseDouble(fieldValS);
      } else {
        // int, short, byte
        result = ((Integer) oldVal).intValue() + Integer.parseInt(fieldValS);
      }

      toDoc.setField(sif.getName(),  result, sif.getBoost());
    }
  }
  
  protected void doRemove(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    final String name = sif.getName();
    SolrInputField existingField = toDoc.get(name);
    if(existingField == null) return;
    SchemaField sf = schema.getField(name);

    if (sf != null) {
      final Collection<Object> original = existingField.getValues();
      if (fieldVal instanceof Collection) {
        for (Object object : (Collection)fieldVal){
          original.remove(sf.getType().toNativeType(object));
        }
      } else {
        original.remove(sf.getType().toNativeType(fieldVal));
      }

      toDoc.setField(name, original);
    }
  }

  protected void doRemoveRegex(SolrInputDocument toDoc, SolrInputField sif, Object valuePatterns) {
    final String name = sif.getName();
    final SolrInputField existingField = toDoc.get(name);
    if (existingField != null) {
      final Collection<Object> valueToRemove = new HashSet<>();
      final Collection<Object> original = existingField.getValues();
      final Collection<Pattern> patterns = preparePatterns(valuePatterns);
      for (Object value : original) {
        for(Pattern pattern : patterns) {
          final Matcher m = pattern.matcher(value.toString());
          if (m.matches()) {
            valueToRemove.add(value);
          }
        }
      }
      original.removeAll(valueToRemove);
      toDoc.setField(name, original);
    }
  }

  private Collection<Pattern> preparePatterns(Object fieldVal) {
    final Collection<Pattern> patterns = new LinkedHashSet<>(1);
    if (fieldVal instanceof Collection) {
      Collection<String> patternVals = (Collection<String>) fieldVal;
      for (String patternVal : patternVals) {
        patterns.add(Pattern.compile(patternVal));
      }
    } else {
      patterns.add(Pattern.compile(fieldVal.toString()));
    }
    return patterns;
  }
  
}
