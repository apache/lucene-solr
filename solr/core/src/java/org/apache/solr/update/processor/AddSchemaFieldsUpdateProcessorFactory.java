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

package org.apache.solr.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessorFactory.SelectorParams;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.core.ConfigSetProperties.IMMUTABLE_CONFIGSET_ARG;


/**
 * <p>
 * This processor will dynamically add fields to the schema if an input document contains
 * one or more fields that don't match any field or dynamic field in the schema.
 * </p>
 * <p>
 * By default, this processor selects all fields that don't match a schema field or
 * dynamic field.  The "fieldName" and "fieldRegex" selectors may be specified to further
 * restrict the selected fields, but the other selectors ("typeName", "typeClass", and
 * "fieldNameMatchesSchemaField") may not be specified.
 * </p>
 * <p>
 * This processor is configured to map from each field's values' class(es) to the schema
 * field type that will be used when adding the new field to the schema.  All new fields
 * are then added to the schema in a single batch.  If schema addition fails for any
 * field, addition is re-attempted only for those that don’t match any schema
 * field.  This process is repeated, either until all new fields are successfully added,
 * or until there are no new fields (presumably because the fields that were new when
 * this processor started its work were subsequently added by a different update
 * request, possibly on a different node).
 * </p>
 * <p>
 * This processor takes as configuration a sequence of zero or more "typeMapping"-s from
 * one or more "valueClass"-s, specified as either an &lt;arr&gt; of &lt;str&gt;, or
 * multiple &lt;str&gt; with the same name, to an existing schema "fieldType".
 * </p>
 * <p>
 * If more than one "valueClass" is specified in a "typeMapping", field values with any
 * of the specified "valueClass"-s will be mapped to the specified target "fieldType".
 * The "typeMapping"-s are attempted in the specified order; if a field value's class
 * is not specified in a "valueClass", the next "typeMapping" is attempted. If no
 * "typeMapping" succeeds, then the specified "defaultFieldType" is used. 
 * </p>
 * <p>
 * Example configuration:
 * </p>
 * 
 * <pre class="prettyprint">
 * &lt;processor class="solr.AddSchemaFieldsUpdateProcessorFactory"&gt;
 *   &lt;str name="defaultFieldType"&gt;text_general&lt;/str&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;Boolean&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;boolean&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;Integer&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;tint&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;Float&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;tfloat&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;Date&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;tdate&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;Long&lt;/str&gt;
 *     &lt;str name="valueClass"&gt;Integer&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;tlong&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;arr name="valueClass"&gt;
 *       &lt;str&gt;Double&lt;/str&gt;
 *       &lt;str&gt;Float&lt;/str&gt;
 *     &lt;/arr&gt;
 *     &lt;str name="fieldType"&gt;tdouble&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/processor&gt;</pre>
 */
public class AddSchemaFieldsUpdateProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TYPE_MAPPING_PARAM = "typeMapping";
  private static final String VALUE_CLASS_PARAM = "valueClass";
  private static final String FIELD_TYPE_PARAM = "fieldType";
  private static final String DEFAULT_FIELD_TYPE_PARAM = "defaultFieldType";
  
  private List<TypeMapping> typeMappings = Collections.emptyList();
  private SelectorParams inclusions = new SelectorParams();
  private Collection<SelectorParams> exclusions = new ArrayList<>();
  private SolrResourceLoader solrResourceLoader = null;
  private String defaultFieldType;

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next) {
    return new AddSchemaFieldsUpdateProcessor(next);
  }

  @Override
  public void init(NamedList args) {
    inclusions = FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
    validateSelectorParams(inclusions);
    inclusions.fieldNameMatchesSchemaField = false;  // Explicitly (non-configurably) require unknown field names
    exclusions = FieldMutatingUpdateProcessorFactory.parseSelectorExclusionParams(args);
    for (SelectorParams exclusion : exclusions) {
      validateSelectorParams(exclusion);
    }
    Object defaultFieldTypeParam = args.remove(DEFAULT_FIELD_TYPE_PARAM);
    if (null == defaultFieldTypeParam) {
      throw new SolrException(SERVER_ERROR, "Missing required init param '" + DEFAULT_FIELD_TYPE_PARAM + "'");
    } else {
      if ( ! (defaultFieldTypeParam instanceof CharSequence)) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEFAULT_FIELD_TYPE_PARAM + "' must be a <str>");
      }
    }
    defaultFieldType = defaultFieldTypeParam.toString();

    typeMappings = parseTypeMappings(args);

    super.init(args);
  }

  @Override
  public void inform(SolrCore core) {
    solrResourceLoader = core.getResourceLoader();

    for (TypeMapping typeMapping : typeMappings) {
      typeMapping.populateValueClasses(core);
    }
  }

  private static List<TypeMapping> parseTypeMappings(NamedList args) {
    List<TypeMapping> typeMappings = new ArrayList<>();
    List<Object> typeMappingsParams = args.getAll(TYPE_MAPPING_PARAM);
    for (Object typeMappingObj : typeMappingsParams) {
      if (null == typeMappingObj) {
        throw new SolrException(SERVER_ERROR, "'" + TYPE_MAPPING_PARAM + "' init param cannot be null");
      }
      if ( ! (typeMappingObj instanceof NamedList) ) {
        throw new SolrException(SERVER_ERROR, "'" + TYPE_MAPPING_PARAM + "' init param must be a <lst>");
      }
      NamedList typeMappingNamedList = (NamedList)typeMappingObj;

      Object fieldTypeObj = typeMappingNamedList.remove(FIELD_TYPE_PARAM);
      if (null == fieldTypeObj) {
        throw new SolrException(SERVER_ERROR,
            "Each '" + TYPE_MAPPING_PARAM + "' <lst/> must contain a '" + FIELD_TYPE_PARAM + "' <str>");
      }
      if ( ! (fieldTypeObj instanceof CharSequence)) {
        throw new SolrException(SERVER_ERROR, "'" + FIELD_TYPE_PARAM + "' init param must be a <str>");
      }
      if (null != typeMappingNamedList.get(FIELD_TYPE_PARAM)) {
        throw new SolrException(SERVER_ERROR,
            "Each '" + TYPE_MAPPING_PARAM + "' <lst/> may contain only one '" + FIELD_TYPE_PARAM + "' <str>");
      }
      String fieldType = fieldTypeObj.toString();

      Collection<String> valueClasses
          = typeMappingNamedList.removeConfigArgs(VALUE_CLASS_PARAM);
      if (valueClasses.isEmpty()) {
        throw new SolrException(SERVER_ERROR, 
            "Each '" + TYPE_MAPPING_PARAM + "' <lst/> must contain at least one '" + VALUE_CLASS_PARAM + "' <str>");
      }
      typeMappings.add(new TypeMapping(fieldType, valueClasses));

      if (0 != typeMappingNamedList.size()) {
        throw new SolrException(SERVER_ERROR, 
            "Unexpected '" + TYPE_MAPPING_PARAM + "' init sub-param(s): '" + typeMappingNamedList.toString() + "'");
      }
      args.remove(TYPE_MAPPING_PARAM);
    }
    return typeMappings;
  }

  private void validateSelectorParams(SelectorParams params) {
    if ( ! params.typeName.isEmpty()) {
      throw new SolrException(SERVER_ERROR, "'typeName' init param is not allowed in this processor");
    }
    if ( ! params.typeClass.isEmpty()) {
      throw new SolrException(SERVER_ERROR, "'typeClass' init param is not allowed in this processor");
    }
    if (null != params.fieldNameMatchesSchemaField) {
      throw new SolrException(SERVER_ERROR, "'fieldNameMatchesSchemaField' init param is not allowed in this processor");
    }
  }

  private static class TypeMapping {
    public String fieldTypeName;
    public Collection<String> valueClassNames;
    public Set<Class<?>> valueClasses;

    public TypeMapping(String fieldTypeName, Collection<String> valueClassNames) {
      this.fieldTypeName = fieldTypeName;
      this.valueClassNames = valueClassNames;
      // this.valueClasses population is delayed until the schema is available
    }

    public void populateValueClasses(SolrCore core) {
      IndexSchema schema = core.getLatestSchema();
      ClassLoader loader = core.getResourceLoader().getClassLoader();
      if (null == schema.getFieldTypeByName(fieldTypeName)) {
        throw new SolrException(SERVER_ERROR, "fieldType '" + fieldTypeName + "' not found in the schema");
      }
      valueClasses = new HashSet<>();
      for (String valueClassName : valueClassNames) {
        try {
          valueClasses.add(loader.loadClass(valueClassName));
        } catch (ClassNotFoundException e) {
          throw new SolrException(SERVER_ERROR,
              "valueClass '" + valueClassName + "' not found for fieldType '" + fieldTypeName + "'");
        }
      }
    }
  }

  private class AddSchemaFieldsUpdateProcessor extends UpdateRequestProcessor {
    public AddSchemaFieldsUpdateProcessor(UpdateRequestProcessor next) {
      super(next);
    }
    
    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      if ( ! cmd.getReq().getSchema().isMutable()) {
        final String message = "This IndexSchema is not mutable.";
        throw new SolrException(BAD_REQUEST, message);
      }
      final SolrInputDocument doc = cmd.getSolrInputDocument();
      final SolrCore core = cmd.getReq().getCore();
      // use the cmd's schema rather than the latest, because the schema
      // can be updated during processing.  Using the cmd's schema guarantees
      // this will be detected and the cmd's schema updated.
      IndexSchema oldSchema = cmd.getReq().getSchema();
      for (;;) {
        List<SchemaField> newFields = new ArrayList<>();
        // build a selector each time through the loop b/c the schema we are
        // processing may have changed
        FieldNameSelector selector = buildSelector(oldSchema);
        Map<String,List<SolrInputField>> unknownFields = new HashMap<>();
        getUnknownFields(selector, doc, unknownFields);
        for (final Map.Entry<String,List<SolrInputField>> entry : unknownFields.entrySet()) {
          String fieldName = entry.getKey();
          String fieldTypeName = mapValueClassesToFieldType(entry.getValue());
          newFields.add(oldSchema.newField(fieldName, fieldTypeName, Collections.<String,Object>emptyMap()));
        }
        if (newFields.isEmpty()) {
          // nothing to do - no fields will be added - exit from the retry loop
          log.debug("No fields to add to the schema.");
          break;
        } else if ( isImmutableConfigSet(core) ) {
          final String message = "This ConfigSet is immutable.";
          throw new SolrException(BAD_REQUEST, message);
        }
        if (log.isDebugEnabled()) {
          StringBuilder builder = new StringBuilder();
          builder.append("Fields to be added to the schema: [");
          boolean isFirst = true;
          for (SchemaField field : newFields) {
            builder.append(isFirst ? "" : ",");
            isFirst = false;
            builder.append(field.getName());
            builder.append("{type=").append(field.getType().getTypeName()).append("}");
          }
          builder.append("]");
          log.debug(builder.toString());
        }
        // Need to hold the lock during the entire attempt to ensure that
        // the schema on the request is the latest
        synchronized (oldSchema.getSchemaUpdateLock()) {
          try {
            IndexSchema newSchema = oldSchema.addFields(newFields);
            if (null != newSchema) {
              core.setLatestSchema(newSchema);
              cmd.getReq().updateSchemaToLatest();
              log.debug("Successfully added field(s) to the schema.");
              break; // success - exit from the retry loop
            } else {
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to add fields.");
            }
          } catch (ManagedIndexSchema.FieldExistsException e) {
            log.error("At least one field to be added already exists in the schema - retrying.");
            oldSchema = core.getLatestSchema();
            cmd.getReq().updateSchemaToLatest();
          } catch (ManagedIndexSchema.SchemaChangedInZkException e) {
            log.debug("Schema changed while processing request - retrying.");
            oldSchema = core.getLatestSchema();
            cmd.getReq().updateSchemaToLatest();
          }
        }
      }
      super.processAdd(cmd);
    }

    /**
     * Recursively find unknown fields in the given doc and its child documents, if any.
     */
    private void getUnknownFields
    (FieldNameSelector selector, SolrInputDocument doc, Map<String,List<SolrInputField>> unknownFields) {
      for (final String fieldName : doc.getFieldNames()) {
        if (selector.shouldMutate(fieldName)) { // returns false if the field already exists in the current schema
          List<SolrInputField> solrInputFields = unknownFields.get(fieldName);
          if (null == solrInputFields) {
            solrInputFields = new ArrayList<>();
            unknownFields.put(fieldName, solrInputFields);
          }
          solrInputFields.add(doc.getField(fieldName));
        }
      }
      List<SolrInputDocument> childDocs = doc.getChildDocuments();
      if (null != childDocs) {
        for (SolrInputDocument childDoc : childDocs) {
          getUnknownFields(selector, childDoc, unknownFields);
        }
      }
    }

    /**
     * Maps all given field values' classes to a field type using the configured type mapping rules.
     * 
     * @param fields one or more (same-named) field values from one or more documents
     */
    private String mapValueClassesToFieldType(List<SolrInputField> fields) {
      NEXT_TYPE_MAPPING: for (TypeMapping typeMapping : typeMappings) {
        for (SolrInputField field : fields) {
          NEXT_FIELD_VALUE: for (Object fieldValue : field.getValues()) {
            for (Class<?> valueClass : typeMapping.valueClasses) {
              if (valueClass.isInstance(fieldValue)) {
                continue NEXT_FIELD_VALUE;
              }
            }
            // This fieldValue is not an instance of any of the mapped valueClass-s,
            // so mapping fails - go try the next type mapping.
            continue NEXT_TYPE_MAPPING;
          }
        }
        // Success! Each of this field's values is an instance of a mapped valueClass
        return typeMapping.fieldTypeName;
      }
      // At least one of this field's values is not an instance of any of the mapped valueClass-s
      return defaultFieldType;
    }

    private FieldNameSelector getDefaultSelector(final IndexSchema schema) {
      return new FieldNameSelector() {
        @Override
        public boolean shouldMutate(final String fieldName) {
          return null == schema.getFieldTypeNoEx(fieldName);
        }
      };
    }

    private FieldNameSelector buildSelector(IndexSchema schema) {
      FieldNameSelector selector = FieldMutatingUpdateProcessor.createFieldNameSelector
        (solrResourceLoader, schema, inclusions, getDefaultSelector(schema));

      for (SelectorParams exc : exclusions) {
        selector = FieldMutatingUpdateProcessor.wrap(selector, FieldMutatingUpdateProcessor.createFieldNameSelector
          (solrResourceLoader, schema, exc, FieldMutatingUpdateProcessor.SELECT_NO_FIELDS));
      }
      return selector;
    }

    private boolean isImmutableConfigSet(SolrCore core) {
      NamedList args = core.getConfigSetProperties();
      Object immutable = args != null ? args.get(IMMUTABLE_CONFIGSET_ARG) : null;
      return immutable != null ? Boolean.parseBoolean(immutable.toString()) : false;
    }
  }
}
