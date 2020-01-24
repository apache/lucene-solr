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
import java.util.stream.Collectors;

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
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessorFactory.SelectorParams;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * one or more "valueClass"-s, specified as either an <code>&lt;arr&gt;</code> of 
 * <code>&lt;str&gt;</code>, or multiple <code>&lt;str&gt;</code> with the same name,
 * to an existing schema "fieldType".
 * </p>
 * <p>
 * If more than one "valueClass" is specified in a "typeMapping", field values with any
 * of the specified "valueClass"-s will be mapped to the specified target "fieldType".
 * The "typeMapping"-s are attempted in the specified order; if a field value's class
 * is not specified in a "valueClass", the next "typeMapping" is attempted. If no
 * "typeMapping" succeeds, then either the "typeMapping" configured with 
 * <code>&lt;bool name="default"&gt;true&lt;/bool&gt;</code> is used, or if none is so
 * configured, the <code>lt;str name="defaultFieldType"&gt;...&lt;/str&gt;</code> is
 * used.
 * </p>
 * <p>
 * Zero or more "copyField" directives may be included with each "typeMapping", using a
 * <code>&lt;lst&gt;</code>. The copy field source is automatically set to the new field
 * name; "dest" must specify the destination field or dynamic field in a
 * <code>&lt;str&gt;</code>; and "maxChars" may optionally be specified in an
 * <code>&lt;int&gt;</code>.
 * </p>
 * <p>
 * Example configuration:
 * </p>
 * 
 * <pre class="prettyprint">
 * &lt;updateProcessor class="solr.AddSchemaFieldsUpdateProcessorFactory" name="add-schema-fields"&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;java.lang.String&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;text_general&lt;/str&gt;
 *     &lt;lst name="copyField"&gt;
 *       &lt;str name="dest"&gt;*_str&lt;/str&gt;
 *       &lt;int name="maxChars"&gt;256&lt;/int&gt;
 *     &lt;/lst&gt;
 *     &lt;!-- Use as default mapping instead of defaultFieldType --&gt;
 *     &lt;bool name="default"&gt;true&lt;/bool&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;java.lang.Boolean&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;booleans&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;java.util.Date&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;pdates&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;java.lang.Long&lt;/str&gt;
 *     &lt;str name="valueClass"&gt;java.lang.Integer&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;plongs&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="typeMapping"&gt;
 *     &lt;str name="valueClass"&gt;java.lang.Number&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;pdoubles&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/updateProcessor&gt;</pre>
 * @since 4.4.0
 */
public class AddSchemaFieldsUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TYPE_MAPPING_PARAM = "typeMapping";
  private static final String VALUE_CLASS_PARAM = "valueClass";
  private static final String FIELD_TYPE_PARAM = "fieldType";
  private static final String DEFAULT_FIELD_TYPE_PARAM = "defaultFieldType";
  private static final String COPY_FIELD_PARAM = "copyField";
  private static final String DEST_PARAM = "dest";
  private static final String MAX_CHARS_PARAM = "maxChars";
  private static final String IS_DEFAULT_PARAM = "default";

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
    if (null != defaultFieldTypeParam) {
      if ( ! (defaultFieldTypeParam instanceof CharSequence)) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEFAULT_FIELD_TYPE_PARAM + "' must be a <str>");
      }
      defaultFieldType = defaultFieldTypeParam.toString();
    }

    typeMappings = parseTypeMappings(args);
    if (null == defaultFieldType && typeMappings.stream().noneMatch(TypeMapping::isDefault)) {
      throw new SolrException(SERVER_ERROR, "Must specify either '" + DEFAULT_FIELD_TYPE_PARAM + 
          "' or declare one typeMapping as default.");
    }

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

      // isDefault (optional)
      Boolean isDefault = false;
      Object isDefaultObj = typeMappingNamedList.remove(IS_DEFAULT_PARAM);
      if (null != isDefaultObj) {
        if ( ! (isDefaultObj instanceof Boolean)) {
          throw new SolrException(SERVER_ERROR, "'" + IS_DEFAULT_PARAM + "' init param must be a <bool>");
        }
        if (null != typeMappingNamedList.get(IS_DEFAULT_PARAM)) {
          throw new SolrException(SERVER_ERROR,
              "Each '" + COPY_FIELD_PARAM + "' <lst/> may contain only one '" + IS_DEFAULT_PARAM + "' <bool>");
        }
        isDefault = Boolean.parseBoolean(isDefaultObj.toString());
      }
      
      Collection<CopyFieldDef> copyFieldDefs = new ArrayList<>(); 
      while (typeMappingNamedList.get(COPY_FIELD_PARAM) != null) {
        Object copyFieldObj = typeMappingNamedList.remove(COPY_FIELD_PARAM);
        if ( ! (copyFieldObj instanceof NamedList)) {
          throw new SolrException(SERVER_ERROR, "'" + COPY_FIELD_PARAM + "' init param must be a <lst>");
        }
        NamedList copyFieldNamedList = (NamedList)copyFieldObj;
        // dest
        Object destObj = copyFieldNamedList.remove(DEST_PARAM);
        if (null == destObj) {
          throw new SolrException(SERVER_ERROR,
              "Each '" + COPY_FIELD_PARAM + "' <lst/> must contain a '" + DEST_PARAM + "' <str>");
        }
        if ( ! (destObj instanceof CharSequence)) {
          throw new SolrException(SERVER_ERROR, "'" + COPY_FIELD_PARAM + "' init param must be a <str>");
        }
        if (null != copyFieldNamedList.get(COPY_FIELD_PARAM)) {
          throw new SolrException(SERVER_ERROR,
              "Each '" + COPY_FIELD_PARAM + "' <lst/> may contain only one '" + COPY_FIELD_PARAM + "' <str>");
        }
        String dest = destObj.toString();
        // maxChars (optional)
        Integer maxChars = 0;
        Object maxCharsObj = copyFieldNamedList.remove(MAX_CHARS_PARAM);
        if (null != maxCharsObj) {
          if ( ! (maxCharsObj instanceof Integer)) {
            throw new SolrException(SERVER_ERROR, "'" + MAX_CHARS_PARAM + "' init param must be a <int>");
          }
          if (null != copyFieldNamedList.get(MAX_CHARS_PARAM)) {
            throw new SolrException(SERVER_ERROR,
                "Each '" + COPY_FIELD_PARAM + "' <lst/> may contain only one '" + MAX_CHARS_PARAM + "' <str>");
          }
          maxChars = Integer.parseInt(maxCharsObj.toString());
        }
        copyFieldDefs.add(new CopyFieldDef(dest, maxChars));
      }
      typeMappings.add(new TypeMapping(fieldType, valueClasses, isDefault, copyFieldDefs));
      
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
    public Collection<CopyFieldDef> copyFieldDefs;
    public Set<Class<?>> valueClasses;
    public Boolean isDefault;

    public TypeMapping(String fieldTypeName, Collection<String> valueClassNames, boolean isDefault,
                       Collection<CopyFieldDef> copyFieldDefs) {
      this.fieldTypeName = fieldTypeName;
      this.valueClassNames = valueClassNames;
      this.isDefault = isDefault;
      this.copyFieldDefs = copyFieldDefs;
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

    public boolean isDefault() {
      return isDefault;
    }
  }

  private static class CopyFieldDef {
    private final String destGlob;
    private final Integer maxChars;

    public CopyFieldDef(String destGlob, Integer maxChars) {
      this.destGlob = destGlob;
      this.maxChars = maxChars;
      if (destGlob.contains("*") && (!destGlob.startsWith("*") && !destGlob.endsWith("*"))) {
        throw new SolrException(SERVER_ERROR, "dest '" + destGlob + 
            "' is invalid. Must either be a plain field name or start or end with '*'");
      }
    }
    
    public Integer getMaxChars() {
      return maxChars;
    }
    
    public String getDest(String srcFieldName) {
      if (!destGlob.contains("*")) {
        return destGlob;
      } else if (destGlob.startsWith("*")) {
        return srcFieldName + destGlob.substring(1);
      } else {
        return destGlob.substring(0,destGlob.length()-1) + srcFieldName;
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
        // Group copyField defs per field and then per maxChar, to adapt to IndexSchema API 
        Map<String,Map<Integer,List<CopyFieldDef>>> newCopyFields = new HashMap<>();
        // build a selector each time through the loop b/c the schema we are
        // processing may have changed
        FieldNameSelector selector = buildSelector(oldSchema);
        Map<String,List<SolrInputField>> unknownFields = new HashMap<>();
        getUnknownFields(selector, doc, unknownFields);
        for (final Map.Entry<String,List<SolrInputField>> entry : unknownFields.entrySet()) {
          String fieldName = entry.getKey();
          String fieldTypeName = defaultFieldType;
          TypeMapping typeMapping = mapValueClassesToFieldType(entry.getValue());
          if (typeMapping != null) {
            fieldTypeName = typeMapping.fieldTypeName;
            if (!typeMapping.copyFieldDefs.isEmpty()) {
              newCopyFields.put(fieldName,
                  typeMapping.copyFieldDefs.stream().collect(Collectors.groupingBy(CopyFieldDef::getMaxChars)));
            }
          } 
          newFields.add(oldSchema.newField(fieldName, fieldTypeName, Collections.<String,Object>emptyMap()));
        }
        if (newFields.isEmpty() && newCopyFields.isEmpty()) {
          // nothing to do - no fields will be added - exit from the retry loop
          log.debug("No fields or copyFields to add to the schema.");
          break;
        } else if ( isImmutableConfigSet(core) ) {
          final String message = "This ConfigSet is immutable.";
          throw new SolrException(BAD_REQUEST, message);
        }
        if (log.isDebugEnabled()) {
          StringBuilder builder = new StringBuilder();
          builder.append("\nFields to be added to the schema: [");
          boolean isFirst = true;
          for (SchemaField field : newFields) {
            builder.append(isFirst ? "" : ",");
            isFirst = false;
            builder.append(field.getName());
            builder.append("{type=").append(field.getType().getTypeName()).append("}");
          }
          builder.append("]");
          builder.append("\nCopyFields to be added to the schema: [");
          isFirst = true;
          for (Map.Entry<String, Map<Integer, List<CopyFieldDef>>> entry : newCopyFields.entrySet()) {
            String fieldName = entry.getKey();
            builder.append(isFirst ? "" : ",");
            isFirst = false;
            builder.append("source=").append(fieldName).append("{");
            for (List<CopyFieldDef> copyFieldDefList : entry.getValue().values()) {
              for (CopyFieldDef copyFieldDef : copyFieldDefList) {
                builder.append("{dest=").append(copyFieldDef.getDest(fieldName));
                builder.append(", maxChars=").append(copyFieldDef.getMaxChars()).append("}");
              }
            }
            builder.append("}");
          }
          builder.append("]");
          log.debug(builder.toString());
        }
        // Need to hold the lock during the entire attempt to ensure that
        // the schema on the request is the latest
        synchronized (oldSchema.getSchemaUpdateLock()) {
          try {
            IndexSchema newSchema = oldSchema.addFields(newFields, Collections.emptyMap(), false);
            // Add copyFields
            for (Map.Entry<String, Map<Integer, List<CopyFieldDef>>> entry : newCopyFields.entrySet()) {
              String srcField = entry.getKey();
              for (Integer maxChars : entry.getValue().keySet()) {
                newSchema = newSchema.addCopyFields(srcField,
                    entry.getValue().get(maxChars).stream().map(f -> f.getDest(srcField)).collect(Collectors.toList()),
                  maxChars);
              }
            }
            if (null != newSchema) {
              ((ManagedIndexSchema)newSchema).persistManagedSchema(false);
              core.setLatestSchema(newSchema);
              cmd.getReq().updateSchemaToLatest();
              log.debug("Successfully added field(s) and copyField(s) to the schema.");
              break; // success - exit from the retry loop
            } else {
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to add fields and/or copyFields.");
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
        //We do a assert and a null check because even after SOLR-12710 is addressed
        //older SolrJ versions can send null values causing an NPE
        assert fieldName != null;
        if (fieldName != null) {
          if (selector.shouldMutate(fieldName)) { // returns false if the field already exists in the current schema
            List<SolrInputField> solrInputFields = unknownFields.get(fieldName);
            if (null == solrInputFields) {
              solrInputFields = new ArrayList<>();
              unknownFields.put(fieldName, solrInputFields);
            }
            solrInputFields.add(doc.getField(fieldName));
          }
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
     * Maps all given field values' classes to a typeMapping object
     * 
     * @param fields one or more (same-named) field values from one or more documents
     */
    private TypeMapping mapValueClassesToFieldType(List<SolrInputField> fields) {
      NEXT_TYPE_MAPPING: for (TypeMapping typeMapping : typeMappings) {
        for (SolrInputField field : fields) {
          //We do a assert and a null check because even after SOLR-12710 is addressed
          //older SolrJ versions can send null values causing an NPE
          assert field.getValues() != null;
          if (field.getValues() != null) {
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
        }
        // Success! Each of this field's values is an instance of a mapped valueClass
        return typeMapping;
      }
      // At least one of this field's values is not an instance of any of the mapped valueClass-s
      // Return the typeMapping marked as default, if we have one, else return null to use fallback type 
      List<TypeMapping> defaultMappings = typeMappings.stream().filter(TypeMapping::isDefault).collect(Collectors.toList());
      if (defaultMappings.size() > 1) {
        throw new SolrException(SERVER_ERROR, "Only one typeMapping can be default");
      } else if (defaultMappings.size() == 1) {
        return defaultMappings.get(0);
      } else {
        return null;
      }
    }

    private FieldNameSelector buildSelector(IndexSchema schema) {
      FieldNameSelector selector = FieldMutatingUpdateProcessor.createFieldNameSelector
        (solrResourceLoader, schema, inclusions, fieldName -> null == schema.getFieldTypeNoEx(fieldName));

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
