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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;


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
 * &lt;updateProcessor class="solr.GuessSchemaFieldsUpdateProcessorFactory" name="add-schema-fields"&gt;
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
public class GuessSchemaFieldsUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements UpdateRequestProcessorFactory.RunAlways {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TYPE_MAPPING_PARAM = "typeMapping";
  private static final String VALUE_CLASS_PARAM = "valueClass";
  private static final String FIELD_TYPE_PARAM = "fieldType";
  private static final String COPY_FIELD_PARAM = "copyField";
  private static final String DEST_PARAM = "dest";
  private static final String MAX_CHARS_PARAM = "maxChars";

  private List<TypeMapping> typeMappings = Collections.emptyList();
  private TreeMap<String, TypeMapping> typeMappingsIndex = new TreeMap<>();

  private static final Map<String, String> OPTION_MULTIVALUED = Collections.singletonMap("multiValued", "true");

  private Map<String, FieldValueTypeInfo> fieldValueTypeMap = new TreeMap<>();

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next) {
    return new GuessSchemaFieldsUpdateProcessor(next);
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    typeMappings = parseTypeMappings(args);

    for (TypeMapping typeMapping : typeMappings) {
      for (String valueClassName : typeMapping.valueClassNames) {
        if (typeMappingsIndex.containsKey(valueClassName)) {
          if (log.isWarnEnabled()) {
            log.warn("Guess Schema has multiple mappings for same valueClass: {}", valueClassName);
          }
        } else {
          typeMappingsIndex.put(valueClassName, typeMapping);
        }
      }
    }
    super.init(args);
  }

  private static List<TypeMapping> parseTypeMappings(@SuppressWarnings({"rawtypes"})NamedList args) {
    List<TypeMapping> typeMappings = new ArrayList<>();
    @SuppressWarnings({"unchecked"})
    List<Object> typeMappingsParams = args.getAll(TYPE_MAPPING_PARAM);
    for (Object typeMappingObj : typeMappingsParams) {
      if (null == typeMappingObj) {
        throw new SolrException(SERVER_ERROR, "'" + TYPE_MAPPING_PARAM + "' init param cannot be null");
      }
      if ( ! (typeMappingObj instanceof NamedList) ) {
        throw new SolrException(SERVER_ERROR, "'" + TYPE_MAPPING_PARAM + "' init param must be a <lst>");
      }
      @SuppressWarnings({"rawtypes"})
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

      @SuppressWarnings({"unchecked"})
      Collection<String> valueClasses
          = typeMappingNamedList.removeConfigArgs(VALUE_CLASS_PARAM);
      if (valueClasses.isEmpty()) {
        throw new SolrException(SERVER_ERROR, 
            "Each '" + TYPE_MAPPING_PARAM + "' <lst/> must contain at least one '" + VALUE_CLASS_PARAM + "' <str>");
      }

      Collection<CopyFieldDef> copyFieldDefs = new ArrayList<>();
      while (typeMappingNamedList.get(COPY_FIELD_PARAM) != null) {
        Object copyFieldObj = typeMappingNamedList.remove(COPY_FIELD_PARAM);
        if ( ! (copyFieldObj instanceof NamedList)) {
          throw new SolrException(SERVER_ERROR, "'" + COPY_FIELD_PARAM + "' init param must be a <lst>");
        }
        @SuppressWarnings({"rawtypes"})
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
      typeMappings.add(new TypeMapping(fieldType, valueClasses, copyFieldDefs));
      
      if (0 != typeMappingNamedList.size()) {
        throw new SolrException(SERVER_ERROR, 
            "Unexpected '" + TYPE_MAPPING_PARAM + "' init sub-param(s): '" + typeMappingNamedList.toString() + "'");
      }
      args.remove(TYPE_MAPPING_PARAM);
    }
    return typeMappings;
  }

  private static class TypeMapping {
    public String fieldTypeName;
    public Collection<String> valueClassNames;
    public Collection<CopyFieldDef> copyFieldDefs;

    public TypeMapping(String fieldTypeName, Collection<String> valueClassNames, Collection<CopyFieldDef> copyFieldDefs) {
      this.fieldTypeName = fieldTypeName;
      this.valueClassNames = valueClassNames;
      this.copyFieldDefs = copyFieldDefs;
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

  public class FieldValueTypeInfo {

    private String valueTypeName;
    private boolean multiValued;
    private int widenIndex;

    private final String TYPE_STRING = "java.lang.String";

    private final List<String> WIDENING_ORDER = List.of(
            "java.lang.Short", "java.lang.Integer", "java.lang.Long",
            "java.lang.Float", "java.lang.Double", "java.lang.Number", TYPE_STRING);

    private int MAX_WIDEN_INDEX = WIDENING_ORDER.size()-1;

    public FieldValueTypeInfo(String valueTypeName, boolean multiValued) {
      this.valueTypeName = valueTypeName;
      this.multiValued = multiValued;

      //if -1 (no match), we cannot widen
      widenIndex = WIDENING_ORDER.indexOf(valueTypeName);
    }

    public void widen(String newTypeName, boolean newMultiValued) {
      multiValued = multiValued || newMultiValued;

      if (valueTypeName.equals(newTypeName)) {
        return;
      } //nothing to do

      if (widenIndex < 0) {
        // mismatch
        widenIndex = -1;
        valueTypeName = TYPE_STRING; //because it may have been date or boolean before, not just string
        return;
      }

      int newWidenIndex = WIDENING_ORDER.indexOf(newTypeName);
      if (newWidenIndex < 0) {
        //mismatch the other way
        widenIndex = -1;
        valueTypeName = TYPE_STRING;
        return;
      }

      //finally, we are compatible, let's check which type is wider
      if (newWidenIndex > widenIndex) {
        //new value type is wider (later in order) than current
        valueTypeName = newTypeName;
        widenIndex = newWidenIndex;
      }
      // else we stick with the old one
    }

    public boolean canWiden() {
      return widenIndex >=0 && widenIndex < MAX_WIDEN_INDEX;
    }

    /**
     * Widen to the next possible type
     */
    public void widenType(){
      assert canWiden();
      widenIndex++;
      valueTypeName = WIDENING_ORDER.get(widenIndex);
    }

    public String getValueTypeName() {
      return valueTypeName;
    }

    public boolean isMultiValued() {
      return multiValued;
    }

  }

  private class GuessSchemaFieldsUpdateProcessor extends UpdateRequestProcessor {

    public static final String GUESS_SCHEMA_FLAG = "guess-schema";

    public GuessSchemaFieldsUpdateProcessor(UpdateRequestProcessor next) {
      super(next);
    }


    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      if (!cmd.getReq().getParams().getBool(GUESS_SCHEMA_FLAG, false)) {
        super.processAdd(cmd);
        return;
      }

      final SolrInputDocument doc = cmd.getSolrInputDocument();
      for (String fieldName : doc.getFieldNames()) {
        SolrInputField field = doc.getField(fieldName);

        //We do a assert and a null check because even after SOLR-12710 is addressed
        //older SolrJ versions can send null values causing an NPE
        Collection<Object> allValues = field.getValues();
        if (allValues == null) {
          throw new AssertionError("Missing field value.");
        }

        // Check ALL values, just in case they are not internally consistent (from SolrJ?)
        FieldValueTypeInfo typeInfo = fieldValueTypeMap.get(fieldName);
        boolean multiValued = allValues.size() > 1;

        for (Object value : allValues) {
          String typeName = value.getClass().getName();
          if (typeInfo == null) {
            typeInfo = new FieldValueTypeInfo(typeName, multiValued);
            fieldValueTypeMap.put(fieldName, typeInfo);
          } else {
            typeInfo.widen(typeName, multiValued);
          }
        }
      }
      // do not process further. risky?
      // super.processAdd(cmd);
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      if (fieldValueTypeMap.isEmpty()) {
        super.processCommit(cmd);
        return;
      }

      IndexSchema currentSchema = cmd.getReq().getSchema();
      if (!currentSchema.isMutable()) {
        final String message = "Adding new Schema fields requires mutable schema. This one is not (or locked).";
        throw new SolrException(BAD_REQUEST, message);
      }

      //Skip the elaborated field matches using selectors for now. Just do plain there/not there
      int fieldsAdded = 0;
      int copyFieldsAdded = 0;

      synchronized (currentSchema.getSchemaUpdateLock()) {
        IndexSchema newSchema = currentSchema; //start the iterations

        for (String fieldName : fieldValueTypeMap.keySet()) {
          if (currentSchema.hasExplicitField(fieldName) || currentSchema.isDynamicField(fieldName)) {
            if (log.isDebugEnabled()) {
              log.debug("Schema already contains field: {}", fieldName);
            }
            continue;
          }

          FieldValueTypeInfo fieldValueTypeInfo = fieldValueTypeMap.get(fieldName);
          TypeMapping typeMapping = typeMappingsIndex.get(fieldValueTypeInfo.getValueTypeName());
          while (typeMapping == null && fieldValueTypeInfo.canWiden()) {
            fieldValueTypeInfo.widenType();
            typeMapping = typeMappingsIndex.get(fieldValueTypeInfo.getValueTypeName());
          }

          if (typeMapping == null) {
            throw new SolrException(BAD_REQUEST, "Schema Guessing is impossible for the field: " + fieldName);
          }

          String fieldTypeName = typeMapping.fieldTypeName;

          SchemaField newField = currentSchema.newField(
                  fieldName, fieldTypeName,
                  fieldValueTypeInfo.isMultiValued() ? OPTION_MULTIVALUED : Collections.emptyMap());
          if (log.isDebugEnabled()) {
            log.debug("Adding new schema field: {}", newField);
          }
          newSchema = newSchema.addField(newField, false);
          fieldsAdded++;

          if (typeMapping != null && typeMapping.copyFieldDefs != null) {
            for (CopyFieldDef copyFieldDef : typeMapping.copyFieldDefs) {
              newSchema = newSchema.addCopyFields(
                      fieldName,
                      Collections.singleton(copyFieldDef.getDest(fieldName)),
                      copyFieldDef.getMaxChars());
              copyFieldsAdded++;
            }
          }
        }

        //finished with all definitions
        if (fieldsAdded > 0 || copyFieldsAdded > 0) {
          ((ManagedIndexSchema) newSchema).persistManagedSchema(false);
          cmd.getReq().getCore().setLatestSchema(newSchema);
          cmd.getReq().updateSchemaToLatest();
          if (log.isDebugEnabled()) {
            log.debug("Successfully added field(s) and copyField(s) to the schema.");
          }
        } else if (log.isDebugEnabled()) {
          log.debug("No new fields/copyFields were found to add to Schema. All existed before.");
        }
      } //end synchronized
      super.processCommit(cmd);
    }
  }
}
