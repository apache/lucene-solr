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
package org.apache.solr.client.solrj.request.schema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;

/**
 * <p>This class offers access to the operations exposed by the Solr Schema API.</p>
 * <p>Most of the operations of this class offer a very abstract interface avoiding
 * in this manner eventual changes due to Solr Schema API updates. On the other
 * hand, the creation of request parameters for creating new fields or field types
 * can be tedious because it is not strongly typed (the user has to build on his own
 * a {@link NamedList} argument containing the field/field type properties).</p>
 * <p>The class does not currently offer explicit support for the Schema API operations exposed
 * through Managed Resources, but such operations can be built with little effort manually
 * based on this class within the client applications.</p>
 * <p>This class is experimental and it is subject to change.</p>
 *
 * @see <a href="https://lucene.apache.org/solr/guide/schema-api.html">Solr Schema API</a>
 * @see <a href="https://lucene.apache.org/solr/guide/managed-resources.html">Solr managed resources</a>
 * @since solr 5.3
 */
public class SchemaRequest extends AbstractSchemaRequest<SchemaResponse> {

  /**
   * Default constructor.
   * It can be used to retrieve the entire schema.
   *
   * @see #process(SolrClient)
   */
  public SchemaRequest() {
    this(null);
  }

  public SchemaRequest(SolrParams q) {
    super(METHOD.GET, "/schema", q);
  }

  private static NamedList<Object> createAddFieldTypeNamedList(FieldTypeDefinition fieldTypeDefinition) {
    final NamedList<Object> addFieldTypeNamedList = new NamedList<>();
    addFieldTypeNamedList.addAll(fieldTypeDefinition.getAttributes());
    AnalyzerDefinition analyzerDefinition = fieldTypeDefinition.getAnalyzer();
    if (analyzerDefinition != null) {
      NamedList<Object> analyzerNamedList = createAnalyzerNamedList(analyzerDefinition);
      addFieldTypeNamedList.add("analyzer", analyzerNamedList);
    }
    AnalyzerDefinition indexAnalyzerDefinition = fieldTypeDefinition.getIndexAnalyzer();
    if (indexAnalyzerDefinition != null) {
      NamedList<Object> indexAnalyzerNamedList = createAnalyzerNamedList(indexAnalyzerDefinition);
      addFieldTypeNamedList.add("indexAnalyzer", indexAnalyzerNamedList);
    }
    AnalyzerDefinition queryAnalyzerDefinition = fieldTypeDefinition.getQueryAnalyzer();
    if (queryAnalyzerDefinition != null) {
      NamedList<Object> queryAnalyzerNamedList = createAnalyzerNamedList(queryAnalyzerDefinition);
      addFieldTypeNamedList.add("queryAnalyzer", queryAnalyzerNamedList);
    }
    AnalyzerDefinition multiTermAnalyzerDefinition = fieldTypeDefinition.getMultiTermAnalyzer();
    if (multiTermAnalyzerDefinition != null) {
      NamedList<Object> multiTermAnalyzerNamedList = createAnalyzerNamedList(multiTermAnalyzerDefinition);
      addFieldTypeNamedList.add("multiTermAnalyzer", multiTermAnalyzerNamedList);
    }
    Map<String, Object> similarityAttributes = fieldTypeDefinition.getSimilarity();
    if (similarityAttributes != null && !similarityAttributes.isEmpty()) {
      addFieldTypeNamedList.add("similarity", new NamedList<>(similarityAttributes));
    }

    return addFieldTypeNamedList;
  }

  private static NamedList<Object> createAnalyzerNamedList(AnalyzerDefinition analyzerDefinition) {
    NamedList<Object> analyzerNamedList = new NamedList<>();
    Map<String, Object> analyzerAttributes = analyzerDefinition.getAttributes();
    if (analyzerAttributes != null)
      analyzerNamedList.addAll(analyzerAttributes);
    List<Map<String, Object>> charFiltersAttributes = analyzerDefinition.getCharFilters();
    if (charFiltersAttributes != null) {
      List<NamedList<Object>> charFiltersList = new LinkedList<>();
      for (Map<String, Object> charFilterAttributes : charFiltersAttributes)
        charFiltersList.add(new NamedList<>(charFilterAttributes));
      analyzerNamedList.add("charFilters", charFiltersList);
    }
    Map<String, Object> tokenizerAttributes = analyzerDefinition.getTokenizer();
    if (tokenizerAttributes != null) {
      analyzerNamedList.add("tokenizer", new NamedList<>(tokenizerAttributes));
    }
    List<Map<String, Object>> filtersAttributes = analyzerDefinition.getFilters();
    if (filtersAttributes != null) {
      List<NamedList<Object>> filtersList = new LinkedList<>();
      for (Map<String, Object> filterAttributes : filtersAttributes)
        filtersList.add(new NamedList<>(filterAttributes));
      analyzerNamedList.add("filters", filtersList);
    }
    return analyzerNamedList;
  }

  private static NamedList<Object> createAddFieldNamedList(Map<String, Object> fieldAttributes) {
    final NamedList<Object> addFieldProps = new NamedList<>();
    if (fieldAttributes != null) addFieldProps.addAll(fieldAttributes);
    return addFieldProps;
  }

  @Override
  protected SchemaResponse createResponse(SolrClient client) {
    return new SchemaResponse();
  }

  /**
   * Schema API request class that can be used to retrieve the name of the schema.
   */
  public static class SchemaName extends AbstractSchemaRequest<SchemaResponse.SchemaNameResponse> {
    public SchemaName() {
      this(null);
    }

    public SchemaName(SolrParams q) {
      super(METHOD.GET, "/schema/name", q);
    }

    @Override
    protected SchemaResponse.SchemaNameResponse createResponse(SolrClient client) {
      return new SchemaResponse.SchemaNameResponse();
    }
  }

  /**
   * Schema API request that can be used to retrieve the version
   * of the schema for the specified collection.
   */
  public static class SchemaVersion extends AbstractSchemaRequest<SchemaResponse.SchemaVersionResponse> {
    public SchemaVersion() {
      this(null);
    }

    public SchemaVersion(SolrParams q) {
      super(METHOD.GET, "/schema/version", q);
    }

    @Override
    protected SchemaResponse.SchemaVersionResponse createResponse(SolrClient client) {
      return new SchemaResponse.SchemaVersionResponse();
    }
  }

  /**
   * Schema API request class that lists the field definitions contained in the schema.
   */
  public static class Fields extends AbstractSchemaRequest<SchemaResponse.FieldsResponse> {
    public Fields() {
      this(null);
    }

    public Fields(SolrParams q) {
      super(METHOD.GET, "/schema/fields", q);
    }

    @Override
    protected SchemaResponse.FieldsResponse createResponse(SolrClient client) {
      return new SchemaResponse.FieldsResponse();
    }
  }

  /**
   * Schema API request that lists the field definition for the specified field
   * contained in the schema.
   */
  public static class Field extends AbstractSchemaRequest<SchemaResponse.FieldResponse> {
    /**
     * Creates a new instance of the request.
     *
     * @param fieldName the name of the field for which the definition is to be retrieved
     */
    public Field(String fieldName) {
      this(fieldName, null);
    }

    public Field(String fieldName, SolrParams q) {
      super(METHOD.GET, "/schema/fields/" + fieldName, q);
    }

    @Override
    protected SchemaResponse.FieldResponse createResponse(SolrClient client) {
      return new SchemaResponse.FieldResponse();
    }
  }

  /**
   * Schema API request that lists the dynamic field definitions contained in the schema.
   */
  public static class DynamicFields extends AbstractSchemaRequest<SchemaResponse.DynamicFieldsResponse> {

    public DynamicFields() {
      this(null);
    }

    public DynamicFields(SolrParams q) {
      super(METHOD.GET, "/schema/dynamicfields", q);
    }

    @Override
    protected SchemaResponse.DynamicFieldsResponse createResponse(SolrClient client) {
      return new SchemaResponse.DynamicFieldsResponse();
    }
  }

  /**
   * Schema API request that lists the dynamic field definition for the specified field
   * contained in the schema.
   */
  public static class DynamicField extends AbstractSchemaRequest<SchemaResponse.DynamicFieldResponse> {
    /**
     * Creates a new instance of the class.
     *
     * @param dynamicFieldName the name of the dynamic field for which the definition is to be retrieved
     */
    public DynamicField(String dynamicFieldName) {
      this(dynamicFieldName, null);
    }

    public DynamicField(String dynamicFieldName, SolrParams q) {
      super(METHOD.GET, "/schema/dynamicfields/" + dynamicFieldName, q);
    }

    @Override
    protected SchemaResponse.DynamicFieldResponse createResponse(SolrClient client) {
      return new SchemaResponse.DynamicFieldResponse();
    }
  }

  /**
   * Schema API request that lists the types definitions contained
   * in the schema.
   */
  public static class FieldTypes extends AbstractSchemaRequest<SchemaResponse.FieldTypesResponse> {
    public FieldTypes() {
      this(null);
    }

    public FieldTypes(SolrParams q) {
      super(METHOD.GET, "/schema/fieldtypes", q);
    }

    @Override
    protected SchemaResponse.FieldTypesResponse createResponse(SolrClient client) {
      return new SchemaResponse.FieldTypesResponse();
    }
  }

  /**
   * Schema API request that retrieves the type definitions for the specified field
   * type contained in the schema.
   */
  public static class FieldType extends AbstractSchemaRequest<SchemaResponse.FieldTypeResponse> {
    /**
     * Creates a new instance of the request.
     *
     * @param fieldTypeName the name of the field type for which the definition is to be retrieved
     */
    public FieldType(String fieldTypeName) {
      this(fieldTypeName, null);
    }

    public FieldType(String fieldTypeName, SolrParams q) {
      super(METHOD.GET, "/schema/fieldtypes/" + fieldTypeName, q);
    }

    @Override
    protected SchemaResponse.FieldTypeResponse createResponse(SolrClient client) {
      return new SchemaResponse.FieldTypeResponse();
    }
  }

  /**
   * Schema API request that retrieves the source and destination of
   * each copy field in the schema.
   */
  public static class CopyFields extends AbstractSchemaRequest<SchemaResponse.CopyFieldsResponse> {
    public CopyFields() {
      this(null);
    }

    public CopyFields(SolrParams q) {
      super(METHOD.GET, "/schema/copyfields", q);
    }

    @Override
    protected SchemaResponse.CopyFieldsResponse createResponse(SolrClient client) {
      return new SchemaResponse.CopyFieldsResponse();
    }
  }

  /**
   * Schema API request that retrieves the field name that is defined as
   * the uniqueKey for the index of the specified collection.
   */
  public static class UniqueKey extends AbstractSchemaRequest<SchemaResponse.UniqueKeyResponse> {
    public UniqueKey() {
      this(null);
    }

    public UniqueKey(SolrParams q) {
      super(METHOD.GET, "/schema/uniquekey", q);
    }

    @Override
    protected SchemaResponse.UniqueKeyResponse createResponse(SolrClient client) {
      return new SchemaResponse.UniqueKeyResponse();
    }
  }

  /**
   * Retrieves the class name of the global similarity defined (if any) in the schema.
   */
  public static class GlobalSimilarity extends AbstractSchemaRequest<SchemaResponse.GlobalSimilarityResponse> {
    public GlobalSimilarity() {
      this(null);
    }

    public GlobalSimilarity(SolrParams q) {
      super(METHOD.GET, "/schema/similarity", q);
    }

    @Override
    protected SchemaResponse.GlobalSimilarityResponse createResponse(SolrClient client) {
      return new SchemaResponse.GlobalSimilarityResponse();
    }
  }

  /**
   * Adds a new field definition to the schema.
   * If the field already exists, the method {@link #process(SolrClient, String)} will fail.
   * Note that the request will be translated to json, so please use concrete values (e.g. : true, 1)
   * instead of their string representation (e.g. : "true", "1") for the field attributes expecting
   * boolean or number values.
   */
  public static class AddField extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param fieldAttributes field type attributes that can be used to enrich the field definition.
     * @see <a href="https://lucene.apache.org/solr/guide/defining-fields.html">Defining Solr fields</a>
     */
    public AddField(Map<String, Object> fieldAttributes) {
      this(fieldAttributes, null);
    }

    public AddField(Map<String, Object> fieldAttributes, SolrParams q) {
      super(createRequestParameters(fieldAttributes), q);
    }

    private static NamedList<Object> createRequestParameters(Map<String, Object> fieldAttributes) {
      final NamedList<Object> addFieldParameters = createAddFieldNamedList(fieldAttributes);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("add-field", addFieldParameters);
      return requestParameters;
    }
  }

  /**
   * Replaces a field's definition.  Note that the full definition for a field must be supplied - this command
   * will not partially modify a field's definition.  If the field does not exist in the schema the method call
   * {@link #process(SolrClient, String)} will fail.
   *
   * @see <a href="https://lucene.apache.org/solr/guide/defining-fields.html">Defining Solr fields</a>
   */
  public static class ReplaceField extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param fieldAttributes field type attributes that can be used to enrich the field definition.
     */
    public ReplaceField(Map<String, Object> fieldAttributes) {
      this(fieldAttributes, null);
    }

    public ReplaceField(Map<String, Object> fieldAttributes, SolrParams q) {
      super(createRequestParameters(fieldAttributes), q);
    }

    private static NamedList<Object> createRequestParameters(Map<String, Object> fieldAttributes) {
      final NamedList<Object> replaceFieldParameters = createAddFieldNamedList(fieldAttributes);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("replace-field", replaceFieldParameters);
      return requestParameters;
    }
  }

  /**
   * Removes a field definition from the schema. If the field does not exist in the schema,
   * or if the field is the source or destination of a copy field rule the method call
   * {@link #process(SolrClient, String)} will fail.
   */
  public static class DeleteField extends SingleUpdate {

    /**
     * Creates a new instance of the request.
     *
     * @param fieldName the name of the new field to be removed
     */
    public DeleteField(String fieldName) {
      this(fieldName, null);
    }

    public DeleteField(String fieldName, SolrParams q) {
      super(createRequestParameters(fieldName), q);
    }

    private static NamedList<Object> createRequestParameters(String fieldName) {
      final NamedList<Object> deleteFieldParameters = new NamedList<>();
      deleteFieldParameters.add("name", fieldName);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("delete-field", deleteFieldParameters);
      return requestParameters;
    }
  }

  /**
   * Adds a new dynamic field rule to the schema of the specified collection.
   *
   * @see <a href="https://lucene.apache.org/solr/guide/defining-fields.html">Defining Solr fields</a>
   * @see <a href="https://lucene.apache.org/solr/guide/dynamic-fields.html">Solr dynamic fields</a>
   */
  public static class AddDynamicField extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param fieldAttributes field type attributes that can be used to enrich the field definition.
     */
    public AddDynamicField(Map<String, Object> fieldAttributes) {
      this(fieldAttributes, null);
    }

    public AddDynamicField(Map<String, Object> fieldAttributes, SolrParams q) {
      super(createRequestParameters(fieldAttributes), q);
    }

    private static NamedList<Object> createRequestParameters(Map<String, Object> fieldAttributes) {
      final NamedList<Object> addDynamicFieldParameters = createAddFieldNamedList(fieldAttributes);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("add-dynamic-field", addDynamicFieldParameters);
      return requestParameters;
    }
  }

  /**
   * Replaces a dynamic field rule in the schema of the specified collection.
   * Note that the full definition for a dynamic field rule must be supplied - this command
   * will not partially modify a dynamic field rule's definition.
   * If the dynamic field rule does not exist in the schema the method call
   * {@link #process(SolrClient, String)} will fail.
   */
  public static class ReplaceDynamicField extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param dynamicFieldAttributes field type attributes that can be used to enrich the field definition.
     * @see <a href="https://lucene.apache.org/solr/guide/defining-fields.html">Defining Solr fields</a>
     * @see <a href="https://lucene.apache.org/solr/guide/dynamic-fields.html">Solr dynamic fields</a>
     */
    public ReplaceDynamicField(Map<String, Object> dynamicFieldAttributes) {
      this(dynamicFieldAttributes, null);
    }

    public ReplaceDynamicField(Map<String, Object> dynamicFieldAttributes, SolrParams q) {
      super(createRequestParameters(dynamicFieldAttributes), q);
    }

    private static NamedList<Object> createRequestParameters(
        Map<String, Object> dynamicFieldAttributes) {
      final NamedList<Object> replaceDynamicFieldParameters = createAddFieldNamedList(dynamicFieldAttributes);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("replace-dynamic-field", replaceDynamicFieldParameters);
      return requestParameters;
    }
  }

  /**
   * Deletes a dynamic field rule from your schema. If the dynamic field rule does not exist in the schema,
   * or if the schema contains a copy field rule with a target or destination that matches only this
   * dynamic field rule the method call {@link #process(SolrClient, String)} will fail.
   */
  public static class DeleteDynamicField extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param dynamicFieldName the name of the dynamic field to be removed
     */
    public DeleteDynamicField(String dynamicFieldName) {
      this(dynamicFieldName, null);
    }

    public DeleteDynamicField(String fieldName, SolrParams q) {
      super(createRequestParameters(fieldName), q);
    }

    private static NamedList<Object> createRequestParameters(String fieldName) {
      final NamedList<Object> deleteDynamicFieldParameters = new NamedList<>();
      deleteDynamicFieldParameters.add("name", fieldName);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("delete-dynamic-field", deleteDynamicFieldParameters);
      return requestParameters;
    }
  }

  /**
   * Update request used to add a new field type to the schema.
   */
  public static class AddFieldType extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param fieldTypeDefinition the field type definition
     * @see <a href="https://lucene.apache.org/solr/guide/solr-field-types.html">Solr field types</a>
     */
    public AddFieldType(FieldTypeDefinition fieldTypeDefinition) {
      this(fieldTypeDefinition, null);
    }

    public AddFieldType(FieldTypeDefinition fieldTypeDefinition, SolrParams q) {
      super(createRequestParameters(fieldTypeDefinition), q);
    }

    private static NamedList<Object> createRequestParameters(FieldTypeDefinition fieldTypeDefinition) {
      final NamedList<Object> addFieldTypeParameters = createAddFieldTypeNamedList(fieldTypeDefinition);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("add-field-type", addFieldTypeParameters);
      return requestParameters;
    }
  }

  /**
   * Replaces a field type in schema belonging to the schema of the specified collection.
   * Note that the full definition for a field type must be supplied- this command will not partially modify
   * a field type's definition.  If the field type does not exist in the schema the
   * method call {@link #process(SolrClient, String)} will fail.
   */
  public static class ReplaceFieldType extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param fieldTypeDefinition the field type definition
     * @see <a href="https://lucene.apache.org/solr/guide/solr-field-types.html">Solr field types</a>
     */
    public ReplaceFieldType(FieldTypeDefinition fieldTypeDefinition) {
      this(fieldTypeDefinition, null);
    }

    public ReplaceFieldType(FieldTypeDefinition fieldTypeDefinition, SolrParams q) {
      super(createRequestParameters(fieldTypeDefinition), q);
    }

    private static NamedList<Object> createRequestParameters(FieldTypeDefinition fieldTypeDefinition) {
      final NamedList<Object> replaceFieldTypeParameters = createAddFieldTypeNamedList(fieldTypeDefinition);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("replace-field-type", replaceFieldTypeParameters);
      return requestParameters;
    }
  }

  /**
   * Removes a field type from the schema of the specified collection.
   * If the field type does not exist in the schema, or if any
   * field or dynamic field rule in the schema uses the field type, the
   * method call {@link #process(SolrClient, String)} will fail.
   */
  public static class DeleteFieldType extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param fieldTypeName the name of the field type to be removed
     */
    public DeleteFieldType(String fieldTypeName) {
      this(fieldTypeName, null);
    }

    public DeleteFieldType(String fieldTypeName, SolrParams q) {
      super(createRequestParameters(fieldTypeName), q);
    }

    private static NamedList<Object> createRequestParameters(String fieldTypeName) {
      final NamedList<Object> deleteFieldTypeParameters = new NamedList<>();
      deleteFieldTypeParameters.add("name", fieldTypeName);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("delete-field-type", deleteFieldTypeParameters);
      return requestParameters;
    }
  }

  /**
   * Adds a new copy field rule to the schema of the specified collection.
   */
  public static class AddCopyField extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param source the source field name
     * @param dest   the collection of the destination field names
     * @see <a href="https://lucene.apache.org/solr/guide/copying-fields.html">Copying fields</a>
     */
    public AddCopyField(String source, List<String> dest) {
      this(source, dest, (SolrParams) null);
    }

    /**
     * Creates a new instance of the request.
     *
     * @param source   the source field name
     * @param dest     the collection of the destination field names
     * @param maxChars the number of characters to be copied from the source to the dest. Specifying
     *                 0 as value, means that all the source characters will be copied to the dest.
     * @see <a href="https://lucene.apache.org/solr/guide/copying-fields.html">Copying fields</a>
     */
    public AddCopyField(String source, List<String> dest, Integer maxChars) {
      this(source, dest, maxChars, null);
    }

    public AddCopyField(String source, List<String> dest, SolrParams q) {
      super(createRequestParameters(source, dest, null), q);
    }

    public AddCopyField(String source, List<String> dest, Integer maxChars, SolrParams q) {
      super(createRequestParameters(source, dest, maxChars), q);
    }

    private static NamedList<Object> createRequestParameters(String source, List<String> dest, Integer maxchars) {
      final NamedList<Object> addCopyFieldParameters = new NamedList<>();
      addCopyFieldParameters.add("source", source);
      addCopyFieldParameters.add("dest", dest);
      if (maxchars != null) {
        addCopyFieldParameters.add("maxChars", maxchars);
      }
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("add-copy-field", addCopyFieldParameters);
      return requestParameters;
    }
  }

  /**
   * Deletes a copy field rule from the schema of the specified collection.
   * If the copy field rule does not exist in the schema then the
   * method call {@link #process(SolrClient, String)} will fail.
   */
  public static class DeleteCopyField extends SingleUpdate {
    /**
     * Creates a new instance of the request.
     *
     * @param source the source field name
     * @param dest   the collection of the destination field names
     */
    public DeleteCopyField(String source, List<String> dest) {
      this(source, dest, null);
    }

    public DeleteCopyField(String source, List<String> dest, SolrParams q) {
      super(createRequestParameters(source, dest), q);
    }

    private static NamedList<Object> createRequestParameters(String source, List<String> dest) {
      final NamedList<Object> addCopyFieldParameters = new NamedList<>();
      addCopyFieldParameters.add("source", source);
      addCopyFieldParameters.add("dest", dest);
      final NamedList<Object> requestParameters = new NamedList<>();
      requestParameters.add("delete-copy-field", addCopyFieldParameters);
      return requestParameters;
    }
  }

  public abstract static class Update extends AbstractSchemaRequest<SchemaResponse.UpdateResponse> {

    public Update() {
      this(null);
    }

    public Update(SolrParams q) {
      super(METHOD.POST, "/schema", q);
    }

    protected abstract NamedList<Object> getRequestParameters();

    @Override
    public RequestWriter.ContentWriter getContentWriter(String expectedType) {
      return new RequestWriter.ContentWriter() {
        @Override
        public void write(OutputStream os) throws IOException {
          Utils.writeJson(getRequestParameters(),
              os, false);
        }

        @Override
        public String getContentType() {
          return CommonParams.JSON_MIME;
        }
      };
    }

    @Override
    protected SchemaResponse.UpdateResponse createResponse(SolrClient client) {
      return new SchemaResponse.UpdateResponse();
    }
  }

  private static abstract class SingleUpdate extends Update {
    private final NamedList<Object> requestParameters;

    public SingleUpdate(NamedList<Object> requestParameters) {
      this(requestParameters, null);
    }

    public SingleUpdate(NamedList<Object> requestParameters, SolrParams q) {
      super(q);
      this.requestParameters = requestParameters;
    }

    @Override
    protected NamedList<Object> getRequestParameters() {
      return requestParameters;
    }
  }

  /**
   * <p>The Schema API offers the possibility to perform one or more add requests in a single command.</p>
   * <p>The API is transactional and all commands in a single {@link #process(SolrClient, String)} call
   * either succeed or fail together.</p>
   */
  public static class MultiUpdate extends Update {
    private List<Update> updateSchemaRequests = new LinkedList<>();

    public MultiUpdate(List<Update> updateSchemaRequests) {
      this(updateSchemaRequests, null);
    }

    public MultiUpdate(List<Update> updateSchemaRequests, SolrParams q) {
      super(q);
      if (updateSchemaRequests == null) {
        throw new IllegalArgumentException("updateSchemaRequests must be non-null");
      }
      for (Update updateSchemaRequest : updateSchemaRequests) {
        if (updateSchemaRequest == null) {
          throw new IllegalArgumentException("updateSchemaRequests elements must be non-null");
        }
        this.updateSchemaRequests.add(updateSchemaRequest);
      }
    }

    @Override
    protected NamedList<Object> getRequestParameters() {
      NamedList<Object> multipleRequestsParameters = new NamedList<>();
      for (Update updateSchemaRequest : updateSchemaRequests) {
        multipleRequestsParameters.addAll(updateSchemaRequest.getRequestParameters());
      }
      return multipleRequestsParameters;
    }
  }

}
