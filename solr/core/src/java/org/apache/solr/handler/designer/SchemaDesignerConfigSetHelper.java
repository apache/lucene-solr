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

package org.apache.solr.handler.designer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.ManagedIndexSchemaFactory;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;
import static org.apache.solr.common.util.Utils.fromJSONString;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.common.util.Utils.toJavabin;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;
import static org.apache.solr.handler.designer.SchemaDesignerAPI.getConfigSetZkPath;
import static org.apache.solr.handler.designer.SchemaDesignerAPI.getMutableId;
import static org.apache.solr.schema.IndexSchema.NEST_PATH_FIELD_NAME;
import static org.apache.solr.schema.IndexSchema.ROOT_FIELD_NAME;
import static org.apache.solr.schema.ManagedIndexSchemaFactory.DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME;

class SchemaDesignerConfigSetHelper implements SchemaDesignerConstants {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Set<String> removeFieldProps = new HashSet<>(Arrays.asList("href", "id", "copyDest"));
  private static final List<String> includeLangIds = Arrays.asList("ws", "general", "rev", "sort");
  private static final String ZNODE_PATH_DELIM = "/";
  private static final String MULTIVALUED = "multiValued";
  private static final int TEXT_PREFIX_LEN = "text_".length();


  private final CoreContainer cc;
  private final SchemaSuggester schemaSuggester;
  private final ZkConfigManager configManager;

  SchemaDesignerConfigSetHelper(CoreContainer cc, SchemaSuggester schemaSuggester) {
    this.cc = cc;
    this.schemaSuggester = schemaSuggester;
    this.configManager = new ZkConfigManager(cc.getZkController().getZkClient());
  }

  @SuppressWarnings("unchecked")
  Map<String, Object> analyzeField(String configSet, String fieldName, String fieldText) throws IOException {
    final String mutableId = getMutableId(configSet);
    final URI uri;
    try {
      uri = collectionApiEndpoint(mutableId, "analysis", "field")
          .setParameter(CommonParams.WT, CommonParams.JSON)
          .setParameter("analysis.showmatch", "true")
          .setParameter("analysis.fieldname", fieldName)
          .setParameter("analysis.fieldvalue", "POST")
          .build();
    } catch (URISyntaxException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    Map<String, Object> analysis = Collections.emptyMap();
    HttpPost httpPost = new HttpPost(uri);
    httpPost.setHeader("Content-Type", "text/plain");
    httpPost.setEntity(new ByteArrayEntity(fieldText.getBytes(StandardCharsets.UTF_8)));
    try {
      HttpResponse resp = cloudClient().getHttpClient().execute(httpPost);
      int statusCode = resp.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
            EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8));
      }

      Map<String, Object> response = (Map<String, Object>) fromJSONString(EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8));
      if (response != null) {
        analysis = (Map<String, Object>) response.get("analysis");
      }
    } finally {
      httpPost.releaseConnection();
    }

    return analysis;
  }

  List<String> listCollectionsForConfig(String configSet) {
    final List<String> collections = new LinkedList<>();
    Map<String, ClusterState.CollectionRef> states = zkStateReader().getClusterState().getCollectionStates();
    for (Map.Entry<String, ClusterState.CollectionRef> e : states.entrySet()) {
      final String coll = e.getKey();
      if (coll.startsWith(DESIGNER_PREFIX)) {
        continue; // ignore temp
      }

      try {
        if (configSet.equals(zkStateReader().readConfigName(coll)) && e.getValue().get() != null) {
          collections.add(coll);
        }
      } catch (Exception exc) {
        log.warn("Failed to get config name for {}", coll, exc);
      }
    }
    return collections;
  }

  @SuppressWarnings("unchecked")
  public String addSchemaObject(String configSet, Map<String, Object> addJson) throws IOException, SolrServerException {
    String mutableId = getMutableId(configSet);
    SchemaRequest.Update addAction;
    String action;
    String objectName = null;
    if (addJson.containsKey("add-field")) {
      action = "add-field";
      Map<String, Object> fieldAttrs = (Map<String, Object>) addJson.get(action);
      objectName = (String) fieldAttrs.get("name");
      addAction = new SchemaRequest.AddField(fieldAttrs);
    } else if (addJson.containsKey("add-dynamic-field")) {
      action = "add-dynamic-field";
      Map<String, Object> fieldAttrs = (Map<String, Object>) addJson.get(action);
      objectName = (String) fieldAttrs.get("name");
      addAction = new SchemaRequest.AddDynamicField(fieldAttrs);
    } else if (addJson.containsKey("add-copy-field")) {
      action = "add-copy-field";
      Map<String, Object> map = (Map<String, Object>) addJson.get(action);
      Object dest = map.get("dest");
      List<String> destFields = null;
      if (dest instanceof String) {
        destFields = Collections.singletonList((String) dest);
      } else if (dest instanceof List) {
        destFields = (List<String>) dest;
      } else if (dest instanceof Collection) {
        Collection<String> destColl = (Collection<String>) dest;
        destFields = new ArrayList<>(destColl);
      }
      addAction = new SchemaRequest.AddCopyField((String) map.get("source"), destFields);
    } else if (addJson.containsKey("add-field-type")) {
      action = "add-field-type";
      Map<String, Object> fieldAttrs = (Map<String, Object>) addJson.get(action);
      objectName = (String) fieldAttrs.get("name");
      FieldTypeDefinition ftDef = new FieldTypeDefinition();
      ftDef.setAttributes(fieldAttrs);
      addAction = new SchemaRequest.AddFieldType(ftDef);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported action in request body! " + addJson);
    }

    // Using the SchemaAPI vs. working on the schema directly because SchemaField.create methods are package protected
    log.info("Sending {} request for configSet {}: {}", action, mutableId, addJson);
    SchemaResponse.UpdateResponse schemaResponse = addAction.process(cloudClient(), mutableId);
    Exception exc = schemaResponse.getException();
    if (exc instanceof SolrException) {
      throw (SolrException) exc;
    } else if (schemaResponse.getStatus() != 0) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exc);
    }

    return objectName;
  }

  void reloadTempCollection(String mutableId, boolean delete) throws IOException, SolrServerException {
    if (delete) {
      log.debug("Deleting and re-creating existing collection {} after schema update", mutableId);
      CollectionAdminRequest.deleteCollection(mutableId).process(cloudClient());
      try {
        zkStateReader().waitForState(mutableId, 30, TimeUnit.SECONDS, Objects::isNull);
      } catch (InterruptedException | TimeoutException e) {
        throw new IOException("Failed to see deleted collection " + mutableId + " reflected in cluster state", SolrZkClient.checkInterrupted(e));
      }
      createCollection(mutableId, mutableId);
      log.debug("Deleted and re-created existing collection: {}", mutableId);
    } else {
      CollectionAdminRequest.reloadCollection(mutableId).process(cloudClient());
      log.debug("Reloaded existing collection: {}", mutableId);
    }
  }

  Map<String, Object> updateSchemaObject(String configSet, Map<String, Object> updateJson, ManagedIndexSchema schemaBeforeUpdate) throws IOException, SolrServerException {
    String name = (String) updateJson.get("name");
    String mutableId = getMutableId(configSet);

    boolean needsRebuild = false;

    SolrException solrExc = null;
    String updateType;
    String updateError = null;

    if (updateJson.get("type") != null) {
      updateType = schemaBeforeUpdate.isDynamicField(name) ? "dynamicField" : "field";
      try {
        needsRebuild = updateField(configSet, updateJson, schemaBeforeUpdate);
      } catch (SolrException exc) {
        if (exc.code() != 400) {
          throw exc;
        }
        solrExc = exc;
        updateError = solrExc.getMessage() + " Previous settings will be restored.";
      }
    } else {
      updateType = "type";
      needsRebuild = updateFieldType(configSet, name, updateJson, schemaBeforeUpdate);
    }

    // the update may have required a full rebuild of the index, otherwise, it's just a reload / re-index sample
    reloadTempCollection(mutableId, needsRebuild);

    Map<String, Object> results = new HashMap<>();
    results.put("rebuild", needsRebuild);
    results.put("updateType", updateType);
    if (updateError != null) {
      results.put("updateError", updateError);
    }
    if (solrExc != null) {
      results.put("solrExc", solrExc);
    }
    return results;
  }

  protected boolean updateFieldType(String configSet, String typeName, Map<String, Object> updateJson, ManagedIndexSchema schemaBeforeUpdate) {
    boolean needsRebuild = false;

    Map<String, Object> typeAttrs = updateJson.entrySet().stream()
        .filter(e -> !removeFieldProps.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    FieldType fieldType = schemaBeforeUpdate.getFieldTypeByName(typeName);

    // this is a field type
    Object multiValued = typeAttrs.get(MULTIVALUED);
    if (typeHasMultiValuedChange(multiValued, fieldType)) {
      needsRebuild = true;
      log.warn("Re-building the temp collection for {} after type {} updated to multi-valued {}", configSet, typeName, multiValued);
    }

    // nice, the json for this field looks like
    // "synonymQueryStyle": "org.apache.solr.parser.SolrQueryParserBase$SynonymQueryStyle:AS_SAME_TERM"
    if (typeAttrs.get("synonymQueryStyle") instanceof String) {
      String synonymQueryStyle = (String) typeAttrs.get("synonymQueryStyle");
      if (synonymQueryStyle.lastIndexOf(':') != -1) {
        typeAttrs.put("synonymQueryStyle", synonymQueryStyle.substring(synonymQueryStyle.lastIndexOf(':') + 1));
      }
    }

    ManagedIndexSchema updatedSchema =
        schemaBeforeUpdate.replaceFieldType(fieldType.getTypeName(), (String) typeAttrs.get("class"), typeAttrs);
    updatedSchema.persistManagedSchema(false);

    return needsRebuild;
  }

  boolean updateField(String configSet, Map<String, Object> updateField, ManagedIndexSchema schemaBeforeUpdate) throws IOException, SolrServerException {
    String mutableId = getMutableId(configSet);

    String name = (String) updateField.get("name");
    String type = (String) updateField.get("type");
    String copyDest = (String) updateField.get("copyDest");
    Map<String, Object> fieldAttributes = updateField.entrySet().stream()
        .filter(e -> !removeFieldProps.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    boolean needsRebuild = false;

    SchemaField schemaField = schemaBeforeUpdate.getField(name);
    boolean isDynamic = schemaBeforeUpdate.isDynamicField(name);
    String currentType = schemaField.getType().getTypeName();

    SimpleOrderedMap<Object> fromTypeProps;
    if (type.equals(currentType)) {
      // no type change, so just pull the current type's props (with defaults) as we'll use these
      // to determine which props get explicitly overridden on the field
      fromTypeProps = schemaBeforeUpdate.getFieldTypeByName(currentType).getNamedPropertyValues(true);
    } else {
      // validate type change
      FieldType newType = schemaBeforeUpdate.getFieldTypeByName(type);
      if (newType == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Invalid update request for field " + name + "! Field type " + type + " doesn't exist!");
      }
      validateTypeChange(configSet, schemaField, newType);

      // type change looks valid
      fromTypeProps = newType.getNamedPropertyValues(true);
    }

    // the diff holds all the explicit properties not inherited from the type
    Map<String, Object> diff = new HashMap<>();
    for (Map.Entry<String, Object> e : fieldAttributes.entrySet()) {
      String attr = e.getKey();
      Object attrValue = e.getValue();
      if ("name".equals(attr) || "type".equals(attr)) {
        continue; // we don't want these in the diff map
      }

      if ("required".equals(attr)) {
        diff.put(attr, attrValue != null ? attrValue : false);
      } else {
        Object fromType = fromTypeProps.get(attr);
        if (fromType == null || !fromType.equals(attrValue)) {
          diff.put(attr, attrValue);
        }
      }
    }

    // detect if they're trying to copy multi-valued fields into a single-valued field
    Object multiValued = diff.get(MULTIVALUED);
    if (multiValued == null) {
      // mv not overridden explicitly, but we need the actual value, which will come from the new type (if that changed) or the current field
      multiValued = type.equals(currentType) ? schemaField.multiValued() : schemaBeforeUpdate.getFieldTypeByName(type).isMultiValued();
    }

    if (!isDynamic && Boolean.FALSE.equals(multiValued)) {
      // make sure there are no mv source fields if this is a copy dest
      for (String src : schemaBeforeUpdate.getCopySources(name)) {
        SchemaField srcField = schemaBeforeUpdate.getField(src);
        if (srcField.multiValued()) {
          log.warn("Cannot change multi-valued field {} to single-valued because it is a copy field destination for multi-valued field {}", name, src);
          multiValued = Boolean.TRUE;
          diff.put(MULTIVALUED, multiValued);
          break;
        }
      }
    }

    if (Boolean.FALSE.equals(multiValued) && schemaField.multiValued()) {
      // changing from multi- to single value ... verify the data agrees ...
      validateMultiValuedChange(configSet, schemaField, Boolean.FALSE);
    }

    // switch from single-valued to multi-valued requires a full rebuild
    // See SOLR-12185 ... if we're switching from single to multi-valued, then it's a big operation
    if (fieldHasMultiValuedChange(multiValued, schemaField)) {
      needsRebuild = true;
      log.warn("Need to rebuild the temp collection for {} after field {} updated to multi-valued {}", configSet, name, multiValued);
    }

    if (!needsRebuild) {
      // check term vectors too
      Boolean storeTermVector = (Boolean) fieldAttributes.getOrDefault("termVectors", Boolean.FALSE);
      if (schemaField.storeTermVector() != storeTermVector) {
        // cannot change termVectors w/o a full-rebuild
        needsRebuild = true;
      }
    }

    log.info("For {}, replacing field {} with attributes: {}", configSet, name, diff);
    final FieldType fieldType = schemaBeforeUpdate.getFieldTypeByName(type);
    ManagedIndexSchema updatedSchema = isDynamic ? schemaBeforeUpdate.replaceDynamicField(name, fieldType, diff)
        : schemaBeforeUpdate.replaceField(name, fieldType, diff);

    // persist the change before applying the copy-field updates
    updatedSchema.persistManagedSchema(false);

    if (!isDynamic) {
      applyCopyFieldUpdates(mutableId, copyDest, name, updatedSchema);
    }

    return needsRebuild;
  }

  protected void validateMultiValuedChange(String configSet, SchemaField field, Boolean multiValued) throws IOException {
    List<SolrInputDocument> docs = getStoredSampleDocs(configSet);
    if (!docs.isEmpty()) {
      boolean isMV = schemaSuggester.isMultiValued(field.getName(), docs);
      if (isMV && !multiValued) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Cannot change field " + field.getName() + " to single-valued as some sample docs have multiple values!");
      }
    }
  }

  protected void validateTypeChange(String configSet, SchemaField field, FieldType toType) throws IOException {
    if ("_version_".equals(field.getName())) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Cannot change type of the _version_ field; it must be a plong.");
    }
    List<SolrInputDocument> docs = getStoredSampleDocs(configSet);
    if (!docs.isEmpty()) {
      schemaSuggester.validateTypeChange(field, toType, docs);
    }
  }

  void deleteStoredSampleDocs(String configSet) {
    try {
      cloudClient().deleteByQuery(BLOB_STORE_ID, "id:" + configSet + "_sample/*", 10);
    } catch (IOException | SolrServerException | SolrException exc) {
      final String excStr = exc.toString();
      log.warn("Failed to delete sample docs from blob store for {} due to: {}", configSet, excStr);
    }
  }

  @SuppressWarnings("unchecked")
  List<SolrInputDocument> getStoredSampleDocs(final String configSet) throws IOException {
    List<SolrInputDocument> docs = null;

    final URI uri;
    try {
      uri = collectionApiEndpoint(BLOB_STORE_ID, "blob", configSet + "_sample")
          .setParameter(CommonParams.WT, "filestream")
          .build();
    } catch (URISyntaxException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    HttpGet httpGet = new HttpGet(uri);
    try {
      HttpResponse entity = cloudClient().getHttpClient().execute(httpGet);
      int statusCode = entity.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK) {
        byte[] bytes = DefaultSampleDocumentsLoader.streamAsBytes(entity.getEntity().getContent());
        if (bytes.length > 0) {
          docs = (List<SolrInputDocument>) Utils.fromJavabin(bytes);
        }
      } else if (statusCode != HttpStatus.SC_NOT_FOUND) {
        byte[] bytes = DefaultSampleDocumentsLoader.streamAsBytes(entity.getEntity().getContent());
        throw new IOException("Failed to lookup stored docs for " + configSet + " due to: " + new String(bytes, StandardCharsets.UTF_8));
      } // else not found is ok
    } finally {
      httpGet.releaseConnection();
    }
    return docs != null ? docs : Collections.emptyList();
  }

  void storeSampleDocs(final String configSet, List<SolrInputDocument> docs) throws IOException {
    docs.forEach(d -> d.removeField(VERSION_FIELD)); // remove _version_ field before storing ...
    postDataToBlobStore(cloudClient(), configSet + "_sample",
        DefaultSampleDocumentsLoader.streamAsBytes(toJavabin(docs)));
  }

  protected void postDataToBlobStore(CloudSolrClient cloudClient, String blobName, byte[] bytes) throws IOException {
    final URI uri;
    try {
      uri = collectionApiEndpoint(BLOB_STORE_ID, "blob", blobName).build();
    } catch (URISyntaxException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    HttpPost httpPost = new HttpPost(uri);
    try {
      httpPost.setHeader("Content-Type", "application/octet-stream");
      httpPost.setEntity(new ByteArrayEntity(bytes));
      HttpResponse resp = cloudClient.getHttpClient().execute(httpPost);
      int statusCode = resp.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
            EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8));
      }
    } finally {
      httpPost.releaseConnection();
    }
  }

  private String getBaseUrl(final String collection) {
    String baseUrl = null;
    try {
      Set<String> liveNodes = zkStateReader().getClusterState().getLiveNodes();
      DocCollection docColl = zkStateReader().getCollection(collection);
      if (docColl != null && !liveNodes.isEmpty()) {
        Optional<Replica> maybeActive = docColl.getReplicas().stream().filter(r -> r.isActive(liveNodes)).findAny();
        if (maybeActive.isPresent()) {
          baseUrl = maybeActive.get().getBaseUrl();
        }
      }
    } catch (Exception exc) {
      log.warn("Failed to lookup base URL for collection {}", collection, exc);
    }

    if (baseUrl == null) {
      baseUrl = zkStateReader().getBaseUrlForNodeName(cc.getZkController().getNodeName());
    }

    return baseUrl;
  }

  private URIBuilder collectionApiEndpoint(final String collection, final String... morePathSegments) throws URISyntaxException {
    URI baseUrl = new URI(getBaseUrl(collection));
    // build up a list of path segments including any path in the base URL, collection, and additional segments provided by caller
    List<String> path = new ArrayList<>(URLEncodedUtils.parsePathSegments(baseUrl.getPath()));
    path.add(collection);
    if (morePathSegments != null && morePathSegments.length > 0) {
      path.addAll(Arrays.asList(morePathSegments));
    }
    return new URIBuilder(baseUrl).setPathSegments(path);
  }

  protected String getManagedSchemaZkPath(final String configSet) {
    return getConfigSetZkPath(configSet, DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME);
  }

  ManagedIndexSchema toggleNestedDocsFields(ManagedIndexSchema schema, boolean enabled) {
    return enabled ? enableNestedDocsFields(schema, true) : deleteNestedDocsFieldsIfNeeded(schema, true);
  }

  ManagedIndexSchema enableNestedDocsFields(ManagedIndexSchema schema, boolean persist) {
    boolean madeChanges = false;

    if (!schema.hasExplicitField(ROOT_FIELD_NAME)) {
      Map<String, Object> fieldAttrs = makeMap("docValues", false, "indexed", true, "stored", false);
      schema = (ManagedIndexSchema) schema.addField(schema.newField(ROOT_FIELD_NAME, "string", fieldAttrs), false);
      madeChanges = true;
    }

    if (!schema.hasExplicitField(NEST_PATH_FIELD_NAME)) {
      schema = (ManagedIndexSchema) schema.addField(schema.newField(NEST_PATH_FIELD_NAME, NEST_PATH_FIELD_NAME, Collections.emptyMap()), false);
      madeChanges = true;
    }

    if (madeChanges && persist) {
      schema.persistManagedSchema(false);
    }

    return schema;
  }

  ManagedIndexSchema deleteNestedDocsFieldsIfNeeded(ManagedIndexSchema schema, boolean persist) {
    List<String> toDelete = new LinkedList<>();
    if (schema.hasExplicitField(ROOT_FIELD_NAME)) {
      toDelete.add(ROOT_FIELD_NAME);
    }
    if (schema.hasExplicitField(NEST_PATH_FIELD_NAME)) {
      toDelete.add(NEST_PATH_FIELD_NAME);
    }
    if (!toDelete.isEmpty()) {
      schema = schema.deleteFields(toDelete);
      if (persist) {
        schema.persistManagedSchema(false);
      }
    }
    return schema;
  }

  SolrConfig loadSolrConfig(String configSet) {
    SolrResourceLoader resourceLoader = cc.getResourceLoader();
    ZkSolrResourceLoader zkLoader =
        new ZkSolrResourceLoader(resourceLoader.getInstancePath(), configSet, resourceLoader.getClassLoader(), new Properties(), cc.getZkController());
    return SolrConfig.readFromResourceLoader(zkLoader, SOLR_CONFIG_XML, true, null);
  }

  ManagedIndexSchema loadLatestSchema(String configSet) {
    return loadLatestSchema(loadSolrConfig(configSet));
  }

  ManagedIndexSchema loadLatestSchema(SolrConfig solrConfig) {
    ManagedIndexSchemaFactory factory = new ManagedIndexSchemaFactory();
    factory.init(new NamedList<>());
    return factory.create(DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME, solrConfig, null);
  }

  int getCurrentSchemaVersion(final String configSet) throws IOException {
    int currentVersion = -1;
    final String path = getManagedSchemaZkPath(configSet);
    try {
      Stat stat = cc.getZkController().getZkClient().exists(path, null, true);
      if (stat != null) {
        currentVersion = stat.getVersion();
      }
    } catch (KeeperException.NoNodeException notExists) {
      // safe to ignore
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error getting version for schema: " + configSet, SolrZkClient.checkInterrupted(e));
    }
    return currentVersion;
  }

  void createCollection(final String collection, final String configSet) throws IOException, SolrServerException {
    createCollection(collection, configSet, 1, 1);
  }

  void createCollection(final String collection, final String configSet, int numShards, int numReplicas) throws IOException, SolrServerException {
    RTimer timer = new RTimer();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, configSet, numShards, numReplicas);
    create.setMaxShardsPerNode(-1);
    SolrResponse rsp = create.process(cloudClient());
    try {
      CollectionsHandler.waitForActiveCollection(collection, cc, rsp);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed waiting for new collection " + collection + " to reach the active state",
          SolrZkClient.checkInterrupted(e));
    }
    double tookMs = timer.getTime();
    log.debug("Took {} ms to create new collection {} with configSet {}", tookMs, collection, configSet);
  }

  protected CloudSolrClient cloudClient() {
    return cc.getSolrClientCache().getCloudSolrClient(cc.getZkController().getZkServerAddress());
  }

  protected ZkStateReader zkStateReader() {
    return cc.getZkController().getZkStateReader();
  }

  boolean applyCopyFieldUpdates(String mutableId, String copyDest, String fieldName, ManagedIndexSchema schema) throws IOException, SolrServerException {
    boolean updated = false;

    if (copyDest == null || copyDest.trim().isEmpty()) {
      // delete all the copy field directives for this field
      List<CopyField> copyFieldsList = schema.getCopyFieldsList(fieldName);
      if (!copyFieldsList.isEmpty()) {
        List<String> dests = copyFieldsList.stream().map(cf -> cf.getDestination().getName()).collect(Collectors.toList());
        SchemaRequest.DeleteCopyField delAction = new SchemaRequest.DeleteCopyField(fieldName, dests);
        SchemaResponse.UpdateResponse schemaResponse = delAction.process(cloudClient(), mutableId);
        if (schemaResponse.getStatus() != 0) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, schemaResponse.getException());
        }
        updated = true;
      }
    } else {
      SchemaField field = schema.getField(fieldName);
      Set<String> desired = new HashSet<>();
      for (String dest : copyDest.trim().split(",")) {
        String toAdd = dest.trim();
        if (toAdd.equals(fieldName)) {
          continue; // cannot copy to self
        }

        // make sure the field exists and is multi-valued if this field is
        SchemaField toAddField = schema.getFieldOrNull(toAdd);
        if (toAddField != null) {
          if (!field.multiValued() || toAddField.multiValued()) {
            desired.add(toAdd);
          } else {
            log.warn("Skipping copy-field dest {} for {} because it is not multi-valued!", toAdd, fieldName);
          }
        } else {
          log.warn("Skipping copy-field dest {} for {} because it doesn't exist!", toAdd, fieldName);
        }
      }
      Set<String> existing = schema.getCopyFieldsList(fieldName).stream().map(cf -> cf.getDestination().getName()).collect(Collectors.toSet());
      Set<String> add = Sets.difference(desired, existing);
      if (!add.isEmpty()) {
        SchemaRequest.AddCopyField addAction = new SchemaRequest.AddCopyField(fieldName, new ArrayList<>(add));
        SchemaResponse.UpdateResponse schemaResponse = addAction.process(cloudClient(), mutableId);
        if (schemaResponse.getStatus() != 0) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, schemaResponse.getException());
        }
        updated = true;
      } // no additions ...

      Set<String> del = Sets.difference(existing, desired);
      if (!del.isEmpty()) {
        SchemaRequest.DeleteCopyField delAction = new SchemaRequest.DeleteCopyField(fieldName, new ArrayList<>(del));
        SchemaResponse.UpdateResponse schemaResponse = delAction.process(cloudClient(), mutableId);
        if (schemaResponse.getStatus() != 0) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, schemaResponse.getException());
        }
        updated = true;
      } // no deletions ...
    }

    return updated;
  }

  protected boolean fieldHasMultiValuedChange(Object multiValued, SchemaField schemaField) {
    return (multiValued == null ||
        (Boolean.TRUE.equals(multiValued) && !schemaField.multiValued()) ||
        (Boolean.FALSE.equals(multiValued) && schemaField.multiValued()));
  }

  protected boolean typeHasMultiValuedChange(Object multiValued, FieldType fieldType) {
    return (multiValued == null ||
        (Boolean.TRUE.equals(multiValued) && !fieldType.isMultiValued()) ||
        (Boolean.FALSE.equals(multiValued) && fieldType.isMultiValued()));
  }

  ManagedIndexSchema syncLanguageSpecificObjectsAndFiles(String configSet, ManagedIndexSchema schema, List<String> langs, boolean dynamicEnabled, String copyFrom) throws IOException {
    if (!langs.isEmpty()) {
      // there's a subset of languages applied, so remove all the other langs
      schema = removeLanguageSpecificObjectsAndFiles(configSet, schema, langs);
    }

    // now restore any missing types / files for the languages we need, optionally adding back dynamic fields too
    schema = restoreLanguageSpecificObjectsAndFiles(configSet, schema, langs, dynamicEnabled, copyFrom);

    schema.persistManagedSchema(false);
    return schema;
  }

  protected ManagedIndexSchema removeLanguageSpecificObjectsAndFiles(String configSet, ManagedIndexSchema schema, List<String> langs) throws IOException {
    final Set<String> languages = new HashSet<>(includeLangIds);
    languages.addAll(langs);

    final Set<String> usedTypes =
        schema.getFields().values().stream().map(f -> f.getType().getTypeName()).collect(Collectors.toSet());

    final Set<String> usedLangs =
        schema.getFields().values().stream().filter(f -> isTextType(f.getType()))
            .map(f -> f.getType().getTypeName().substring(TEXT_PREFIX_LEN)).collect(Collectors.toSet());

    // don't remove types / files for langs that are explicitly being used by a field
    languages.addAll(usedLangs);

    Map<String, FieldType> types = schema.getFieldTypes();
    final Set<String> toRemove = types.values().stream()
        .filter(this::isTextType)
        .filter(t -> !languages.contains(t.getTypeName().substring(TEXT_PREFIX_LEN)))
        .map(FieldType::getTypeName)
        .filter(t -> !usedTypes.contains(t)) // not explicitly used by a field
        .collect(Collectors.toSet());

    // find dynamic fields that refer to the types we're removing ...
    List<String> toRemoveDF = Arrays.stream(schema.getDynamicFields())
        .filter(df -> toRemove.contains(df.getPrototype().getType().getTypeName()))
        .map(df -> df.getPrototype().getName())
        .collect(Collectors.toList());

    schema = schema.deleteDynamicFields(toRemoveDF);
    schema = schema.deleteFieldTypes(toRemove);

    SolrZkClient zkClient = cc.getZkController().getZkClient();
    final String configPathInZk = ZkConfigManager.CONFIGS_ZKNODE + ZNODE_PATH_DELIM + configSet;
    final Set<String> toRemoveFiles = new HashSet<>();
    final Set<String> langExt = languages.stream().map(l -> "_" + l).collect(Collectors.toSet());
    try {
      ZkMaintenanceUtils.traverseZkTree(zkClient, configPathInZk, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, path -> {
        if (!isMatchingLangOrNonLangFile(path, langExt)) toRemoveFiles.add(path);
      });
    } catch (KeeperException.NoNodeException nne) {
      // no-op
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to traverse znode path: " + configPathInZk, SolrZkClient.checkInterrupted(e));
    }

    for (String path : toRemoveFiles) {
      try {
        zkClient.delete(path, -1, false);
      } catch (KeeperException.NoNodeException nne) {
        // no-op
      } catch (KeeperException | InterruptedException e) {
        throw new IOException("Failed to delete znode: " + path, SolrZkClient.checkInterrupted(e));
      }
    }

    return schema;
  }

  protected ManagedIndexSchema restoreLanguageSpecificObjectsAndFiles(String configSet,
                                                                      ManagedIndexSchema schema,
                                                                      List<String> langs,
                                                                      boolean dynamicEnabled,
                                                                      String copyFrom) throws IOException {
    // pull the dynamic fields from the copyFrom schema
    ManagedIndexSchema copyFromSchema = loadLatestSchema(copyFrom);

    final Set<String> langSet = new HashSet<>(includeLangIds);
    langSet.addAll(langs);

    boolean restoreAllLangs = langs.isEmpty();

    final Set<String> langFilesToRestore = new HashSet<>();

    // Restore missing files
    SolrZkClient zkClient = zkStateReader().getZkClient();
    String configPathInZk = ZkConfigManager.CONFIGS_ZKNODE + ZNODE_PATH_DELIM + copyFrom;
    final Set<String> langExt = langSet.stream().map(l -> "_" + l).collect(Collectors.toSet());
    try {
      ZkMaintenanceUtils.traverseZkTree(zkClient, configPathInZk, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, path -> {
        if (path.endsWith(".txt")) {
          if (restoreAllLangs) {
            langFilesToRestore.add(path);
            return;
          }

          final String pathWoExt = path.substring(0, path.length() - 4);
          for (String lang : langExt) {
            if (pathWoExt.endsWith(lang)) {
              langFilesToRestore.add(path);
              break;
            }
          }
        }
      });
    } catch (KeeperException.NoNodeException nne) {
      // no-op
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to traverse znode path: " + configPathInZk, SolrZkClient.checkInterrupted(e));
    }

    if (!langFilesToRestore.isEmpty()) {
      final String replacePathDir = "/" + configSet;
      final String origPathDir = "/" + copyFrom;
      for (String path : langFilesToRestore) {
        String copyToPath = path.replace(origPathDir, replacePathDir);
        try {
          if (!zkClient.exists(copyToPath, true)) {
            zkClient.makePath(copyToPath, false, true);
            zkClient.setData(copyToPath, zkClient.getData(path, null, null, true), true);
          }
        } catch (KeeperException | InterruptedException e) {
          throw new IOException("Failed to restore file at znode path: " + copyToPath, SolrZkClient.checkInterrupted(e));
        }
      }
    }

    // Restore field types
    final Map<String, FieldType> existingTypes = schema.getFieldTypes();
    List<FieldType> addTypes = copyFromSchema.getFieldTypes().values().stream()
        .filter(t -> isLangTextType(t, restoreAllLangs ? null : langSet) && !existingTypes.containsKey(t.getTypeName()))
        .collect(Collectors.toList());
    if (!addTypes.isEmpty()) {
      schema = schema.addFieldTypes(addTypes, false);
    }

    if (dynamicEnabled) {
      // restore language specific dynamic fields
      final Set<String> existingDynFields = Arrays.stream(schema.getDynamicFieldPrototypes())
          .map(SchemaField::getName)
          .collect(Collectors.toSet());

      final Set<String> langFieldTypeNames = schema.getFieldTypes().values().stream()
          .filter(t -> isLangTextType(t, restoreAllLangs ? null : langSet))
          .map(FieldType::getTypeName)
          .collect(Collectors.toSet());

      List<SchemaField> addDynFields = Arrays.stream(copyFromSchema.getDynamicFields())
          .filter(df -> langFieldTypeNames.contains(df.getPrototype().getType().getTypeName()))
          .filter(df -> !existingDynFields.contains(df.getPrototype().getName()))
          .map(IndexSchema.DynamicField::getPrototype)
          .collect(Collectors.toList());
      if (!addDynFields.isEmpty()) {
        schema = schema.addDynamicFields(addDynFields, null, false);
      }
    } else {
      schema = removeDynamicFields(schema);
    }

    return schema;
  }

  private boolean isMatchingLangOrNonLangFile(final String path, final Set<String> langs) {
    if (!path.endsWith(".txt"))
      return true; // not a .txt file, always include

    int slashAt = path.lastIndexOf('/');
    String fileName = slashAt != -1 ? path.substring(slashAt + 1) : "";
    if (!fileName.contains("_"))
      return true; // looking for file names like stopwords_en.txt, not a match, so skip it

    // remove the .txt extension
    final String pathWoExt = fileName.substring(0, fileName.length() - 4);
    for (String lang : langs) {
      if (pathWoExt.endsWith(lang)) {
        return true;
      }
    }

    // if we fall thru to here, then the file should be excluded
    return false;
  }

  private boolean isTextType(final FieldType t) {
    return t.getTypeName().startsWith("text_") && TextField.class.equals(t.getClass());
  }

  private boolean isLangTextType(final FieldType t, final Set<String> langSet) {
    return isTextType(t) && (langSet == null || langSet.contains(t.getTypeName().substring("text_".length())));
  }

  protected ManagedIndexSchema removeDynamicFields(ManagedIndexSchema schema) {
    List<String> dynamicFieldNames =
        Arrays.stream(schema.getDynamicFields()).map(f -> f.getPrototype().getName()).collect(Collectors.toList());
    if (!dynamicFieldNames.isEmpty()) {
      schema = schema.deleteDynamicFields(dynamicFieldNames);
    }
    return schema;
  }

  protected ManagedIndexSchema restoreDynamicFields(ManagedIndexSchema schema, List<String> langs, String copyFrom) {
    // pull the dynamic fields from the copyFrom schema
    ManagedIndexSchema copyFromSchema = loadLatestSchema(copyFrom);
    IndexSchema.DynamicField[] dynamicFields = copyFromSchema.getDynamicFields();
    if (dynamicFields.length == 0 && !DEFAULT_CONFIGSET_NAME.equals(copyFrom)) {
      copyFromSchema = loadLatestSchema(DEFAULT_CONFIGSET_NAME);
      dynamicFields = copyFromSchema.getDynamicFields();
    }

    if (dynamicFields.length == 0) {
      return schema;
    }

    final Set<String> existingDFNames =
        Arrays.stream(schema.getDynamicFields()).map(df -> df.getPrototype().getName()).collect(Collectors.toSet());
    List<SchemaField> toAdd = Arrays.stream(dynamicFields)
        .filter(df -> !existingDFNames.contains(df.getPrototype().getName()))
        .map(IndexSchema.DynamicField::getPrototype)
        .collect(Collectors.toList());

    // only restore language specific dynamic fields that match our langSet
    if (!langs.isEmpty()) {
      final Set<String> langSet = new HashSet<>(includeLangIds);
      langSet.addAll(langs);
      toAdd = toAdd.stream()
          .filter(df -> !df.getName().startsWith("*_txt_") || langSet.contains(df.getName().substring("*_txt_".length())))
          .collect(Collectors.toList());
    }

    if (!toAdd.isEmpty()) {
      // grab any field types that need to be re-added
      final Map<String, FieldType> fieldTypes = schema.getFieldTypes();
      List<FieldType> addTypes = toAdd.stream()
          .map(SchemaField::getType)
          .filter(t -> !fieldTypes.containsKey(t.getTypeName()))
          .collect(Collectors.toList());
      if (!addTypes.isEmpty()) {
        schema = schema.addFieldTypes(addTypes, false);
      }

      schema = schema.addDynamicFields(toAdd, null, true);
    }

    return schema;
  }

  void checkSchemaVersion(String configSet, final int versionInRequest, int currentVersion) throws IOException {
    if (versionInRequest < 0) {
      return; // don't enforce the version check
    }

    if (currentVersion == -1) {
      currentVersion = getCurrentSchemaVersion(configSet);
    }

    if (currentVersion != versionInRequest) {
      if (configSet.startsWith(DESIGNER_PREFIX)) {
        configSet = configSet.substring(DESIGNER_PREFIX.length());
      }
      throw new SolrException(SolrException.ErrorCode.CONFLICT,
          "Your schema version " + versionInRequest + " for " + configSet + " is out-of-date; current version is: " + currentVersion +
              ". Perhaps another user also updated the schema while you were editing it? You'll need to retry your update after the schema is refreshed.");
    }
  }

  List<String> listConfigsInZk() throws IOException {
    return configManager.listConfigs();
  }

  byte[] downloadAndZipConfigSet(String configId) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Path tmpDirectory = Files.createTempDirectory("schema-designer-" + FilenameUtils.getName(configId));
    File tmpDir = tmpDirectory.toFile();
    try {
      configManager.downloadConfigDir(configId, tmpDirectory);
      try (ZipOutputStream zipOut = new ZipOutputStream(baos)) {
        zipIt(tmpDir, "", zipOut);
      }
    } finally {
      FileUtils.deleteDirectory(tmpDir);
    }
    return baos.toByteArray();
  }

  protected void zipIt(File f, String fileName, ZipOutputStream zipOut) throws IOException {
    if (f.isHidden()) {
      return;
    }

    if (f.isDirectory()) {
      String dirPrefix = "";
      if (fileName.endsWith("/")) {
        zipOut.putNextEntry(new ZipEntry(fileName));
        zipOut.closeEntry();
        dirPrefix = fileName;
      } else if (!fileName.isEmpty()) {
        dirPrefix = fileName + "/";
        zipOut.putNextEntry(new ZipEntry(dirPrefix));
        zipOut.closeEntry();
      }
      File[] files = f.listFiles();
      if (files != null) {
        for (File child : files) {
          zipIt(child, dirPrefix + child.getName(), zipOut);
        }
      }
      return;
    }

    byte[] bytes = new byte[1024];
    int r;
    try (FileInputStream fis = new FileInputStream(f)) {
      ZipEntry zipEntry = new ZipEntry(fileName);
      zipOut.putNextEntry(zipEntry);
      while ((r = fis.read(bytes)) >= 0) {
        zipOut.write(bytes, 0, r);
      }
    }
  }

  boolean checkConfigExists(String configSet) throws IOException {
    return configManager.configExists(configSet);
  }

  void deleteConfig(String configSet) throws IOException {
    configManager.deleteConfigDir(configSet);
  }

  void copyConfig(String from, String to) throws IOException {
    configManager.copyConfigDir(from, to);
  }
}
