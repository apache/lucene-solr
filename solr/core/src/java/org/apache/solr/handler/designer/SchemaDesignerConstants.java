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

public interface SchemaDesignerConstants {
  String CONFIG_SET_PARAM = "configSet";
  String COPY_FROM_PARAM = "copyFrom";
  String SCHEMA_VERSION_PARAM = "schemaVersion";
  String RELOAD_COLLECTIONS_PARAM = "reloadCollections";
  String INDEX_TO_COLLECTION_PARAM = "indexToCollection";
  String NEW_COLLECTION_PARAM = "newCollection";
  String CLEANUP_TEMP_PARAM = "cleanupTemp";
  String ENABLE_DYNAMIC_FIELDS_PARAM = "enableDynamicFields";
  String ENABLE_FIELD_GUESSING_PARAM = "enableFieldGuessing";
  String ENABLE_NESTED_DOCS_PARAM = "enableNestedDocs";
  String TEMP_COLLECTION_PARAM = "tempCollection";
  String PUBLISHED_VERSION = "publishedVersion";
  String DISABLE_DESIGNER_PARAM = "disableDesigner";
  String DISABLED = "disabled";
  String DOC_ID_PARAM = "docId";
  String FIELD_PARAM = "field";
  String UNIQUE_KEY_FIELD_PARAM = "uniqueKeyField";
  String AUTO_CREATE_FIELDS = "update.autoCreateFields";
  String SOLR_CONFIG_XML = "solrconfig.xml";
  String DESIGNER_KEY = "_designer.";
  String LANGUAGES_PARAM = "languages";
  String CONFIGOVERLAY_JSON = "configoverlay.json";
  String BLOB_STORE_ID = ".system";
  String UPDATE_ERROR = "updateError";
  String ANALYSIS_ERROR = "analysisError";
  String ERROR_DETAILS = "errorDetails";

  String DESIGNER_PREFIX = "._designer_";
  int MAX_SAMPLE_DOCS = 1000;
}
