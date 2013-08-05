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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.solr.rest.API;
import org.apache.solr.rest.ResourceFinder;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

@Singleton
public class SchemaAPI extends API {
  public static final Logger log = LoggerFactory.getLogger(SchemaAPI.class);
  public static final String FIELDS_PATH = "/{collection}/" + IndexSchema.FIELDS;

  public static final String DYNAMIC_FIELDS = IndexSchema.DYNAMIC_FIELDS.toLowerCase(Locale.ROOT);
  public static final String DYNAMIC_FIELDS_PATH = "/{collection}/" + DYNAMIC_FIELDS;

  public static final String FIELDTYPES = IndexSchema.FIELD_TYPES.toLowerCase(Locale.ROOT);
  public static final String FIELDTYPES_PATH = "/{collection}/" + FIELDTYPES;

  public static final String NAME_PATH = "/{collection}/" + IndexSchema.NAME.toLowerCase(Locale.ROOT);
  public static final String NAME_SEGMENT = "/{collection}/{" + IndexSchema.NAME.toLowerCase(Locale.ROOT) + "}";

  public static final String COPY_FIELDS = IndexSchema.COPY_FIELDS.toLowerCase(Locale.ROOT);
  public static final String COPY_FIELDS_PATH = "/{collection}/" + COPY_FIELDS;

  public static final String VERSION_PATH = "/{collection}/" + IndexSchema.VERSION.toLowerCase(Locale.ROOT);

  public static final String DEFAULT_SEARCH_FIELD = IndexSchema.DEFAULT_SEARCH_FIELD.toLowerCase(Locale.ROOT);
  public static final String DEFAULT_SEARCH_FIELD_PATH = "/{collection}/" + DEFAULT_SEARCH_FIELD;

  public static final String SIMILARITY_PATH = "/{collection}/" + IndexSchema.SIMILARITY.toLowerCase(Locale.ROOT);

  public static final String SOLR_QUERY_PARSER = IndexSchema.SOLR_QUERY_PARSER.toLowerCase(Locale.ROOT);
  public static final String SOLR_QUERY_PARSER_PATH = "/{collection}/" + SOLR_QUERY_PARSER;

  public static final String DEFAULT_OPERATOR = IndexSchema.DEFAULT_OPERATOR.toLowerCase(Locale.ROOT);
  public static final String DEFAULT_OPERATOR_PATH = SOLR_QUERY_PARSER_PATH + "/" + DEFAULT_OPERATOR;

  public static final String UNIQUE_KEY_FIELD = IndexSchema.UNIQUE_KEY.toLowerCase(Locale.ROOT);
  public static final String UNIQUE_KEY_FIELD_PATH = "/{collection}/" + UNIQUE_KEY_FIELD;


  @Inject
  public SchemaAPI(ResourceFinder finder) {
    super(finder);
  }


  @Override
  protected void initAttachments() {
    log.info("initAttachments started");

    attach("", SchemaSR.class);
    // Allow a trailing slash on full-schema requests
    attach("/", SchemaSR.class);

    attach(FIELDS_PATH, FieldCollectionSR.class);
    // Allow a trailing slash on collection requests
    attach(FIELDS_PATH + "/", FieldCollectionSR.class);
    attach(FIELDS_PATH + NAME_SEGMENT, FieldSR.class);

    attach(DYNAMIC_FIELDS_PATH, DynamicFieldCollectionSR.class);
    // Allow a trailing slash on collection requests
    attach(DYNAMIC_FIELDS_PATH + "/", DynamicFieldCollectionSR.class);
    attach(DYNAMIC_FIELDS_PATH + NAME_SEGMENT, DynamicFieldSR.class);

    attach(FIELDTYPES_PATH, FieldTypeCollectionSR.class);
    // Allow a trailing slash on collection requests
    attach(FIELDTYPES_PATH + "/", FieldTypeCollectionSR.class);
    attach(FIELDTYPES_PATH + NAME_SEGMENT, FieldTypeSR.class);

    attach(COPY_FIELDS_PATH, CopyFieldCollectionSR.class);
    // Allow a trailing slash on collection requests
    attach(COPY_FIELDS_PATH + "/", CopyFieldCollectionSR.class);

    attach(NAME_PATH, SchemaNameSR.class);

    attach(VERSION_PATH, SchemaVersionSR.class);

    attach(UNIQUE_KEY_FIELD_PATH, UniqueKeyFieldSR.class);

    attach(DEFAULT_SEARCH_FIELD_PATH, DefaultSearchFieldSR.class);

    attach(SIMILARITY_PATH, SchemaSimilaritySR.class);

    // At present solrQueryParser only contains defaultOperator, but there may be more children in the future
    attach(SOLR_QUERY_PARSER_PATH, SolrQueryParserSR.class);
    attach(DEFAULT_OPERATOR_PATH, SolrQueryParserDefaultOperatorSR.class);

    router.attachDefault(DefaultSchemaSR.class);

    log.info("initAttachments complete");

  }

  @Override
  public String getAPIRoot() {
    return "/schema";
  }

  @Override
  public String getAPIName() {
    return "SCHEMA";
  }


}
