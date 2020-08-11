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

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This Factory generates an UpdateProcessor that helps to enforce Version 
 * constraints on documents based on per-document version numbers using a configured 
 * <code>versionField</code>, a comma-delimited list of fields to check for version
 * numbers.  It should be configured on the "default"
 * update processor somewhere before the DistributedUpdateProcessorFactory.
 * As an example, see the solrconfig.xml that the tests use:
 * solr/core/src/test-files/solr/collection1/conf/solrconfig-externalversionconstraint.xml
 * </p>
 * <p>
 * When documents are added through this processor, if a document with the same
 * unique key already exists in the collection, then the values within the fields
 * as specified by the comma-delimited <code>versionField</code> property are checked,
 * and if in the <i>existing</i> document the values for all fields are not less than the
 * field values in the <i>new</i> document, then the new document is rejected with a
 * 409 Version Conflict error.
 * </p>
 * <p>
 * In addition to the mandatory <code>versionField</code> init param, two additional
 * optional init params affect the behavior of this factory:
 * </p>
 * <ul>
 *   <li><code>deleteVersionParam</code> - This string parameter controls whether this
 *     processor will intercept and inspect Delete By Id commands in addition to adding
 *     documents.  If specified, then the value will specify the name(s) of the request
 *     parameter(s) which becomes  mandatory for all Delete By Id commands. Like
 *     <code>versionField</code>, <code>deleteVersionParam</code> is comma-delimited.
 *     For each of the params given, it specifies the document version associated with
 *     the delete, where the index matches <code>versionField</code>. For example, if
 *     <code>versionField</code> was set to 'a,b' and <code>deleteVersionParam</code>
 *     was set to 'p1,p2', p1 should give the version for field 'a' and p2 should give
 *     the version for field 'b'. If the versions specified using these params are not
 *     greater then the value in the <code>versionField</code> for any existing document,
 *     then the delete will fail with a 409 Version Conflict error.  When using this
 *     param, Any Delete By Id command with a high enough document version number to
 *     succeed will be internally converted into an Add Document command that replaces
 *     the existing document with a new one which is empty except for the Unique Key
 *     and fields corresponding to the fields listed in <code>versionField</code>
 *     to keeping a record of the deleted version so future Add Document commands will
 *     fail if their "new" version is not high enough.</li>
 *
 *   <li><code>ignoreOldUpdates</code> - This boolean parameter defaults to 
 *     <code>false</code>, but if set to <code>true</code> causes any update with a 
 *     document version that is not great enough to be silently ignored (and return 
 *     a status 200 to the client) instead of generating a 409 Version Conflict error.
 *   </li>
 *
 *   <li><code>supportMissingVersionOnOldDocs</code> - This boolean parameter defaults to
 *     <code>false</code>, but if set to <code>true</code> allows any documents written *before*
 *     this feature is enabled and which are missing the versionField to be overwritten.
 *   </li>
 *   <li><code>tombstoneConfig</code> - a list of field names to values to add to the
 *   created tombstone document. In general is not a good idea to populate tombsone documents
 *   with anything other than the minimum required fields so that it doean't match queries</li>
 * </ul>
 * @since 4.6.0
 */
public class DocBasedVersionConstraintsProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean ignoreOldUpdates = false;
  private List<String> versionFields = null;
  private List<String> deleteVersionParamNames = Collections.emptyList();
  private boolean useFieldCache;
  private boolean supportMissingVersionOnOldDocs = false;
  private NamedList<Object> tombstoneConfig;

  @SuppressWarnings("unchecked")
  @Override
  public void init( @SuppressWarnings({"rawtypes"})NamedList args )  {

    Object tmp = args.remove("versionField");
    if (null == tmp) {
      throw new SolrException(SERVER_ERROR,
          "'versionField' must be configured");
    }
    if (! (tmp instanceof String) ) {
      throw new SolrException(SERVER_ERROR,
          "'versionField' must be configured as a <str>");
    }
    versionFields = StrUtils.splitSmart((String)tmp, ',');

    // optional
    tmp = args.remove("deleteVersionParam");
    if (null != tmp) {
      if (! (tmp instanceof String) ) {
        throw new SolrException(SERVER_ERROR,
            "'deleteVersionParam' must be configured as a <str>");
      }
      deleteVersionParamNames = StrUtils.splitSmart((String)tmp, ',');
    }

    if (deleteVersionParamNames.size() > 0 && deleteVersionParamNames.size() != versionFields.size()) {
      throw new SolrException(SERVER_ERROR, "The number of 'deleteVersionParam' params " +
          "must either be 0 or equal to the number of 'versionField' fields");
    }

    // optional - defaults to false
    tmp = args.remove("ignoreOldUpdates");
    if (null != tmp) {
      if (! (tmp instanceof Boolean) ) {
        throw new SolrException(SERVER_ERROR, 
                                "'ignoreOldUpdates' must be configured as a <bool>");
      }
      ignoreOldUpdates = (Boolean) tmp;
    }

    // optional - defaults to false
    tmp = args.remove("supportMissingVersionOnOldDocs");
    if (null != tmp) {
      if (! (tmp instanceof Boolean) ) {
        throw new SolrException(SERVER_ERROR,
                "'supportMissingVersionOnOldDocs' must be configured as a <bool>");
      }
      supportMissingVersionOnOldDocs = ((Boolean)tmp).booleanValue();
    }
    
    tmp = args.remove("tombstoneConfig");
    if (null != tmp) {
      if (! (tmp instanceof NamedList) ) {
        throw new SolrException(SERVER_ERROR,
                "'tombstoneConfig' must be configured as a <lst>.");
      }
      tombstoneConfig = (NamedList<Object>)tmp;
    }

    super.init(args);
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, 
                                            SolrQueryResponse rsp, 
                                            UpdateRequestProcessor next ) {
    return new DocBasedVersionConstraintsProcessor(versionFields,
                                                   ignoreOldUpdates,
                                                   deleteVersionParamNames,
                                                   supportMissingVersionOnOldDocs,
                                                   useFieldCache,
                                                   tombstoneConfig,
                                                   req, next);
  }

  @Override
  public void inform(SolrCore core) {

    if (core.getUpdateHandler().getUpdateLog() == null) {
      throw new SolrException(SERVER_ERROR,
          "updateLog must be enabled.");
    }

    if (core.getLatestSchema().getUniqueKeyField() == null) {
      throw new SolrException(SERVER_ERROR,
          "schema must have uniqueKey defined.");
    }

    useFieldCache = true;
    for (String versionField : versionFields) {
      SchemaField userVersionField = core.getLatestSchema().getField(versionField);
      if (userVersionField == null || !userVersionField.stored() || userVersionField.multiValued()) {
        throw new SolrException(SERVER_ERROR,
            "field " + versionField + " must be defined in schema, be stored, and be single valued.");
      }
      if (useFieldCache) {
        try {
          userVersionField.getType().getValueSource(userVersionField, null);
        } catch (Exception e) {
          useFieldCache = false;
          log.warn("Can't use fieldcache/valuesource: ", e);
        }
      }
    }
    
    canCreateTombstoneDocument(core.getLatestSchema());
  }
  
  /**
   * Validates that the schema would allow tombstones to be created by DocBasedVersionConstraintsProcessor by
   * checking if the required fields are of known types
   */
  protected boolean canCreateTombstoneDocument(IndexSchema schema) {
    Set<String> requiredFieldNames = schema.getRequiredFields().stream()
        .filter(field -> field.getDefaultValue() == null)
        .map(field -> field.getName())
        .collect(Collectors.toSet());
    if (tombstoneConfig != null) {
      tombstoneConfig.forEach((k,v) -> requiredFieldNames.remove(k));
    }
    requiredFieldNames.remove(schema.getUniqueKeyField().getName());
    if (versionFields != null) {
      versionFields.forEach(field -> requiredFieldNames.remove(field));
    }
    if (!requiredFieldNames.isEmpty()) {
      log.warn("The schema '{}' has required fields that aren't added in the tombstone.  This can cause deletes to fail if those aren't being added in some other way. Required Fields={}",
          schema.getSchemaName(),
          requiredFieldNames);
      return false;
    }
    return true;
  }

}
