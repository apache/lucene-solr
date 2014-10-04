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

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.restlet.resource.ResourceException;

import java.util.LinkedHashSet;
import java.util.Map;


/**
 * Base class for Schema Field and DynamicField requests.
 */
abstract class BaseFieldResource extends BaseSolrResource {
  protected static final String INCLUDE_DYNAMIC_PARAM = "includeDynamic";
  private static final String DYNAMIC_BASE = "dynamicBase";

  private LinkedHashSet<String> requestedFields;
  private boolean showDefaults;

  protected LinkedHashSet<String> getRequestedFields() {
    return requestedFields; 
  }
  

  protected BaseFieldResource() {
    super();
  }

  /**
   * Pulls the "fl" param from the request and splits it to get the
   * requested list of fields.  The (Dynamic)FieldCollectionResource classes
   * will then restrict the fields sent back in the response to those
   * on this list.  The (Dynamic)FieldResource classes ignore this list, 
   * since the (dynamic) field is specified in the URL path, rather than
   * in a query parameter.
   * <p/>
   * Also pulls the "showDefaults" param from the request, for use by all
   * subclasses to include default values from the associated field type
   * in the response.  By default this param is off.
   */
  @Override
  public void doInit() throws ResourceException {
    super.doInit();
    if (isExisting()) {
      String flParam = getSolrRequest().getParams().get(CommonParams.FL);
      if (null != flParam) {
        String[] fields = flParam.trim().split("[,\\s]+");
        if (fields.length > 0) {
          requestedFields = new LinkedHashSet<>();
          for (String field : fields) {
            if ( ! field.trim().isEmpty()) {
              requestedFields.add(field.trim());
            }
          }
        }
      }
      showDefaults = getSolrRequest().getParams().getBool(SHOW_DEFAULTS, false);
    }
  }

  /** Get the properties for a given field.
   *
   * @param field not required to exist in the schema
   */
  protected SimpleOrderedMap<Object> getFieldProperties(SchemaField field) {
    if (null == field) {
      return null;
    }
    SimpleOrderedMap<Object> properties = field.getNamedPropertyValues(showDefaults);
    if ( ! getSchema().getFields().containsKey(field.getName())) {
      String dynamicBase = getSchema().getDynamicPattern(field.getName());
      // Add dynamicBase property if it's different from the field name. 
      if ( ! field.getName().equals(dynamicBase)) {
        properties.add(DYNAMIC_BASE, dynamicBase);
      }
    }
    if (field == getSchema().getUniqueKeyField()) {
      properties.add(IndexSchema.UNIQUE_KEY, true);
    }
    return properties;
  }

  /**
   * When running in cloud mode, waits for a schema update to be
   * applied by all active replicas of the current collection.
   */
  protected void waitForSchemaUpdateToPropagate(IndexSchema newSchema) {
    // If using ZooKeeper and the client application has requested an update timeout, then block until all
    // active replicas for this collection process the updated schema
    if (getUpdateTimeoutSecs() > 0 && newSchema != null &&
        newSchema.getResourceLoader() instanceof ZkSolrResourceLoader)
    {
      CoreDescriptor cd = getSolrCore().getCoreDescriptor();
      String collection = cd.getCollectionName();
      if (collection != null) {
        ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) newSchema.getResourceLoader();
        ManagedIndexSchema.waitForSchemaZkVersionAgreement(collection,
            cd.getCloudDescriptor().getCoreNodeName(),
            ((ManagedIndexSchema) newSchema).getSchemaZkVersion(),
            zkLoader.getZkController(),
            getUpdateTimeoutSecs());
      }
    }
  }

  // protected access on this class triggers a bug in javadoc generation caught by
  // documentation-link: "BROKEN LINK" reported in javadoc for classes using
  // NewFieldArguments because the link target file is BaseFieldResource.NewFieldArguments,
  // but the actual file is BaseFieldResource$NewFieldArguments.
  static class NewFieldArguments {
    private String name;
    private String type;
    Map<String,Object> map;
    NewFieldArguments(String name, String type, Map<String,Object> map) {
      this.name = name;
      this.type = type;
      this.map = map;
    }

    public String getName() { return name; }
    public String getType() { return type; }
    public Map<String, Object> getMap() { return map; }
  }
}
