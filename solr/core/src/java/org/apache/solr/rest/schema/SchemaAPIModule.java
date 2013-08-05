package org.apache.solr.rest.schema;


import org.apache.solr.common.SolrException;
import org.apache.solr.rest.APIModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 **/
public class SchemaAPIModule extends APIModule {
  private transient static Logger log = LoggerFactory.getLogger(SchemaAPIModule.class);
  //TODO: dynamically generate this
  private static Class[] resources = {CopyFieldCollectionResource.class,
      CopyFieldCollectionResource.class, DefaultSchemaResource.class, DefaultSearchFieldResource.class,
      DynamicFieldCollectionResource.class, DynamicFieldResource.class, FieldCollectionResource.class,
      FieldResource.class, FieldTypeCollectionResource.class, FieldTypeResource.class,
      SchemaNameResource.class, SchemaResource.class, SchemaSimilarityResource.class,
      SchemaVersionResource.class, SolrQueryParserDefaultOperatorResource.class,
      SolrQueryParserResource.class, UniqueKeyFieldResource.class
  };

  @Override
  protected void defineBindings() {
    for (Class resource : resources) {
      String implName = resource.getName().replace("Resource", "SR");
      Class impl = null;
      try {
        impl = Class.forName(implName);//TODO: use SolrResourceLoader?
      } catch (ClassNotFoundException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to find: " + implName + " for SchemaAPI binding");
      }
      if (impl != null) {
        bind(resource).to(impl);
      } else {
        log.error("Unable to bind " + implName + " to resource: " + resource.getName());
      }
    }

  }
}
