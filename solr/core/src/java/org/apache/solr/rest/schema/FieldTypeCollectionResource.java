package org.apache.solr.rest.schema;


import org.apache.solr.rest.GETable;
import org.restlet.representation.Representation;

/**
 *
 *
 **/
public interface FieldTypeCollectionResource extends GETable {
  @Override
  Representation get();
}
