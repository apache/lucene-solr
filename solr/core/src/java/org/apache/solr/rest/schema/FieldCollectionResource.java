package org.apache.solr.rest.schema;


import org.apache.solr.rest.GETable;
import org.apache.solr.rest.POSTable;
import org.restlet.representation.Representation;

/**
 *
 *
 **/
public interface FieldCollectionResource extends GETable, POSTable {
  @Override
  Representation get();

  @Override
  Representation post(Representation entity);
}
