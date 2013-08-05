package org.apache.solr.rest.schema;


import org.apache.solr.rest.GETable;
import org.apache.solr.rest.PUTable;
import org.restlet.representation.Representation;

/**
 *
 *
 **/
public interface FieldResource extends GETable, PUTable {
  @Override
  Representation get();

  /**
   * Accepts JSON add field request, to URL
   */
  @Override
  Representation put(Representation entity);
}
