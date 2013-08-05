package org.apache.solr.rest.schema;


import org.apache.solr.rest.GETable;
import org.restlet.representation.Representation;

/**
 *
 *
 **/
public interface DefaultSearchFieldResource extends GETable {
  @Override
  Representation get();
}
