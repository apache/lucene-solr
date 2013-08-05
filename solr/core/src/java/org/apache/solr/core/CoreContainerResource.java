package org.apache.solr.core;


import org.apache.solr.rest.GETable;
import org.apache.solr.rest.POSTable;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;

/**
 *
 *
 **/
public interface CoreContainerResource extends GETable, POSTable {
  @Override
  public Representation get();

  @Override
  public Representation post(Representation entity) throws ResourceException;
}
