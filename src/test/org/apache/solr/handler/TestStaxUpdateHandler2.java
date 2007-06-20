package org.apache.solr.handler;

import org.apache.solr.ConvertedLegacyTest;

/**
 * Temporary test to duplicate behavior for the StaxUpdateRequestHandler
 * 
 * When the XmlUpdateRequestHandler is replaced, this should go away
 */
public class TestStaxUpdateHandler2 extends ConvertedLegacyTest 
{
  @Override
  public void setUp() throws Exception {
    super.setUp();

    h.updater = new StaxUpdateRequestHandler();
    h.updater.init( null );
  }
}
