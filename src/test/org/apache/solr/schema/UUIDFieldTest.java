package org.apache.solr.schema;

import java.util.UUID;

import junit.framework.TestCase;

import org.apache.solr.common.SolrException;

public class UUIDFieldTest extends TestCase {
  public void testToInternal() {
    boolean ok = false;
    UUIDField uuidfield = new UUIDField();

    try {
      uuidfield.toInternal((String) null);
      ok = true;
    } catch (SolrException se) {
      ok = false;
    }
    assertTrue("ID generation from null failed", ok);

    try {
      uuidfield.toInternal("");
      ok = true;
    } catch (SolrException se) {
      ok = false;
    }
    assertTrue("ID generation from empty string failed", ok);

    try {
      uuidfield.toInternal("NEW");
      ok = true;
    } catch (SolrException se) {
      ok = false;
    }
    assertTrue("ID generation from 'NEW' failed", ok);

    try {
      uuidfield.toInternal("d574fb6a-5f79-4974-b01a-fcd598a19ef5");
      ok = true;
    } catch (SolrException se) {
      ok = false;
    }
    assertTrue("ID generation from UUID failed", ok);

    try {
      uuidfield.toInternal("This is a test");
      ok = false;
    } catch (SolrException se) {
      ok = true;
    }
    assertTrue("Bad UUID check failed", ok);
  }
}
