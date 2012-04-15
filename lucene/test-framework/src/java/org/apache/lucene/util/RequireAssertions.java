package org.apache.lucene.util;

import com.carrotsearch.randomizedtesting.ClassValidator;

/**
 * Require assertions for Lucene/Solr packages.
 */
public class RequireAssertions implements ClassValidator {
  @Override
  public void validate(Class<?> clazz) throws Throwable {
    try {
      assert false;
      throw new RuntimeException("Enable assertions globally (-ea) or for Solr/Lucene subpackages only.");
    } catch (AssertionError e) {
      // Ok, enabled.
    }    
  }
}
