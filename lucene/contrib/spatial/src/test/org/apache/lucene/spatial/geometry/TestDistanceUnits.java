package org.apache.lucene.spatial.geometry;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/**
 * Tests for {@link org.apache.lucene.spatial.geometry.DistanceUnits}
 */
public class TestDistanceUnits extends LuceneTestCase {

  /**
   * Pass condition: When finding the DistanceUnit for "km", KILOMETRES is found.  When finding the DistanceUnit for
   * "miles", MILES is found.
   */
  @Test
  public void testFindDistanceUnit() {
    assertEquals(DistanceUnits.KILOMETERS, DistanceUnits.findDistanceUnit("km"));
    assertEquals(DistanceUnits.MILES, DistanceUnits.findDistanceUnit("miles"));
  }

  /**
   * Pass condition: Searching for the DistanceUnit of an unknown unit "mls" should throw an IllegalArgumentException.
   */
  @Test
  public void testFindDistanceUnit_unknownUnit() {
    try {
      DistanceUnits.findDistanceUnit("mls");
      assertTrue("IllegalArgumentException should have been thrown", false);
    } catch (IllegalArgumentException iae) {
      // Expected
    }
  }

  /**
   * Pass condition: Converting between the same units should not change the value.  Converting from MILES to KILOMETRES
   * involves multiplying the distance by the ratio, and converting from KILOMETRES to MILES involves dividing by the ratio
   */
  @Test
  public void testConvert() {
    assertEquals(10.5, DistanceUnits.MILES.convert(10.5, DistanceUnits.MILES), 0D);
    assertEquals(10.5, DistanceUnits.KILOMETERS.convert(10.5, DistanceUnits.KILOMETERS), 0D);
    assertEquals(10.5 * 1.609344, DistanceUnits.KILOMETERS.convert(10.5, DistanceUnits.MILES), 0D);
    assertEquals(10.5 / 1.609344, DistanceUnits.MILES.convert(10.5, DistanceUnits.KILOMETERS), 0D);
  }
}
