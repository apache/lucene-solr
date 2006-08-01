package org.apache.solr.util;

import junit.framework.TestCase;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Random;
import java.util.BitSet;

/** Test (some of the) character escaping functions of the XML class
 *  $Id$
 */

public class TestXMLEscaping extends TestCase {
  private void doSimpleTest(String input,String expectedOutput) throws IOException {
    final StringWriter sw = new StringWriter();
    XML.escapeCharData(input, sw);
    final String result = sw.toString();
    assertEquals("Escaped output matches '" + expectedOutput + "'",result,expectedOutput);
  }
  
  public void testNoEscape() throws IOException {
    doSimpleTest("Bonnie","Bonnie");
  }
  
  public void testAmpAscii() throws IOException {
    doSimpleTest("Bonnie & Clyde","Bonnie &amp; Clyde");
  }

  public void testAmpAndTagAscii() throws IOException {
    doSimpleTest("Bonnie & Cl<em>y</em>de","Bonnie &amp; Cl&lt;em&gt;y&lt;/em&gt;de");
  }

  public void testAmpWithAccents() throws IOException {
    // 00e9 is unicode eacute
    doSimpleTest("Les \u00e9v\u00e9nements chez Bonnie & Clyde","Les \u00e9v\u00e9nements chez Bonnie &amp; Clyde");
  }

  public void testAmpDotWithAccents() throws IOException {
    // 00e9 is unicode eacute
    doSimpleTest("Les \u00e9v\u00e9nements chez Bonnie & Clyde.","Les \u00e9v\u00e9nements chez Bonnie &amp; Clyde.");
  }

  public void testAmpAndTagWithAccents() throws IOException {
    // 00e9 is unicode eacute
    doSimpleTest("Les \u00e9v\u00e9nements <chez/> Bonnie & Clyde","Les \u00e9v\u00e9nements &lt;chez/&gt; Bonnie &amp; Clyde");
  }

  public void testGt() throws IOException {
    doSimpleTest("a ]]> b","a ]]&gt; b");
  }
}



