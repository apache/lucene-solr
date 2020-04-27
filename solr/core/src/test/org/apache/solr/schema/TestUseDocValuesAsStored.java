/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.schema;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.apache.solr.util.DOMUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Tests the useDocValuesAsStored functionality.
 */
public class TestUseDocValuesAsStored extends AbstractBadConfigTestBase {

  private int id = 1;

  private static File tmpSolrHome;
  private static File tmpConfDir;
  
  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  private static final long START_RANDOM_EPOCH_MILLIS;
  private static final long END_RANDOM_EPOCH_MILLIS;
  private static final String[] SEVERITY;

  // http://www.w3.org/TR/2006/REC-xml-20060816/#charsets
  private static final String LOW_NON_XML_CHARS = "\u0000-\u0008\u000B-\u000C\u000E-\u001F";
  private static final String HIGH_NON_XML_CHARS = "\uFFFE\uFFFF";
  // Avoid single quotes (problematic in XPath literals) and carriage returns (XML roundtripping fails)
  private static final Pattern BAD_CHAR_PATTERN = Pattern.compile("[\'\r" + LOW_NON_XML_CHARS + "]|([" + HIGH_NON_XML_CHARS + "])");
  private static final Pattern STORED_FIELD_NAME_PATTERN = Pattern.compile("_dv$");

  static {
    START_RANDOM_EPOCH_MILLIS = LocalDateTime.of(-11000, Month.JANUARY, 1, 0, 0)// BC
        .toInstant(ZoneOffset.UTC).toEpochMilli();
    END_RANDOM_EPOCH_MILLIS = LocalDateTime.of(11000, Month.DECEMBER, 31, 23, 59, 59, 999_000_000) // AD, 5 digit year
        .toInstant(ZoneOffset.UTC).toEpochMilli();
    try {
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      InputStream stream = TestUseDocValuesAsStored.class.getResourceAsStream("/solr/collection1/conf/enumsConfig.xml");
      Document doc = builder.parse(new InputSource(IOUtils.getDecodingReader(stream, StandardCharsets.UTF_8)));
      XPath xpath = XPathFactory.newInstance().newXPath();
      NodeList nodes = (NodeList)xpath.evaluate
          ("/enumsConfig/enum[@name='severity']/value", doc, XPathConstants.NODESET);
      SEVERITY = new String[nodes.getLength()];
      for (int i = 0 ; i < nodes.getLength() ; ++i) {
        SEVERITY[i] = DOMUtil.getText(nodes.item(i));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  private void initManagedSchemaCore() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(TEST_HOME(), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig-managed-schema.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig.snippet.randomindexconfig.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "enumsConfig.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-non-stored-docvalues.xml"), tmpConfDir);

    // initCore will trigger an upgrade to managed schema, since the solrconfig has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    System.setProperty("enable.update.log", "false");
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-non-stored-docvalues.xml", tmpSolrHome.getPath());

    assertQ("sanity check", req("q", "*:*"), "//*[@numFound='0']");
  }

  @After
  private void afterTest() throws Exception {
    clearIndex();
    deleteCore();
    System.clearProperty("managed.schema.mutable");
    System.clearProperty("enable.update.log");
  }

  public String getCoreName() {
    return "basic";
  }

  @Test
  public void testOnEmptyIndex() throws Exception {
    clearIndex();
    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "*"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "test_nonstored_dv_str"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "*,test_nonstored_dv_str"), "/response/numFound==0");

    assertU(adoc("id", "xyz", "test_nonstored_dv_str", "xyz"));
    assertJQ(req("q", "*:*"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "*"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "test_nonstored_dv_str"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "*,test_nonstored_dv_str"), "/response/numFound==0");

    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==1",
        "/response/docs==[" +
            "{'id':'xyz','test_nonstored_dv_str':'xyz'}"
            + "]");
    assertJQ(req("q", "*:*", "fl", "*"), "/response/numFound==1",
        "/response/docs==[" +
            "{'id':'xyz','test_nonstored_dv_str':'xyz'}"
            + "]");
    assertJQ(req("q", "*:*", "fl", "test_nonstored_dv_str"), "/response/numFound==1",
        "/response/docs==[" +
            "{'test_nonstored_dv_str':'xyz'}"
            + "]");
    assertJQ(req("q", "*:*", "fl", "*,test_nonstored_dv_str"), "/response/numFound==1",
        "/response/docs==[" +
            "{'id':'xyz','test_nonstored_dv_str':'xyz'}"
            + "]");

    assertU(adoc("id", "xyz"));
    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==1",
        "/response/docs==[" +
            "{'id':'xyz'}"
            + "]");
  }
  
  @Test
  public void testDuplicateMultiValued() throws Exception {
    doTest("strTF", dvStringFieldName(3,true,false), "str", "X", "X", "Y");
    doTest("strTT", dvStringFieldName(3,true,true), "str", "X", "X", "Y");
    doTest("strFF", dvStringFieldName(3,false,false), "str", "X", "X", "Y");
    doTest("int", "test_is_dvo", "int", "42", "42", "-666");
    doTest("float", "test_fs_dvo", "float", "4.2", "4.2", "-66.666");
    doTest("long", "test_ls_dvo", "long", "420", "420", "-6666666" );
    doTest("double", "test_ds_dvo", "double", "0.0042", "0.0042", "-6.6666E-5");
    doTest("date", "test_dts_dvo", "date", "2016-07-04T03:02:01Z", "2016-07-04T03:02:01Z", "1999-12-31T23:59:59Z" );
    doTest("enum", "enums_dvo", "str", SEVERITY[0], SEVERITY[0], SEVERITY[1]);
  }

  @Test
  public void testRandomSingleAndMultiValued() throws Exception {
    for (int c = 0 ; c < 10 * RANDOM_MULTIPLIER ; ++c) {
      clearIndex();
      int[] arity = new int[9];
      for (int a = 0 ; a < arity.length ; ++a) {
        // Single-valued 50% of the time; other 50%: 2-10 values equally likely
        arity[a] = random().nextBoolean() ? 1 : TestUtil.nextInt(random(), 2, 10);
      }
      doTest("check string value is correct", dvStringFieldName(arity[0], true, false), "str", nextValues(arity[0], "str"));
      doTest("check int value is correct", "test_i" + plural(arity[1]) + "_dvo", "int", nextValues(arity[1], "int"));
      doTest("check double value is correct", "test_d" + plural(arity[2]) + "_dvo", "double", nextValues(arity[2], "double"));
      doTest("check long value is correct", "test_l" + plural(arity[3]) + "_dvo", "long", nextValues(arity[3], "long"));
      doTest("check float value is correct", "test_f" + plural(arity[4]) + "_dvo", "float", nextValues(arity[4], "float"));
      doTest("check date value is correct", "test_dt" + plural(arity[5]) + "_dvo", "date", nextValues(arity[5], "date"));
      doTest("check stored and docValues value is correct", dvStringFieldName(arity[6], true, true), "str", nextValues(arity[6], "str"));
      doTest("check non-stored and non-indexed is accessible", dvStringFieldName(arity[7], false, false), "str", nextValues(arity[7], "str"));
      doTest("enumField", "enum" + plural(arity[8]) + "_dvo", "str", nextValues(arity[8], "enum"));

      // binary dv string types are useful in particular for large values (exceeding the limit of 32766 UTF8 bytes for StrField), so test these.
      // The maxLength specified below is the number of *codepoints*, some (or even all) of which may resolve to surrogate pairs, so the range
      // of returned values will include values whose UTF8 encoding exceeds the 32766 StrField byte limit. Multi-valued fields not supported.
      BinaryStringType testType = BINARY_STRING_TYPES[random().nextInt(BINARY_STRING_TYPES.length - 1)];
      doTest("check non-stored and non-indexed binary is accessible", dvStringFieldName(1, false, false, testType), "str", nextValues(1, "str", 32766));
    }
  }

  @Test
  public void testExcessiveBinaryLength() throws Exception {
      clearIndex();
      doTest("check binary maxLength limit", dvStringFieldName(1, false, false, BinaryStringType.UTF8), "str", nextValues(1, "str", ~196606));
      try {
        doTest("check binary maxLength limit enforced", dvStringFieldName(1, false, false, BinaryStringType.UTF8), "str", nextValues(1, "str", ~196607));
        assertFalse(true);
      } catch (SolrException ex) {
        assertTrue(ex.getCause().getCause().getMessage().contains("length of field (196607) exceeds effective maxLength of 196606"));
      }
  }

  private String plural(int arity) {
    return arity > 1 ? "s" : "";
  }

  private static boolean isStoredField(String fieldName) {
    return STORED_FIELD_NAME_PATTERN.matcher(fieldName).find();
  }

  private static enum BinaryStringType { UTF8, NONE }
  private static final BinaryStringType[] BINARY_STRING_TYPES = BinaryStringType.values();

  private String dvStringFieldName(int arity, boolean indexed, boolean stored) {
    return dvStringFieldName(arity, indexed, stored, BinaryStringType.NONE);
  }
  private String dvStringFieldName(int arity, boolean indexed, boolean stored, BinaryStringType binaryType) {
    String base = "test_s" + (arity > 1 ? "s": "");
    String suffix = "";
    if (indexed && stored) suffix = "_dv";
    else if (indexed && ! stored) suffix = "_dvo";
    else if ( ! indexed && ! stored) {
      switch (binaryType) {
        case NONE:
          suffix = "_dvo2";
          break;
        case UTF8:
          suffix = "_dvob";
          break;
      }
    } else assertTrue("unsupported dv string field combination: stored and not indexed", false);
    return base + suffix;
  }

  private String[] nextValues(int arity, String valueType) throws Exception {
    return nextValues(arity, valueType, 0);
  }

  private String[] nextValues(int arity, String valueType, int maxLength) throws Exception {
    String[] values = new String[arity];
    for (int i = 0 ; i < arity ; ++i) {
      switch (valueType) {
        case "int": values[i] = String.valueOf(random().nextInt()); break;
        case "double": values[i] = String.valueOf(Double.longBitsToDouble(random().nextLong())); break;
        case "long": values[i] = String.valueOf(random().nextLong()); break;
        case "float": values[i] = String.valueOf(Float.intBitsToFloat(random().nextInt())); break;
        case "enum": values[i] = SEVERITY[TestUtil.nextInt(random(), 0, SEVERITY.length - 1)]; break;
        case "str": {
          final String str;
          if (maxLength < 0) {
            final String tmp = TestUtil.randomFixedByteLengthUnicodeString(random(), ~maxLength);
            str = replaceWithStableUnicodeLength(tmp);
          } else {
            final String tmp;
            if (maxLength == 0) {
              tmp = TestUtil.randomRealisticUnicodeString(random());
            } else {
              tmp = TestUtil.randomRealisticUnicodeString(random(), maxLength);
            }
            str = BAD_CHAR_PATTERN.matcher(tmp).replaceAll("\uFFFD");
          }
          values[i] = str;
          break;
        }
        case "date": {
          long epochMillis = TestUtil.nextLong(random(), START_RANDOM_EPOCH_MILLIS, END_RANDOM_EPOCH_MILLIS);
          values[i] = Instant.ofEpochMilli(epochMillis).toString();
          break;
        }
        default: throw new Exception("unknown type '" + valueType + "'");
      }
    }
    return values;
  }

  private static String replaceWithStableUnicodeLength(String input) {
    final Matcher m = BAD_CHAR_PATTERN.matcher(input);
    if (!m.find()) {
      return input;
    } else {
      final StringBuilder sb = new StringBuilder(input.length());
      do {
        m.appendReplacement(sb, m.group(1) == null ? "?" : "\uFFFD");
      } while (m.find());
      m.appendTail(sb);
      return sb.toString();
    }
  }

  @Test
  public void testMultipleSearchResults() throws Exception {
    // Three documents with different numbers of values for a field
    assertU(adoc("id", "myid1", "test_is_dvo", "101", "test_is_dvo", "102", "test_is_dvo", "103"));
    assertU(adoc("id", "myid2", "test_is_dvo", "201", "test_is_dvo", "202"));
    assertU(adoc("id", "myid3", "test_is_dvo", "301", "test_is_dvo", "302",
        "test_is_dvo", "303", "test_is_dvo", "304"));

    // Multivalued and singly valued fields in the same document
    assertU(adoc("id", "myid4", "test_s_dvo", "hello", "test_is_dvo", "401", "test_is_dvo", "402"));

    // Test a field which has useDocValuesAsStored=false
    assertU(adoc("id", "myid5", "nonstored_dv_str", "dont see me"));
    assertU(adoc("id", "myid6", "nonstored_dv_str", "dont see me", "test_s_dvo", "hello"));
    assertU(commit());

    assertJQ(req("q", "id:myid*", "fl", "*"),
        "/response/docs==["
            + "{'id':'myid1','test_is_dvo':[101,102,103]},"
            + "{'id':'myid2','test_is_dvo':[201,202]},"
            + "{'id':'myid3','test_is_dvo':[301,302,303,304]},"
            + "{'id':'myid4','test_s_dvo':'hello','test_is_dvo':[401,402]},"
            + "{'id':'myid5'},"
            + "{'id':'myid6','test_s_dvo':'hello'}"
            + "]");
  }
  
  @Test
  public void testUseDocValuesAsStoredFalse() throws Exception {
    SchemaField sf = h.getCore().getLatestSchema().getField("nonstored_dv_str");
    assertNotNull(sf);
    assertTrue(sf.hasDocValues());
    assertFalse(sf.useDocValuesAsStored());
    assertFalse(sf.stored());
    assertU(adoc("id", "myid", "nonstored_dv_str", "dont see me"));
    assertU(commit());
    
    assertJQ(req("q", "id:myid"),
        "/response/docs==["
            + "{'id':'myid'}"
            + "]");
    assertJQ(req("q", "id:myid", "fl", "*"),
        "/response/docs==["
            + "{'id':'myid'}"
            + "]");
    assertJQ(req("q", "id:myid", "fl", "id,nonstored_dv_*"),
        "/response/docs==["
            + "{'id':'myid'}"
            + "]");
    assertJQ(req("q", "id:myid", "fl", "id,nonstored_dv_str"),
        "/response/docs==["
            + "{'id':'myid','nonstored_dv_str':'dont see me'}"
            + "]");
  }

  public void testManagedSchema() throws Exception {
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    StrField type = new StrField();
    type.setTypeName("str");
    SchemaField falseDVASField = new SchemaField("false_dvas", type, 
        SchemaField.INDEXED | SchemaField.DOC_VALUES, null);
    SchemaField trueDVASField = new SchemaField("true_dvas", type, 
        SchemaField.INDEXED | SchemaField.DOC_VALUES | SchemaField.USE_DOCVALUES_AS_STORED, null);
    IndexSchema newSchema = oldSchema.addField(falseDVASField).addField(trueDVASField);
    h.getCore().setLatestSchema(newSchema);

    clearIndex();
    assertU(adoc("id", "myid1", "false_dvas", "101", "true_dvas", "102"));
    assertU(commit());

    assertJQ(req("q", "id:myid*", "fl", "*"),
        "/response/docs==["
            + "{'id':'myid1', 'true_dvas':'102'}]");
  }

  private void doTest(String desc, String field, String type, String... value) {
    String id = "" + this.id++;


    String[] xpaths = new String[value.length + 1];

    if (value.length > 1) {
      Set<String> valueSet = new HashSet<>();
      valueSet.addAll(Arrays.asList(value));
      String[] fieldAndValues = new String[value.length * 2 + 2];
      fieldAndValues[0] = "id";
      fieldAndValues[1] = id;

      for (int i = 0; i < value.length; ++i) {
        fieldAndValues[i * 2 + 2] = field;
        fieldAndValues[i * 2 + 3] = value[i];
        xpaths[i] = "//arr[@name='" + field + "']/" + type + "[.='" + value[i] + "']";
      }

      // See SOLR-10924...
      // Trie/String based Docvalues are sets, but stored values & Point DVs are ordered multisets,
      // so cardinality depends on the value source
      final int expectedCardinality =
        (isStoredField(field) || (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)
                                  && ! field.startsWith("test_s")))
        ? value.length : valueSet.size();
      xpaths[value.length] = "*[count(//arr[@name='"+field+"']/"+type+")="+expectedCardinality+"]";
      assertU(adoc(fieldAndValues));

    } else {
      assertU(adoc("id", id, field, value[0]));
      xpaths[0] = "//" + type + "[@name='" + field + "'][.='" + value[0] + "']";
      xpaths[1] = "*[count(//" + type + "[@name='" + field + "']) = 1]";
    }

    assertU(commit());

    String fl = field;
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = field + ",*";
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = "*" + field.substring(field.length() - 3);
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = "*";
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = field + ",fakeFieldName";
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = "*";
    assertQ(desc + ": " + fl, req("q", "*:*", "fl", fl), xpaths);

  }
  
  // See SOLR-8740 for a discussion. This test is here to make sure we consciously change behavior of multiValued
  // fields given that we can now return docValues fields. The behavior we've guaranteed in the past is that if
  // multiValued fields are stored, they're returned in the document in the order they were added.
  // There are four new fieldTypes added:
  // <field name="test_mvt_dvt_st_str" type="string" indexed="true" multiValued="true" docValues="true"  stored="true"/>
  // <field name="test_mvt_dvt_sf_str" type="string" indexed="true" multiValued="true" docValues="true"  stored="false"/>
  // <field name="test_mvt_dvf_st_str" type="string" indexed="true" multiValued="true" docValues="false" stored="true"/>
  // <field name="test_mvt_dvu_st_str" type="string" indexed="true" multiValued="true"                   stored="true"/>
  //
  // If any of these tests break as a result of returning DocValues rather than stored values, make sure we reach some
  // consensus that any breaks on back-compat are A Good Thing and that that behavior is carefully documented!

  @Test
  public void testMultivaluedOrdering() throws Exception {
    clearIndex();
    
    // multiValued=true, docValues=true, stored=true. Should return in original order
    assertU(adoc("id", "1", "test_mvt_dvt_st_str", "cccc", "test_mvt_dvt_st_str", "aaaa", "test_mvt_dvt_st_str", "bbbb"));
    
    // multiValued=true, docValues=true, stored=false. Should return in sorted order
    assertU(adoc("id", "2", "test_mvt_dvt_sf_str", "cccc", "test_mvt_dvt_sf_str", "aaaa", "test_mvt_dvt_sf_str", "bbbb"));
    
    // multiValued=true, docValues=false, stored=true. Should return in original order
    assertU(adoc("id", "3", "test_mvt_dvf_st_str", "cccc", "test_mvt_dvf_st_str", "aaaa", "test_mvt_dvf_st_str", "bbbb"));
    
    // multiValued=true, docValues=not specified, stored=true. Should return in original order
    assertU(adoc("id", "4", "test_mvt_dvu_st_str", "cccc", "test_mvt_dvu_st_str", "aaaa", "test_mvt_dvu_st_str", "bbbb"));
    
    assertU(commit());
    
    assertJQ(req("q", "id:1", "fl", "test_mvt_dvt_st_str"), 
        "/response/docs/[0]/test_mvt_dvt_st_str/[0]==cccc",
        "/response/docs/[0]/test_mvt_dvt_st_str/[1]==aaaa",
        "/response/docs/[0]/test_mvt_dvt_st_str/[2]==bbbb");

    // Currently, this test fails since stored=false. When SOLR-8740 is committed, it should not throw an exception
    // and should succeed, returning the field in sorted order.
    try {
      assertJQ(req("q", "id:2", "fl", "test_mvt_dvt_sf_str"),
          "/response/docs/[0]/test_mvt_dvt_sf_str/[0]==aaaa",
          "/response/docs/[0]/test_mvt_dvt_sf_str/[1]==bbbb",
          "/response/docs/[0]/test_mvt_dvt_sf_str/[2]==cccc");
    } catch (Exception e) {
      // do nothing until SOLR-8740 is committed. At that point this should not throw an exception. 
      // NOTE: I think the test is correct after 8740 so just remove the try/catch
    }
    assertJQ(req("q", "id:3", "fl", "test_mvt_dvf_st_str"),
        "/response/docs/[0]/test_mvt_dvf_st_str/[0]==cccc",
        "/response/docs/[0]/test_mvt_dvf_st_str/[1]==aaaa",
        "/response/docs/[0]/test_mvt_dvf_st_str/[2]==bbbb");

    assertJQ(req("q", "id:4", "fl", "test_mvt_dvu_st_str"),
        "/response/docs/[0]/test_mvt_dvu_st_str/[0]==cccc",
        "/response/docs/[0]/test_mvt_dvu_st_str/[1]==aaaa",
        "/response/docs/[0]/test_mvt_dvu_st_str/[2]==bbbb");

  }
}
