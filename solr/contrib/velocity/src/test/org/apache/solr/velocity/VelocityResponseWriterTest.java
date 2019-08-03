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
package org.apache.solr.velocity;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrParamResourceLoader;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.VelocityResponseWriter;
import org.junit.BeforeClass;
import org.junit.Test;

public class VelocityResponseWriterTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml", getFile("velocity/solr").getAbsolutePath());
    System.out.println(getFile("velocity/solr").getAbsolutePath());
  }

  @Test
  public void testVelocityResponseWriterRegistered() {
    QueryResponseWriter writer = h.getCore().getQueryResponseWriter("velocity");
    assertTrue("VrW registered check", writer instanceof VelocityResponseWriter);
  }

  @Test
  public void testCustomParamTemplate() throws Exception {
    org.apache.solr.response.VelocityResponseWriter vrw = new VelocityResponseWriter();
    NamedList<String> nl = new NamedList<>();
    nl.add(VelocityResponseWriter.PARAMS_RESOURCE_LOADER_ENABLED, "true");
    vrw.init(nl);
    SolrQueryRequest req = req(VelocityResponseWriter.TEMPLATE,"custom",
        SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"custom","$response.response.response_data");
    SolrQueryResponse rsp = new SolrQueryResponse();
    StringWriter buf = new StringWriter();
    rsp.add("response_data", "testing");
    vrw.write(buf, req, rsp);
    assertEquals("testing", buf.toString());
  }

  @Test
  public void testParamResourceLoaderDisabled() {
    VelocityResponseWriter vrw = new VelocityResponseWriter();
    // by default param resource loader is disabled, no need to set it here
    SolrQueryRequest req = req(VelocityResponseWriter.TEMPLATE,"custom",
        SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"custom","$response.response.response_data");
    SolrQueryResponse rsp = new SolrQueryResponse();
    StringWriter buf = new StringWriter();
    expectThrows(IOException.class, () -> vrw.write(buf, req, rsp));
  }

  @Test
  public void testFileResourceLoader() throws Exception {
    VelocityResponseWriter vrw = new VelocityResponseWriter();
    NamedList<String> nl = new NamedList<>();
    nl.add("template.base.dir", getFile("velocity").getAbsolutePath());
    vrw.init(nl);
    SolrQueryRequest req = req(VelocityResponseWriter.TEMPLATE,"file");
    SolrQueryResponse rsp = new SolrQueryResponse();
    StringWriter buf = new StringWriter();
    vrw.write(buf, req, rsp);
    assertEquals("testing", buf.toString());
  }

  @Test
  public void testSolrResourceLoaderTemplate() throws Exception {
    assertEquals("0", h.query(req("q","*:*", "wt","velocity",VelocityResponseWriter.TEMPLATE,"numFound")));
  }

  @Test
  public void testEncoding() throws Exception {
    assertEquals("éñçø∂îñg", h.query(req("q","*:*", "wt","velocity",VelocityResponseWriter.TEMPLATE,"encoding")));
  }

  @Test
  public void testMacros() throws Exception {
    // tests that a macro in a custom macros.vm is visible
    assertEquals("test_macro_SUCCESS", h.query(req("q","*:*", "wt","velocity",VelocityResponseWriter.TEMPLATE,"test_macro_visible")));

    // tests that a builtin (_macros.vm) macro, #url_root in this case, can be overridden in a custom macros.vm
    // the macro is also defined in VM_global_library.vm, which should also be overridden by macros.vm
    assertEquals("Loaded from: macros.vm", h.query(req("q","*:*", "wt","velocity",VelocityResponseWriter.TEMPLATE,"test_macro_overridden")));

    // tests that macros defined in VM_global_library.vm are visible.  This file was where macros in pre-5.0 versions were defined
    assertEquals("legacy_macro_SUCCESS", h.query(req("q","*:*", "wt","velocity",VelocityResponseWriter.TEMPLATE,"test_macro_legacy_support")));
  }

  @Test
  public void testInitProps() throws Exception {
    // The test init properties file turns off being able to use $foreach.index (the implicit loop counter)
    // The foreach.vm template uses $!foreach.index, with ! suppressing the literal "$foreach.index" output

    assertEquals("01", h.query(req("q","*:*", "wt","velocity",VelocityResponseWriter.TEMPLATE,"foreach")));
    assertEquals("", h.query(req("q","*:*", "wt","velocityWithInitProps",VelocityResponseWriter.TEMPLATE,"foreach")));
  }

  @Test
  public void testCustomTools() throws Exception {
    // custom_tool.vm responds with $!mytool.star("foo"), but $mytool is not defined (only in velocityWithCustomTools)
    assertEquals("", h.query(req("q","*:*", "wt","velocity",VelocityResponseWriter.TEMPLATE,"custom_tool")));

    assertEquals("** LATERALUS **", h.query(req("q","*:*", "wt","velocityWithCustomTools",VelocityResponseWriter.TEMPLATE,"t",
            SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"t", "$mytool.star(\"LATERALUS\")")));

    // Does $log get overridden?
    assertEquals("** log overridden **", h.query(req("q","*:*", "wt","velocityWithCustomTools",VelocityResponseWriter.TEMPLATE,"t",
            SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"t", "$log.star(\"log overridden\")")));

    // Does $response get overridden?  actual blank response because of the bang on $! reference that silences bogus $-references
    assertEquals("", h.query(req("q","*:*", "wt","velocityWithCustomTools",VelocityResponseWriter.TEMPLATE,"t",
        SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"t", "$!response.star(\"response overridden??\")")));

    // Custom tools can also have a SolrCore-arg constructor because they are instantiated with SolrCore.createInstance
    // TODO: do we really need to support this?  no great loss, as a custom tool could take a SolrCore object as a parameter to
    // TODO: any method, so one could do $mytool.my_method($request.core)
    // I'm currently inclined to make this feature undocumented/unsupported, as we may want to instantiate classes
    // in a different manner that only supports no-arg constructors, commented (passing) test case out
//    assertEquals("collection1", h.query(req("q","*:*", "wt","velocityWithCustomTools",VelocityResponseWriter.TEMPLATE,"t",
//        SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"t", "$mytool.core.name")));
  }

  @Test
  public void testLocaleFeature() throws Exception {
    assertEquals("Color", h.query(req("q", "*:*", "wt", "velocity", VelocityResponseWriter.TEMPLATE, "locale",
        VelocityResponseWriter.LOCALE,"en_US")));
    assertEquals("Colour", h.query(req("q", "*:*", "wt", "velocity", VelocityResponseWriter.TEMPLATE, "locale",
        VelocityResponseWriter.LOCALE,"en_UK")));

    // Test that $resource.get(key,baseName,locale) works with specified locale
    assertEquals("Colour", h.query(req("q","*:*", "wt","velocity",VelocityResponseWriter.TEMPLATE,"resource_get")));

    // Test that $number tool uses the specified locale
    assertEquals("2,112", h.query(req("q","*:*", "wt","velocityWithCustomTools",VelocityResponseWriter.TEMPLATE,"t",
        SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"t","$number.format(2112)", VelocityResponseWriter.LOCALE, "en_US")));
    assertEquals("2.112", h.query(req("q","*:*", "wt","velocityWithCustomTools",VelocityResponseWriter.TEMPLATE,"t",
        SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"t","$number.format(2112)", VelocityResponseWriter.LOCALE, "de_DE")));

    // Test that custom tool extending LocaleConfig gets the right locale
    assertEquals("de_DE", h.query(req("q","*:*", "wt","velocityWithCustomTools",VelocityResponseWriter.TEMPLATE,"t",
        SolrParamResourceLoader.TEMPLATE_PARAM_PREFIX+"t","$mytool.locale", VelocityResponseWriter.LOCALE, "de_DE")));
  }

  @Test
  public void testLayoutFeature() throws Exception {
    assertEquals("{{{0}}}", h.query(req("q","*:*", "wt","velocity",
        VelocityResponseWriter.TEMPLATE,"numFound", VelocityResponseWriter.LAYOUT,"layout")));

    // even with v.layout specified, layout can be disabled explicitly
    assertEquals("0", h.query(req("q","*:*", "wt","velocity",
        VelocityResponseWriter.TEMPLATE,"numFound",
        VelocityResponseWriter.LAYOUT,"layout",
        VelocityResponseWriter.LAYOUT_ENABLED,"false")));
  }

  @Test
  public void testJSONWrapper() throws Exception {
    assertEquals("foo({\"result\":\"0\"})", h.query(req("q", "*:*", "wt", "velocity",
        VelocityResponseWriter.TEMPLATE, "numFound",
        VelocityResponseWriter.JSON,"foo")));

    // Now with layout, for good measure
    assertEquals("foo({\"result\":\"{{{0}}}\"})", h.query(req("q", "*:*", "wt", "velocity",
        VelocityResponseWriter.TEMPLATE, "numFound",
        VelocityResponseWriter.JSON,"foo",
        VelocityResponseWriter.LAYOUT,"layout")));

    assertQEx("Bad function name should throw exception", req("q", "*:*", "wt", "velocity",
        VelocityResponseWriter.TEMPLATE, "numFound",
        VelocityResponseWriter.JSON,"<foo>"), SolrException.ErrorCode.BAD_REQUEST
    );
  }

  @Test
  public void testContentType() {
    VelocityResponseWriter vrw = new VelocityResponseWriter();
    NamedList<String> nl = new NamedList<>();
    vrw.init(nl);
    SolrQueryResponse rsp = new SolrQueryResponse();

    // with v.json=wrf, content type should default to application/json
    assertEquals("application/json;charset=UTF-8",
        vrw.getContentType(req(VelocityResponseWriter.TEMPLATE, "numFound",
            VelocityResponseWriter.JSON, "wrf"), rsp));

    // with no v.json specified, the default text/html should be returned
    assertEquals("text/html;charset=UTF-8",
        vrw.getContentType(req(VelocityResponseWriter.TEMPLATE, "numFound"), rsp));

    // if v.contentType is specified, that should be used, even if v.json is specified
    assertEquals("text/plain",
        vrw.getContentType(req(VelocityResponseWriter.TEMPLATE, "numFound",
            VelocityResponseWriter.CONTENT_TYPE,"text/plain"), rsp));
    assertEquals("text/plain",
        vrw.getContentType(req(VelocityResponseWriter.TEMPLATE, "numFound",
            VelocityResponseWriter.JSON,"wrf",
            VelocityResponseWriter.CONTENT_TYPE,"text/plain"), rsp));
  }
}
