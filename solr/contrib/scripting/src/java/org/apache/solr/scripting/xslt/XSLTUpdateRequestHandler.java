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

package org.apache.solr.scripting.xslt;

import static org.apache.solr.scripting.xslt.XSLTConstants.*;

import java.util.Map;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.common.EmptyEntityResolver;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.XMLLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * Send XML formatted documents to Solr, transforming them from the original XML
 * format to the Solr XML format using an XSLT stylesheet via the 'tr' parameter.
 */
public class XSLTUpdateRequestHandler extends UpdateRequestHandler {

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init(args);
    setAssumeContentType("application/xml");

    SolrParams p = null;
    if (args != null) {
      p = args.toSolrParams();
    }
    final XsltXMLLoader loader = new XsltXMLLoader().init(p);
    loaders = Map.of("application/xml", loader, "text/xml", loader);
  }

  @VisibleForTesting
  static class XsltXMLLoader extends XMLLoader {

    int xsltCacheLifetimeSeconds;

    @Override
    public XsltXMLLoader init(SolrParams args) {
      super.init(args);

      xsltCacheLifetimeSeconds = XSLT_CACHE_DEFAULT;
      if (args != null) {
        xsltCacheLifetimeSeconds = args.getInt(XSLT_CACHE_PARAM, XSLT_CACHE_DEFAULT);
      }
      return this;
    }

    @Override
    public void load(
        SolrQueryRequest req,
        SolrQueryResponse rsp,
        ContentStream stream,
        UpdateRequestProcessor processor)
        throws Exception {

      String tr = req.getParams().get(TR, null);
      if (tr == null) {
        super.load(req, rsp, stream, processor); // no XSLT; do standard processing
        return;
      }

      if (req.getCore().getCoreDescriptor().isConfigSetTrusted() == false) {
        throw new SolrException(
            SolrException.ErrorCode.UNAUTHORIZED,
            "The configset for this collection was uploaded without any authentication in place,"
                + " and this operation is not available for collections with untrusted configsets. To use this feature, re-upload the configset"
                + " after enabling authentication and authorization.");
      }

      final Transformer t = TransformerProvider.getTransformer(req, tr, xsltCacheLifetimeSeconds);
      final DOMResult result = new DOMResult();

      // first step: read XML and build DOM using Transformer (this is no overhead, as XSL always
      // produces
      // an internal result DOM tree, we just access it directly as input for StAX):
      try (var is = stream.getStream()) {
        final XMLReader xmlr = saxFactory.newSAXParser().getXMLReader();
        xmlr.setErrorHandler(xmllog);
        xmlr.setEntityResolver(EmptyEntityResolver.SAX_INSTANCE);
        final InputSource isrc = new InputSource(is);
        isrc.setEncoding(ContentStreamBase.getCharsetFromContentType(stream.getContentType()));
        final SAXSource source = new SAXSource(xmlr, isrc);
        t.transform(source, result);
      } catch (TransformerException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.toString(), e);
      }

      // second step: feed the intermediate DOM tree into StAX parser:
      XMLStreamReader parser = null; // does not implement AutoCloseable!
      try {
        parser = inputFactory.createXMLStreamReader(new DOMSource(result.getNode()));
        this.processUpdate(req, processor, parser);
      } catch (XMLStreamException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.toString(), e);
      } finally {
        if (parser != null) parser.close();
      }
    }

  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Add documents with XML, transforming with XSLT first";
  }
}
