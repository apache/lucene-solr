package org.apache.solr.handler;
/**
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

import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.xslt.TransformerProvider;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.core.SolrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.InputSource;

import java.io.InputStream;
import java.io.IOException;
import java.util.Map;


/**
 * Extends the XMLLoader by applying an XSLT transform before the
 * XMLLoader actually loads the XML
 *
 **/
class XsltXMLLoader extends XMLLoader {

  public static final String TRANSFORM_PARAM = "tr";
  public static final String CONTEXT_TRANSFORMER_KEY = "xsltupdater.transformer";
  
  private final Integer xsltCacheLifetimeSeconds; 

  public XsltXMLLoader(UpdateRequestProcessor processor, XMLInputFactory inputFactory, Integer xsltCacheLifetimeSeconds) {
    super(processor, inputFactory);
    this.xsltCacheLifetimeSeconds = xsltCacheLifetimeSeconds;
  }

  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream) throws Exception {
    final DOMResult result = new DOMResult();
    final Transformer t = getTransformer(req);
    InputStream is = null;
    XMLStreamReader parser = null;
    // first step: read XML and build DOM using Transformer (this is no overhead, as XSL always produces
    // an internal result DOM tree, we just access it directly as input for StAX):
    try {
      is = stream.getStream();
      final String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());
      final InputSource isrc = new InputSource(is);
      isrc.setEncoding(charset);
      final SAXSource source = new SAXSource(isrc);
      t.transform(source, result);
    } catch(TransformerException te) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, te.getMessage(), te);
    } finally {
      IOUtils.closeQuietly(is);
    }
    // second step feed the intermediate DOM tree into StAX parser:
    try {
      parser = inputFactory.createXMLStreamReader(new DOMSource(result.getNode()));
      this.processUpdate(req, processor, parser);
    } catch (XMLStreamException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
    } finally {
      if (parser != null) parser.close();
    }
  }


  /** Get Transformer from request context, or from TransformerProvider.
   *  This allows either getContentType(...) or write(...) to instantiate the Transformer,
   *  depending on which one is called first, then the other one reuses the same Transformer
   */
  protected Transformer getTransformer(SolrQueryRequest request) throws IOException {
    final String xslt = request.getParams().get(TRANSFORM_PARAM,null);
    if(xslt==null) {
      throw new IOException("'" + TRANSFORM_PARAM + "' request parameter is required to use the XSLTResponseWriter");
    }
    // not the cleanest way to achieve this
    SolrConfig solrConfig = request.getCore().getSolrConfig();
    // no need to synchronize access to context, right? 
    // Nothing else happens with it at the same time
    final Map<Object,Object> ctx = request.getContext();
    Transformer result = (Transformer)ctx.get(CONTEXT_TRANSFORMER_KEY);
    if(result==null) {
      result = TransformerProvider.instance.getTransformer(solrConfig, xslt,xsltCacheLifetimeSeconds.intValue());
      result.setErrorListener(XsltUpdateRequestHandler.xmllog);
      ctx.put(CONTEXT_TRANSFORMER_KEY,result);
    }
    return result;
  }

}
