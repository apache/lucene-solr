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
package org.apache.solr.util;

import org.apache.lucene.analysis.util.ResourceLoader;

import org.xml.sax.InputSource;
import org.xml.sax.EntityResolver;
import org.xml.sax.ext.EntityResolver2;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.URIResolver;
import javax.xml.transform.sax.SAXSource;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamException;

/**
 * This is a helper class to support resolving of XIncludes or other hrefs
 * inside XML files on top of a {@link ResourceLoader}. Just plug this class
 * on top of a {@link ResourceLoader} and pass it as {@link EntityResolver} to SAX parsers
 * or via wrapper methods as {@link URIResolver} to XSL transformers or {@link XMLResolver} to STAX parsers.
 * The resolver handles special SystemIds with an URI scheme of {@code solrres:} that point
 * to resources. To produce such systemIds when you initially call the parser, use
 * {@link #createSystemIdFromResourceName} which produces a SystemId that can
 * be included along the InputStream coming from {@link ResourceLoader#openResource}.
 * <p>In general create the {@link InputSource} to be passed to the parser like:</p>
 * <pre class="prettyprint">
 *  InputSource is = new InputSource(loader.openSchema(name));
 *  is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(name));
 *  final DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
 *  db.setEntityResolver(new SystemIdResolver(loader));
 *  Document doc = db.parse(is);
 * </pre>
 */
public final class SystemIdResolver implements EntityResolver, EntityResolver2 {

  public static final String RESOURCE_LOADER_URI_SCHEME = "solrres";
  public static final String RESOURCE_LOADER_AUTHORITY_ABSOLUTE = "@";

  private final ResourceLoader loader;

  public SystemIdResolver(ResourceLoader loader) {
    this.loader = loader;
  }
  
  public EntityResolver asEntityResolver() {
    return this;
  }
  
  public URIResolver asURIResolver() {
    return new URIResolver() {
      @Override
      public Source resolve(String href, String base) throws TransformerException {
        try {
          final InputSource src = SystemIdResolver.this.resolveEntity(null, null, base, href);
          return (src == null) ? null : new SAXSource(src);
        } catch (IOException ioe) {
          throw new TransformerException("Cannot resolve entity", ioe);
        }
      }
    };
  }
  
  public XMLResolver asXMLResolver() {
    return new XMLResolver() {
      @Override
      public Object resolveEntity(String publicId, String systemId, String baseURI, String namespace) throws XMLStreamException {
        try {
          final InputSource src = SystemIdResolver.this.resolveEntity(null, publicId, baseURI, systemId);
          return (src == null) ? null : src.getByteStream();
        } catch (IOException ioe) {
          throw new XMLStreamException("Cannot resolve entity", ioe);
        }
      }
    };
  }
  
  URI resolveRelativeURI(String baseURI, String systemId) throws URISyntaxException {
    URI uri;
    
    // special case for backwards compatibility: if relative systemId starts with "/" (we convert that to an absolute solrres:-URI)
    if (systemId.startsWith("/")) {
      uri = new URI(RESOURCE_LOADER_URI_SCHEME, RESOURCE_LOADER_AUTHORITY_ABSOLUTE, "/", null, null).resolve(systemId);
    } else {
      // simply parse as URI
      uri = new URI(systemId);
    }
    
    // do relative resolving
    if (baseURI != null ) {
      uri = new URI(baseURI).resolve(uri);
    }
    
    return uri;
  }
  
  // *** EntityResolver(2) methods:
  
  @Override
  public InputSource getExternalSubset(String name, String baseURI) {
    return null;
  }
  
  @Override
  public InputSource resolveEntity(String name, String publicId, String baseURI, String systemId) throws IOException {
    if (systemId == null) {
      return null;
    }
    try {
      final URI uri = resolveRelativeURI(baseURI, systemId);
      
      // check schema and resolve with ResourceLoader
      if (RESOURCE_LOADER_URI_SCHEME.equals(uri.getScheme())) {
        String path = uri.getPath(), authority = uri.getAuthority();
        if (!RESOURCE_LOADER_AUTHORITY_ABSOLUTE.equals(authority)) {
          path = path.substring(1);
        }
        try {
          final InputSource is = new InputSource(loader.openResource(path));
          is.setSystemId(uri.toASCIIString());
          is.setPublicId(publicId);
          return is;
        } catch (RuntimeException re) {
          // unfortunately XInclude fallback only works with IOException, but openResource() never throws that one
          throw new IOException(re.getMessage(), re);
        }
      } else {
        throw new IOException("Cannot resolve absolute systemIDs / external entities (only relative paths work): " + systemId);
      }
    } catch (URISyntaxException use) {
      throw new IOException("An URI syntax problem occurred during resolving systemId: " + systemId, use);
    }
  }

  @Override
  public InputSource resolveEntity(String publicId, String systemId) throws IOException {
    return resolveEntity(null, publicId, null, systemId);
  }
  
  public static String createSystemIdFromResourceName(String name) {
    name = name.replace(File.separatorChar, '/');
    final String authority;
    if (name.startsWith("/")) {
      // a hack to preserve absolute filenames and keep them absolute after resolving, we set the URI's authority to "@" on absolute filenames:
      authority = RESOURCE_LOADER_AUTHORITY_ABSOLUTE;
    } else {
      authority = null;
      name = "/" + name;
    }
    try {
      return new URI(RESOURCE_LOADER_URI_SCHEME, authority, name, null, null).toASCIIString();
    } catch (URISyntaxException use) {
      throw new IllegalArgumentException("Invalid syntax of Solr Resource URI", use);
    }
  }

}
