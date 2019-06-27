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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Locale;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update processor which examines a URL and outputs to various other fields
 * characteristics of that URL, including length, number of path levels, whether
 * it is a top level URL (levels==0), whether it looks like a landing/index page,
 * a canonical representation of the URL (e.g. stripping index.html), the domain
 * and path parts of the URL etc.
 * <p>
 * This processor is intended used in connection with processing web resources,
 * and helping to produce values which may be used for boosting or filtering later.
 */
public class URLClassifyProcessor extends UpdateRequestProcessor {

  private static final String INPUT_FIELD_PARAM = "inputField";
  private static final String OUTPUT_LENGTH_FIELD_PARAM = "lengthOutputField";
  private static final String OUTPUT_LEVELS_FIELD_PARAM = "levelsOutputField";
  private static final String OUTPUT_TOPLEVEL_FIELD_PARAM = "toplevelOutputField";
  private static final String OUTPUT_LANDINGPAGE_FIELD_PARAM = "landingpageOutputField";
  private static final String OUTPUT_DOMAIN_FIELD_PARAM = "domainOutputField";
  private static final String OUTPUT_CANONICALURL_FIELD_PARAM = "canonicalUrlOutputField";
  private static final String DEFAULT_URL_FIELDNAME = "url";
  private static final String DEFAULT_LENGTH_FIELDNAME = "url_length";
  private static final String DEFAULT_LEVELS_FIELDNAME = "url_levels";
  private static final String DEFAULT_TOPLEVEL_FIELDNAME = "url_toplevel";
  private static final String DEFAULT_LANDINGPAGE_FIELDNAME = "url_landingpage";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean enabled = true;
  private String urlFieldname = DEFAULT_URL_FIELDNAME;
  private String lengthFieldname = DEFAULT_LENGTH_FIELDNAME;
  private String levelsFieldname = DEFAULT_LEVELS_FIELDNAME;
  private String toplevelpageFieldname = DEFAULT_TOPLEVEL_FIELDNAME;
  private String landingpageFieldname = DEFAULT_LANDINGPAGE_FIELDNAME;
  private String domainFieldname = null;
  private String canonicalUrlFieldname = null;
  private static final String[] landingPageSuffixes = {
      "/",
      "index.html",
      "index.htm",
      "index.phtml",
      "index.shtml",
      "index.xml",
      "index.php",
      "index.asp",
      "index.aspx",
      "welcome.html",
      "welcome.htm",
      "welcome.phtml",
      "welcome.shtml",
      "welcome.xml",
      "welcome.php",
      "welcome.asp",
      "welcome.aspx"
  };

  public URLClassifyProcessor(SolrParams parameters,
      SolrQueryRequest request,
      SolrQueryResponse response,
      UpdateRequestProcessor nextProcessor) {
    super(nextProcessor);

    this.initParameters(parameters);
  }

  private void initParameters(SolrParams parameters) {
    if (parameters != null) {
      this.setEnabled(parameters.getBool("enabled", true));
      this.urlFieldname = parameters.get(INPUT_FIELD_PARAM, DEFAULT_URL_FIELDNAME);
      this.lengthFieldname = parameters.get(OUTPUT_LENGTH_FIELD_PARAM, DEFAULT_LENGTH_FIELDNAME);
      this.levelsFieldname = parameters.get(OUTPUT_LEVELS_FIELD_PARAM, DEFAULT_LEVELS_FIELDNAME);
      this.toplevelpageFieldname = parameters.get(OUTPUT_TOPLEVEL_FIELD_PARAM, DEFAULT_TOPLEVEL_FIELDNAME);
      this.landingpageFieldname = parameters.get(OUTPUT_LANDINGPAGE_FIELD_PARAM, DEFAULT_LANDINGPAGE_FIELDNAME);
      this.domainFieldname = parameters.get(OUTPUT_DOMAIN_FIELD_PARAM);
      this.canonicalUrlFieldname = parameters.get(OUTPUT_CANONICALURL_FIELD_PARAM);
    }
  }

  @Override
  public void processAdd(AddUpdateCommand command) throws IOException {
    if (isEnabled()) {
      SolrInputDocument document = command.getSolrInputDocument();
      if (document.containsKey(urlFieldname)) {
        String url = (String) document.getFieldValue(urlFieldname);
        try {
          URL normalizedURL = getNormalizedURL(url);
          document.setField(lengthFieldname, length(normalizedURL));
          document.setField(levelsFieldname, levels(normalizedURL));
          document.setField(toplevelpageFieldname, isTopLevelPage(normalizedURL) ? 1 : 0);
          document.setField(landingpageFieldname, isLandingPage(normalizedURL) ? 1 : 0);
          if (domainFieldname != null) {
            document.setField(domainFieldname, normalizedURL.getHost());
          }
          if (canonicalUrlFieldname != null) {
            document.setField(canonicalUrlFieldname, getCanonicalUrl(normalizedURL));
          }
          log.debug(document.toString());
        } catch (MalformedURLException | URISyntaxException e) {
          log.warn("cannot get the normalized url for \"" + url + "\" due to " + e.getMessage());
        }
      }
    }
    super.processAdd(command);
  }

  /**
   * Gets a canonical form of the URL for use as main URL
   * @param url The input url
   * @return The URL object representing the canonical URL
   */
  public URL getCanonicalUrl(URL url) throws MalformedURLException {
    // NOTE: Do we want to make sure this URL is normalized? (Christian thinks we should)
    String urlString = url.toString();
    String lps = landingPageSuffix(url);
    return new URL(urlString.replaceFirst("/" + lps + "$", "/"));
  }

  /**
   * Calculates the length of the URL in characters
   * @param url The input URL
   * @return the length of the URL
   */
  public int length(URL url) {
    return url.toString().length();
  }

  /**
   * Calculates the number of path levels in the given URL
   * @param url The input URL
   * @return the number of levels, where a top-level URL is 0
   */
  public int levels(URL url) {
    // Remove any trailing slashes for the purpose of level counting
    String path = getPathWithoutSuffix(url).replaceAll("/+$", "");
    int levels = 0;
    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == '/') {
        levels++;
      }
    }
    return levels;
  }

  /**
   * Calculates whether a URL is a top level page
   * @param url The input URL
   * @return true if page is a top level page
   */
  public boolean isTopLevelPage(URL url) {
    // Remove any trailing slashes for the purpose of level counting
    String path = getPathWithoutSuffix(url).replaceAll("/+$", "");
    return path.length() == 0 && url.getQuery() == null;
  }

  /**
   * Calculates whether the URL is a landing page or not
   * @param url The input URL
   * @return true if URL represents a landing page (index page)
   */
  public boolean isLandingPage(URL url) {
    if (url.getQuery() != null) {
      return false;
    } else {
      return landingPageSuffix(url) != "";
    }
  }

  public URL getNormalizedURL(String url) throws MalformedURLException, URISyntaxException {
    return new URI(url).normalize().toURL();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  private String landingPageSuffix(URL url) {
    String path = url.getPath().toLowerCase(Locale.ROOT);
    for(String suffix : landingPageSuffixes) {
      if(path.endsWith(suffix)) {
        return suffix;
      }
    }
    return "";
  }

  private String getPathWithoutSuffix(URL url) {
    return url.getPath().toLowerCase(Locale.ROOT).replaceFirst(landingPageSuffix(url)+"$", "");
  }
}
