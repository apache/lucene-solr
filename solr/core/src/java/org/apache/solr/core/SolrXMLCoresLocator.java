package org.apache.solr.core;

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

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Writes any changes in core definitions to this instance's solr.xml
 */
public class SolrXMLCoresLocator implements CoresLocator {

  private static final Logger logger = LoggerFactory.getLogger(SolrXMLCoresLocator.class);

  private final String solrXmlTemplate;
  private final ConfigSolrXmlOld cfg;

  /** Core name to use if a core definition has no name */
  public static final String DEFAULT_CORE_NAME = "collection1";

  /**
   * Create a new SolrXMLCoresLocator
   * @param originalXML   the original content of the solr.xml file
   * @param cfg           the CoreContainer's config object
   */
  public SolrXMLCoresLocator(String originalXML, ConfigSolrXmlOld cfg) {
    this.solrXmlTemplate = buildTemplate(originalXML);
    this.cfg = cfg;
  }

  private static Pattern POPULATED_CORES_TAG
      = Pattern.compile("^(.*<cores[^>]*>)(.*)(</cores>.*)$", Pattern.DOTALL);
  private static Pattern EMPTY_CORES_TAG
      = Pattern.compile("^(.*<cores[^>]*)/>(.*)$", Pattern.DOTALL);

  private static Pattern SHARD_HANDLER_TAG
      = Pattern.compile("(<shardHandlerFactory[^>]*>.*</shardHandlerFactory>)|(<shardHandlerFactory[^>]*/>)",
                          Pattern.DOTALL);

  private static String CORES_PLACEHOLDER = "{{CORES_PLACEHOLDER}}";

  // Package-private for testing
  // We replace the existing <cores></cores> contents with a template pattern
  // that we can later replace with the up-to-date core definitions.  We also
  // need to extract the <shardHandlerFactory> section, as, annoyingly, it's
  // kept inside <cores/>.
  static String buildTemplate(String originalXML) {

    String shardHandlerConfig = "";
    Matcher shfMatcher = SHARD_HANDLER_TAG.matcher(originalXML);
    if (shfMatcher.find()) {
      shardHandlerConfig = shfMatcher.group(0);
    }

    Matcher popMatcher = POPULATED_CORES_TAG.matcher(originalXML);
    if (popMatcher.matches()) {
      return new StringBuilder(popMatcher.group(1))
          .append(CORES_PLACEHOLDER).append(shardHandlerConfig).append(popMatcher.group(3)).toString();
    }

    // Self-closing <cores/> tag gets expanded to <cores></cores>
    Matcher emptyMatcher = EMPTY_CORES_TAG.matcher(originalXML);
    if (emptyMatcher.matches())
      return new StringBuilder(emptyMatcher.group(1))
          .append(">").append(CORES_PLACEHOLDER).append("</cores>")
          .append(emptyMatcher.group(2)).toString();

    // If there's no <cores> tag at all, add one at the end of the file
    return originalXML.replace("</solr>", "<cores>" + CORES_PLACEHOLDER + "</cores></solr>");
  }

  // protected access for testing
  protected String buildSolrXML(List<CoreDescriptor> cds) {
    StringBuilder builder = new StringBuilder();
    for (CoreDescriptor cd : cds) {
      builder.append(buildCoreTag(cd));
    }
    return solrXmlTemplate.replace(CORES_PLACEHOLDER, builder.toString());
  }

  public static final String NEWLINE = System.getProperty("line.separator");
  public static final String INDENT = "    ";

  /**
   * Serialize a coredescriptor as a String containing an XML &lt;core> tag.
   * @param cd the CoreDescriptor
   * @return an XML representation of the CoreDescriptor
   */
  protected static String buildCoreTag(CoreDescriptor cd) {

    StringBuilder builder = new StringBuilder(NEWLINE).append(INDENT).append("<core");
    for (Map.Entry<Object, Object> entry : cd.getPersistableStandardProperties().entrySet()) {
      builder.append(" ").append(entry.getKey()).append("=\"").append(entry.getValue()).append("\"");
    }

    Properties userProperties = cd.getPersistableUserProperties();
    if (userProperties.isEmpty()) {
      return builder.append("/>").append(NEWLINE).toString();
    }

    builder.append(">").append(NEWLINE);
    for (Map.Entry<Object, Object> entry : userProperties.entrySet()) {
      builder.append(INDENT).append(INDENT)
          .append("<property name=\"").append(entry.getKey()).append("\" value=\"")
          .append(entry.getValue()).append("\"/>").append(NEWLINE);
    }

    return builder.append("</core>").append(NEWLINE).toString();

  }

  @Override
  public synchronized final void persist(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    List<CoreDescriptor> cds = new ArrayList<>(cc.getCoreDescriptors().size() + coreDescriptors.length);
    
    cds.addAll(cc.getCoreDescriptors());
    cds.addAll(Arrays.asList(coreDescriptors));

    doPersist(buildSolrXML(cds));
  }

  protected void doPersist(String xml) {
    File file = new File(cfg.config.getResourceLoader().getInstanceDir(), ConfigSolr.SOLR_XML_FILE);
    Writer writer = null;
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
      writer.write(xml);
      writer.close();
      logger.info("Persisted core descriptions to {}", file.getAbsolutePath());
    } catch (IOException e) {
      logger.error("Couldn't persist core descriptions to {} : {}",
          file.getAbsolutePath(), e);
    } finally {
      IOUtils.closeQuietly(writer);
      IOUtils.closeQuietly(fos);
    }
  }

  @Override
  public void create(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    this.persist(cc, coreDescriptors);
  }

  @Override
  public void delete(CoreContainer cc, CoreDescriptor... coreDescriptors) {
    // coreDescriptors is kind of a useless param - we persist the current state off cc
    this.persist(cc);
  }

  @Override
  public void rename(CoreContainer cc, CoreDescriptor oldCD, CoreDescriptor newCD) {
    // we don't need those params, we just write out the current cc state
    this.persist(cc);
  }

  @Override
  public void swap(CoreContainer cc, CoreDescriptor cd1, CoreDescriptor cd2) {
    this.persist(cc);
  }

  @Override
  public List<CoreDescriptor> discover(CoreContainer cc) {

    ImmutableList.Builder<CoreDescriptor> listBuilder = ImmutableList.builder();

    for (String coreName : cfg.getAllCoreNames()) {

      String name = cfg.getProperty(coreName, CoreDescriptor.CORE_NAME, DEFAULT_CORE_NAME);
      String instanceDir = cfg.getProperty(coreName, CoreDescriptor.CORE_INSTDIR, "");

      Properties coreProperties = new Properties();
      for (String propName : CoreDescriptor.standardPropNames) {
        String propValue = cfg.getProperty(coreName, propName, "");
        if (StringUtils.isNotEmpty(propValue))
          coreProperties.setProperty(propName, propValue);
      }
      coreProperties.putAll(cfg.getCoreProperties(coreName));

      listBuilder.add(new CoreDescriptor(cc, name, instanceDir, coreProperties));
    }

    return listBuilder.build();
  }

  // for testing
  String getTemplate() {
    return solrXmlTemplate;
  }

  public static class NonPersistingLocator extends SolrXMLCoresLocator {

    public NonPersistingLocator(String originalXML, ConfigSolrXmlOld cfg) {
      super(originalXML, cfg);
      this.xml = originalXML;
    }

    @Override
    public void doPersist(String xml) {
      this.xml = xml;
    }

    public String xml;

  }

}
