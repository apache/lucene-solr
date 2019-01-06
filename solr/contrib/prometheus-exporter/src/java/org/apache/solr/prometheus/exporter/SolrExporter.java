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
package org.apache.solr.prometheus.exporter;

import javax.xml.parsers.ParserConfigurationException;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.HTTPServer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.core.XmlConfigFile;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.prometheus.collector.SolrCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SolrExporter
 */
public class SolrExporter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String[] ARG_PORT_FLAGS = { "-p", "--port" };
  private static final String ARG_PORT_METAVAR = "PORT";
  private static final String ARG_PORT_DEST = "port";
  private static final Integer ARG_PORT_DEFAULT = 9983;
  private static final String ARG_PORT_HELP = "Specify the solr-exporter HTTP listen port; default is " + String.valueOf(ARG_PORT_DEFAULT) + ".";

  private static final String[] ARG_BASE_URL_FLAGS = { "-b", "--baseurl" };
  private static final String ARG_BASE_URL_METAVAR = "BASE_URL";
  private static final String ARG_BASE_URL_DEST = "baseUrl";
  private static final String ARG_BASE_URL_DEFAULT = "";
  private static final String ARG_BASE_URL_HELP = "Specify the Solr base URL when connecting to Solr in standalone mode. If omitted both the -b parameter and the -z parameter, connect to http://localhost:8983/solr. For example 'http://localhost:8983/solr'.";

  private static final String[] ARG_ZK_HOST_FLAGS = { "-z", "--zkhost" };
  private static final String ARG_ZK_HOST_METAVAR = "ZK_HOST";
  private static final String ARG_ZK_HOST_DEST = "zkHost";
  private static final String ARG_ZK_HOST_DEFAULT = "";
  private static final String ARG_ZK_HOST_HELP = "Specify the ZooKeeper connection string when connecting to Solr in SolrCloud mode. If omitted both the -b parameter and the -z parameter, connect to http://localhost:8983/solr. For example 'localhost:2181/solr'.";

  private static final String[] ARG_CONFIG_FLAGS = { "-f", "--config-file" };
  private static final String ARG_CONFIG_METAVAR = "CONFIG";
  private static final String ARG_CONFIG_DEST = "configFile";
  private static final String ARG_CONFIG_DEFAULT = "./conf/solr-exporter-config.xml";
  private static final String ARG_CONFIG_HELP = "Specify the configuration file; default is " + ARG_CONFIG_DEFAULT + ".";

  private static final String[] ARG_NUM_THREADS_FLAGS = { "-n", "--num-threads" };
  private static final String ARG_NUM_THREADS_METAVAR = "NUM_THREADS";
  private static final String ARG_NUM_THREADS_DEST = "numThreads";
  private static final Integer ARG_NUM_THREADS_DEFAULT = 1;
  private static final String ARG_NUM_THREADS_HELP = "Specify the number of threads. solr-exporter creates a thread pools for request to Solr. If you need to improve request latency via solr-exporter, you can increase the number of threads; default is " + String.valueOf(ARG_NUM_THREADS_DEFAULT) + ".";

  private int port;
  private SolrClient solrClient;
  private XmlConfigFile config;
  private int numThreads;

  CollectorRegistry registry = new CollectorRegistry();

  private HTTPServer httpServer;
  private SolrCollector collector;

  private SolrResourceLoader loader;

  public static final Counter scrapeErrorTotal = Counter.build()
      .name("solr_exporter_scrape_error_total")
      .help("Number of scrape error.").register();

  /**
   * Constructor.
   */
  public SolrExporter(int port, String connStr, Path configPath, int numThreads) throws ParserConfigurationException, SAXException, IOException {
    this(port, createClient(connStr), configPath, numThreads);
  }

  /**
   * Constructor.
   */
  public SolrExporter(int port, SolrClient solrClient, Path configPath, int numThreads) throws ParserConfigurationException, SAXException, IOException {
    super();

    this.loader = new SolrResourceLoader(configPath.getParent());

    this.port = port;
    this.solrClient = solrClient;
    this.config = new XmlConfigFile(this.loader, configPath.getFileName().toString());
    this.numThreads = numThreads;
  }

  /**
   * Start HTTP server for exporting Solr metrics.
   */
  public void start() throws IOException {
    this.collector = new SolrCollector(solrClient, config, numThreads);
    this.registry.register(this.collector);
    this.registry.register(scrapeErrorTotal);
    this.httpServer = new HTTPServer(new InetSocketAddress(port), this.registry);
  }

  /**
   * Stop HTTP server for exporting Solr metrics.
   */
  public void stop() {
    this.httpServer.stop();
    this.registry.unregister(this.collector);
  }

  /**
   * Create Solr client
   */
  private static SolrClient createClient(String connStr) {
    SolrClient solrClient;

    Pattern baseUrlPattern = Pattern.compile("^https?:\\/\\/[\\w\\/:%#\\$&\\?\\(\\)~\\.=\\+\\-]+$");
    Pattern zkHostPattern = Pattern.compile("^(?<host>[^\\/]+)(?<chroot>|(?:\\/.*))$");
    Matcher matcher;

    matcher = baseUrlPattern.matcher(connStr);
    if (matcher.matches()) {
      NoOpResponseParser responseParser = new NoOpResponseParser();
      responseParser.setWriterType("json");

      HttpSolrClient.Builder builder = new HttpSolrClient.Builder();
      builder.withBaseSolrUrl(connStr);

      HttpSolrClient httpSolrClient = builder.build();
      httpSolrClient.setParser(responseParser);

      solrClient = httpSolrClient;
    } else {
      String host = "";
      String chroot = "";

      matcher = zkHostPattern.matcher(connStr);
      if (matcher.matches()) {
        host = matcher.group("host") != null ? matcher.group("host") : "";
        chroot = matcher.group("chroot") != null ? matcher.group("chroot") : "";
      }

      NoOpResponseParser responseParser = new NoOpResponseParser();
      responseParser.setWriterType("json");

      CloudSolrClient.Builder builder = new CloudSolrClient.Builder();
      if (host.contains(",")) {
        List<String> hosts = new ArrayList<>();
        for (String h : host.split(",")) {
          if (h != null && !h.equals("")) {
            hosts.add(h.trim());
          }
        }
        builder.withZkHost(hosts);
      } else {
        builder.withZkHost(host);
      }
      if (chroot.equals("")) {
        builder.withZkChroot("/");
      } else {
        builder.withZkChroot(chroot);
      }

      CloudSolrClient cloudSolrClient = builder.build();
      cloudSolrClient.setParser(responseParser);

      solrClient = cloudSolrClient;
    }

    return solrClient;
  }

  /**
   * Entry point of SolrExporter.
   */
  public static void main( String[] args ) {
    ArgumentParser parser = ArgumentParsers.newArgumentParser(SolrCollector.class.getSimpleName())
        .description("Prometheus exporter for Apache Solr.");

    parser.addArgument(ARG_PORT_FLAGS)
        .metavar(ARG_PORT_METAVAR).dest(ARG_PORT_DEST).type(Integer.class)
        .setDefault(ARG_PORT_DEFAULT).help(ARG_PORT_HELP);

    parser.addArgument(ARG_BASE_URL_FLAGS)
        .metavar(ARG_BASE_URL_METAVAR).dest(ARG_BASE_URL_DEST).type(String.class)
        .setDefault(ARG_BASE_URL_DEFAULT).help(ARG_BASE_URL_HELP);

    parser.addArgument(ARG_ZK_HOST_FLAGS)
        .metavar(ARG_ZK_HOST_METAVAR).dest(ARG_ZK_HOST_DEST).type(String.class)
        .setDefault(ARG_ZK_HOST_DEFAULT).help(ARG_ZK_HOST_HELP);

    parser.addArgument(ARG_CONFIG_FLAGS)
        .metavar(ARG_CONFIG_METAVAR).dest(ARG_CONFIG_DEST).type(String.class)
        .setDefault(ARG_CONFIG_DEFAULT).help(ARG_CONFIG_HELP);

    parser.addArgument(ARG_NUM_THREADS_FLAGS)
        .metavar(ARG_NUM_THREADS_METAVAR).dest(ARG_NUM_THREADS_DEST).type(Integer.class)
        .setDefault(ARG_NUM_THREADS_DEFAULT).help(ARG_NUM_THREADS_HELP);

    try {
      Namespace res = parser.parseArgs(args);

      int port = res.getInt(ARG_PORT_DEST);

      String connStr = "http://localhost:8983/solr";
      if (!res.getString(ARG_BASE_URL_DEST).equals("")) {
        connStr = res.getString(ARG_BASE_URL_DEST);
      } else if (!res.getString(ARG_ZK_HOST_DEST).equals("")) {
        connStr = res.getString(ARG_ZK_HOST_DEST);
      }

      Path configPath = Paths.get(res.getString(ARG_CONFIG_DEST));
      int numThreads = res.getInt(ARG_NUM_THREADS_DEST);

      SolrExporter solrExporter = new SolrExporter(port, connStr, configPath, numThreads);
      solrExporter.start();
      log.info("Start server");
    } catch (ParserConfigurationException | SAXException | IOException e) {
      log.error("Start server failed: " + e.toString());
    } catch (ArgumentParserException e) {
      parser.handleError(e);
    }
  }
}
