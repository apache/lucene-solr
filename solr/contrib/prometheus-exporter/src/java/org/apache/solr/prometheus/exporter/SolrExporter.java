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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.XmlConfigFile;
import org.apache.solr.prometheus.collector.MetricsCollectorFactory;
import org.apache.solr.prometheus.collector.SchedulerMetricsCollector;
import org.apache.solr.prometheus.scraper.SolrCloudScraper;
import org.apache.solr.prometheus.scraper.SolrScraper;
import org.apache.solr.prometheus.scraper.SolrStandaloneScraper;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrExporter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String[] ARG_PORT_FLAGS = {"-p", "--port"};
  private static final String ARG_PORT_METAVAR = "PORT";
  private static final String ARG_PORT_DEST = "port";
  private static final int ARG_PORT_DEFAULT = 9983;
  private static final String ARG_PORT_HELP = "Specify the solr-exporter HTTP listen port; default is " + ARG_PORT_DEFAULT + ".";

  private static final String[] ARG_BASE_URL_FLAGS = {"-b", "--baseurl"};
  private static final String ARG_BASE_URL_METAVAR = "BASE_URL";
  private static final String ARG_BASE_URL_DEST = "baseUrl";
  private static final String ARG_BASE_URL_DEFAULT = "http://localhost:8983/solr";
  private static final String ARG_BASE_URL_HELP = "Specify the Solr base URL when connecting to Solr in standalone mode. If omitted both the -b parameter and the -z parameter, connect to http://localhost:8983/solr. For example 'http://localhost:8983/solr'.";

  private static final String[] ARG_ZK_HOST_FLAGS = {"-z", "--zkhost"};
  private static final String ARG_ZK_HOST_METAVAR = "ZK_HOST";
  private static final String ARG_ZK_HOST_DEST = "zkHost";
  private static final String ARG_ZK_HOST_DEFAULT = "";
  private static final String ARG_ZK_HOST_HELP = "Specify the ZooKeeper connection string when connecting to Solr in SolrCloud mode. If omitted both the -b parameter and the -z parameter, connect to http://localhost:8983/solr. For example 'localhost:2181/solr'.";

  private static final String[] ARG_CONFIG_FLAGS = {"-f", "--config-file"};
  private static final String ARG_CONFIG_METAVAR = "CONFIG";
  private static final String ARG_CONFIG_DEST = "configFile";
  private static final String ARG_CONFIG_DEFAULT = "./conf/solr-exporter-config.xml";
  private static final String ARG_CONFIG_HELP = "Specify the configuration file; the default is " + ARG_CONFIG_DEFAULT + ".";

  private static final String[] ARG_SCRAPE_INTERVAL_FLAGS = {"-s", "--scrape-interval"};
  private static final String ARG_SCRAPE_INTERVAL_METAVAR = "SCRAPE_INTERVAL";
  private static final String ARG_SCRAPE_INTERVAL_DEST = "scrapeInterval";
  private static final int ARG_SCRAPE_INTERVAL_DEFAULT = 60;
  private static final String ARG_SCRAPE_INTERVAL_HELP = "Specify the delay between scraping Solr metrics; the default is " + ARG_SCRAPE_INTERVAL_DEFAULT + " seconds.";

  private static final String[] ARG_NUM_THREADS_FLAGS = {"-n", "--num-threads"};
  private static final String ARG_NUM_THREADS_METAVAR = "NUM_THREADS";
  private static final String ARG_NUM_THREADS_DEST = "numThreads";
  private static final Integer ARG_NUM_THREADS_DEFAULT = 1;
  private static final String ARG_NUM_THREADS_HELP = "Specify the number of threads. solr-exporter creates a thread pools for request to Solr. If you need to improve request latency via solr-exporter, you can increase the number of threads; the default is " + ARG_NUM_THREADS_DEFAULT + ".";

  public static final CollectorRegistry defaultRegistry = new CollectorRegistry();

  private final int port;
  private final CachedPrometheusCollector prometheusCollector;
  private final SchedulerMetricsCollector metricsCollector;
  private final SolrScraper solrScraper;

  private final ExecutorService metricCollectorExecutor;
  private final ExecutorService requestExecutor;

  private HTTPServer httpServer;

  public SolrExporter(
      int port,
      int numberThreads,
      int scrapeInterval,
      SolrScrapeConfiguration scrapeConfiguration,
      MetricsConfiguration metricsConfiguration) {
    this.port = port;

    this.metricCollectorExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(
        numberThreads,
        new SolrNamedThreadFactory("solr-exporter-collectors"));

    this.requestExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(
        numberThreads,
        new SolrNamedThreadFactory("solr-exporter-requests"));

    this.solrScraper = createScraper(scrapeConfiguration, metricsConfiguration.getSettings());
    this.metricsCollector = new MetricsCollectorFactory(metricCollectorExecutor, scrapeInterval, solrScraper, metricsConfiguration).create();
    this.prometheusCollector = new CachedPrometheusCollector();
  }

  void start() throws IOException {
    defaultRegistry.register(prometheusCollector);

    metricsCollector.addObserver(prometheusCollector);
    metricsCollector.start();

    httpServer = new HTTPServer(new InetSocketAddress(port), defaultRegistry);
  }

  void stop() {
    httpServer.stop();

    metricsCollector.removeObserver(prometheusCollector);

    requestExecutor.shutdownNow();
    metricCollectorExecutor.shutdownNow();

    IOUtils.closeQuietly(metricsCollector);
    IOUtils.closeQuietly(solrScraper);

    defaultRegistry.unregister(this.prometheusCollector);
  }

  private SolrScraper createScraper(SolrScrapeConfiguration configuration, PrometheusExporterSettings settings) {
    SolrClientFactory factory = new SolrClientFactory(settings);

    switch (configuration.getType()) {
      case STANDALONE:
        return new SolrStandaloneScraper(
            factory.createStandaloneSolrClient(configuration.getSolrHost().get()), requestExecutor);
      case CLOUD:
        return new SolrCloudScraper(
            factory.createCloudSolrClient(configuration.getZookeeperConnectionString().get()), requestExecutor, factory);
      default:
        throw new RuntimeException(String.format(Locale.ROOT, "Invalid type: %s", configuration.getType()));
    }
  }

  public static void main(String[] args) {
    ArgumentParser parser = ArgumentParsers.newFor(SolrExporter.class.getSimpleName()).build()
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

    parser.addArgument(ARG_SCRAPE_INTERVAL_FLAGS)
        .metavar(ARG_SCRAPE_INTERVAL_METAVAR).dest(ARG_SCRAPE_INTERVAL_DEST).type(Integer.class)
        .setDefault(ARG_SCRAPE_INTERVAL_DEFAULT).help(ARG_SCRAPE_INTERVAL_HELP);

    parser.addArgument(ARG_NUM_THREADS_FLAGS)
        .metavar(ARG_NUM_THREADS_METAVAR).dest(ARG_NUM_THREADS_DEST).type(Integer.class)
        .setDefault(ARG_NUM_THREADS_DEFAULT).help(ARG_NUM_THREADS_HELP);

    try {
      Namespace res = parser.parseArgs(args);

      SolrScrapeConfiguration scrapeConfiguration = null;

      if (!res.getString(ARG_ZK_HOST_DEST).equals("")) {
        scrapeConfiguration = SolrScrapeConfiguration.solrCloud(res.getString(ARG_ZK_HOST_DEST));
      } else if (!res.getString(ARG_BASE_URL_DEST).equals("")) {
        scrapeConfiguration = SolrScrapeConfiguration.standalone(res.getString(ARG_BASE_URL_DEST));
      }

      if (scrapeConfiguration == null) {
        log.error("Must provide either {} or {}", ARG_BASE_URL_FLAGS, ARG_ZK_HOST_FLAGS);
      }

      SolrExporter solrExporter = new SolrExporter(
          res.getInt(ARG_PORT_DEST),
          res.getInt(ARG_NUM_THREADS_DEST),
          res.getInt(ARG_SCRAPE_INTERVAL_DEST),
          scrapeConfiguration,
          loadMetricsConfiguration(Paths.get(res.getString(ARG_CONFIG_DEST))));

      log.info("Starting Solr Prometheus Exporting");
      solrExporter.start();
      log.info("Solr Prometheus Exporter is running");
    } catch (IOException e) {
      log.error("Failed to start Solr Prometheus Exporter: ", e);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
    }
  }

  private static MetricsConfiguration loadMetricsConfiguration(Path configPath) {
    try (SolrResourceLoader loader = new SolrResourceLoader(configPath.getParent())) {
      XmlConfigFile config = new XmlConfigFile(loader, configPath.getFileName().toString(), null, null);
      return MetricsConfiguration.from(config);
    } catch (Exception e) {
      log.error("Could not load scrape configuration from {}", configPath.toAbsolutePath());
      throw new RuntimeException(e);
    }
  }

}
