package org.apache.solr.search;

import java.io.IOException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * This component can be configured for a RequestHandler for query requests
 *
 * <ul>
 * <li>This component mandates that clients to pass in a "qi" request parameter with a valid value which is configured in the SearchComponent definition in the solrconfig.xml file
 * <li>It fails the query if the "qi" parameter is missing or if the value passed in is in-valid. This behavior of failing the queries can be controlled by the failQueries config parameter
 * <li>If also collects the rate per sec metric per unique "qi" value
 * </ul>
 *
 *
 */
public class QuerySourceTracker extends SearchComponent {

    private static final Logger LOG = LoggerFactory.getLogger(QuerySourceTracker.class);
    private static final MetricRegistry metrics = new MetricRegistry();
    final Map<String, Meter> metricsMap = new HashMap<>();


    private NamedList<?> initParams;
    private Set<String> queryIdentifiers = new HashSet<>();
    private boolean collectMetrics = false;
    private boolean failQueries = false;

    @Override
    @SuppressWarnings("unchecked")
    public void init(NamedList args) {
        LOG.info("Loading the BlockQueryComponent instance");
        super.init(args);
        this.initParams = args;

        if (args != null) {
            Object o = args.get("queryIdentifiers");
            if (o != null && o instanceof NamedList) {
                List<String> qis = ((NamedList)o).getAll("qi");
                LOG.info("Valid query identifiers {}", qis);
                queryIdentifiers.addAll(qis);
                Boolean collMetrics = args.getBooleanArg("collectMetrics");
                if (collMetrics != null) {
                    collectMetrics = collMetrics.booleanValue();
                }
                if (collectMetrics) {
                    for (String qi : queryIdentifiers) {
                        metricsMap.put(qi, metrics.meter(qi + "_queries_meter"));
                    }
                    LOG.info("Starting the JMX reporter");
                    startJMXReporting(metrics);
                }
                Boolean fQueries = args.getBooleanArg("failQueries");
                if (fQueries != null) {
                    failQueries = fQueries.booleanValue();
                }
            }
        }
    }

    @Override
    public String getDescription() {
        return "QuerySourceTracker component to track the source of the queries";
    }

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        SolrQueryRequest req = rb.req;
        SolrParams params = req.getParams();
        if (failQueries) {
            String qi = params.get("qi");
            if (qi != null) {
                if (!queryIdentifiers.contains(qi)) {
                    throw new IOException("In order to perform queries please provide a valid \"qi\" query param as part of the request, provided qi param " + qi + " is not valid");
                }
            } else {
                throw new IOException("In order to perform queries please provide a valid \"qi\" query param as part of the request");
            }
        }
        HttpServletRequest httpRequest = (HttpServletRequest) req.getContext().get("httpRequest");
        String clientIp = getClientIp(httpRequest);
        rb.rsp.addToLog("clientIp", clientIp);
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
    }

    @Override
    public void finishStage(ResponseBuilder rb) {
        LOG.debug("Finishing the QuerySourceTracker component, stage = {}", rb.stage);
        SolrQueryRequest req = rb.req;
        SolrParams params = req.getParams();
        String qi = params.get("qi");
        if (collectMetrics && qi != null) {
            boolean isDistributed = req.getParams().getBool("distrib", true);
            if (isDistributed && rb.stage >= ResponseBuilder.STAGE_GET_FIELDS) {
                if (queryIdentifiers.contains(qi)) {
                    metricsMap.get(qi).mark();
                }
            } else {
                LOG.debug("Not incrementing the meter" + isDistributed + "," + rb.stage);
            }
        }
    }

    private void startJMXReporting(MetricRegistry registry) {
        final JmxReporter reporter = JmxReporter.forRegistry(registry).build();
        reporter.start();
    }

    private String getClientIp(HttpServletRequest request) {
        String remoteAddr = "";
        if (request != null) {
            remoteAddr = request.getHeader("X-FORWARDED-FOR");
            if (remoteAddr == null || "".equals(remoteAddr)) {
                remoteAddr = request.getRemoteAddr();
            }
        }
        return remoteAddr;
    }
}
