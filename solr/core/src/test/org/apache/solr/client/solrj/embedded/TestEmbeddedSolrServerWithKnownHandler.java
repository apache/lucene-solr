import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

public class TestEmbeddedSolrServerWithKnownHandler extends SolrTestCaseJ4 {

    @Test
    public void testPathIsAddedToContext() throws IOException, SolrServerException {
        final Path path = createTempDir();

        final SolrResourceLoader loader = new SolrResourceLoader(path);
        final NodeConfig config = new NodeConfig.NodeConfigBuilder("testnode", loader)
                .setConfigSetBaseDirectory(Paths.get(TEST_HOME()).resolve("configsets").toString())
                .build();

        try (final EmbeddedSolrServer server = new EmbeddedSolrServer(config, "collection1")) {
            final SystemInfoRequest info = new SystemInfoRequest();
            final NamedList response = server.request(info);
            assertTrue(response.size() > 0);
        }
    }

    private static class SystemInfoRequest extends SolrRequest<QueryResponse> {

        public SystemInfoRequest() {
            super(METHOD.GET, "/admin/info/system");
        }

        @Override
        public SolrParams getParams() {
            return new ModifiableSolrParams();
        }

        @Override
        public Collection<ContentStream> getContentStreams() throws IOException {
            return null;
        }

        @Override
        protected QueryResponse createResponse(final SolrClient client) {
            return new QueryResponse();
        }
    }

}
