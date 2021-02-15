package org.apache.solr.client.solrj.impl;

import org.apache.solr.SolrTestCase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class PreemptiveBasicAuthClientBuilderFactoryTest extends SolrTestCase {

    private void assertIllegalArgumentException(ThrowingRunnable executable, String expectedMessage) {
        Exception e = expectThrows(IllegalArgumentException.class, executable);
        assertTrue("Expecting message to contain \"" + expectedMessage + "\" but was: " + e.getMessage(), e.getMessage().contains(expectedMessage));
    }

    @Override
    public void tearDown() throws Exception {
        System.clearProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS);
        System.clearProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_HTTP_CLIENT_CONFIG);
        super.tearDown();
    }

    public void testBadSysPropsCredentials() {
        System.setProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS, "foo");
        assertIllegalArgumentException(PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver::new, "Invalid Authentication credentials");
        System.setProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS, "foo:");
        assertIllegalArgumentException(PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver::new, "Invalid Authentication credentials");
        System.setProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS, ":foo");
        assertIllegalArgumentException(PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver::new, "Invalid Authentication credentials");
    }

    public void testSysPropsAndPropsFile() {
        System.setProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS, "foo");
        System.setProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_HTTP_CLIENT_CONFIG, "foo");
        assertIllegalArgumentException(PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver::new, "Basic authentication credentials passed");

    }

    public void testCredentialsFromSystemProperties() {
        System.setProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS, "foo:bar");
        PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver credentialsResolver = new PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver();
        assertEquals("foo", credentialsResolver.defaultParams.get(HttpClientUtil.PROP_BASIC_AUTH_USER));
        assertEquals("bar", credentialsResolver.defaultParams.get(HttpClientUtil.PROP_BASIC_AUTH_PASS));
    }

    public void testCredentialsFromConfigFile() throws IOException {
        Properties p = new Properties();
        p.setProperty("httpBasicAuthUser", "foo");
        p.setProperty("httpBasicAuthPassword", "bar");
        File f = createTempFile().toFile();
        try (FileWriter fw = new FileWriter(f, StandardCharsets.UTF_8)) {
            p.store(fw, "tmp properties file for PreemptiveBasicAuthClientBuilderFactoryTest.testCredentialsFromConfigFile");
        }
        System.setProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_HTTP_CLIENT_CONFIG, f.getAbsolutePath());
        PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver credentialsResolver = new PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver();
        assertEquals("foo", credentialsResolver.defaultParams.get(HttpClientUtil.PROP_BASIC_AUTH_USER));
        assertEquals("bar", credentialsResolver.defaultParams.get(HttpClientUtil.PROP_BASIC_AUTH_PASS));
    }
}
