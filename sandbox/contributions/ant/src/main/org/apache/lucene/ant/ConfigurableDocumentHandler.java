package org.apache.lucene.ant;

import java.util.Properties;

public interface ConfigurableDocumentHandler extends DocumentHandler {
    void configure(Properties props);
}
