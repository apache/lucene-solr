package org.apache.solr.cluster.events;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Intended for {@link org.apache.solr.core.CoreContainer} plugins that should be
 * enabled only one instance per cluster.
 * <p>Implementation detail: currently these plugins are instantiated on the
 * Overseer leader, and closed when the current node loses its leadership.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ClusterSingleton {
}
