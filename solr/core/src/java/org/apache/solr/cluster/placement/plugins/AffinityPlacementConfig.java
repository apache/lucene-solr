package org.apache.solr.cluster.placement.plugins;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.io.IOException;

/**
 *
 */
public class AffinityPlacementConfig implements ReflectMapWriter {

  public static final AffinityPlacementConfig DEFAULT = new AffinityPlacementConfig();

  @JsonProperty
  public long minimalFreeDiskGB;

  @JsonProperty
  public long prioritizedFreeDiskGB;

  // no-arg public constructor required for deserialization
  public AffinityPlacementConfig() {
    minimalFreeDiskGB = 20L;
    prioritizedFreeDiskGB = 100L;
  }

  public AffinityPlacementConfig(long minimalFreeDiskGB, long prioritizedFreeDiskGB) {
    this.minimalFreeDiskGB = minimalFreeDiskGB;
    this.prioritizedFreeDiskGB = prioritizedFreeDiskGB;
  }
}
