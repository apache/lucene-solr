package org.apache.solr.cluster.placement;

import java.util.Optional;

public interface AttributeValues {
    /** For the given node: number of cores */
    Optional<Integer> getCoresCount(Node node);

    /** For the given node: Hardware type of the disk partition where cores are stored */
    Optional<AttributeFetcher.DiskHardwareType> getDiskType(Node node);

    /** For the given node: Free disk size in Gigabytes of the partition on which cores are stored */
    Optional<Long> getFreeDisk(Node node);

    /** For the given node: Total disk size in Gigabytes of the partition on which cores are stored */
    Optional<Long> getTotalDisk(Node node);

    /** For the given node: Percentage between 0 and 100 of used heap over max heap */
    Optional<Double> getHeapUsage(Node node);

    /** For the given node: matches {@link java.lang.management.OperatingSystemMXBean#getSystemLoadAverage()} */
    Optional<Double> getSystemLoadAverage(Node node);

    /** For the given node: system property value (system properties are passed to Java using {@code -Dname=value} */
    Optional<String> getSystemProperty(Node node, String name);

    /** For the given node: environment variable value */
    Optional<String> getEnvironmentVariable(Node node, String name);

    /** For the given node: metric of specific name and registry */
    Optional<Double> getMetric(Node node, String metricName, AttributeFetcher.NodeMetricRegistry registry);


    /** Get a non node related metric of specific scope and name */
    Optional<Double> getMetric(String scope, String metricName);
}
