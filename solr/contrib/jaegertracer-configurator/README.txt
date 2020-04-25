Welcome to Apache Solr Jaeger Tracer Configurator
========

Apache Solr Jaeger Tracer Configurator (solr-jaegertracer) provides a way for you to expose Solr's tracing to Jaeger.

# Setup Jaeger Tracer Configurator

Note that all library of solr-jaegertracer must be included in the classpath of all nodes then Jaeger tracer can be setup in `solr.xml` like this:

```
<tracerConfig name="tracerConfig" class="org.apache.solr.jaeger.JaegerTracerConfigurator">
  <str name="agentHost">localhost</str>
  <int name="agentPort">5775</int>
  <bool name="logSpans">true</bool>
  <int name="flushInterval">1000</int>
  <int name="maxQueueSize">10000</int>
</tracerConfig>
```

List of parameters for JaegerTracerConfigurator include:
|Parameter|Type|Required|Default|Description|
|---------|----|--------|-------|-----------|
|agentHost|string|Yes||The host of Jaeger backend|
|agentPort|int|Yes||The port of Jaeger port|
|logsSpans|bool|No|true|Whether the tracer should also log the spans|
|flushInterval|int|No|5000|The tracer's flush interval (ms)|
|maxQueueSize|int|No|10000|The tracer's maximum queue size|

Other parameters which are not listed above can be configured using System Properties or Environment Variables. The full list are listed at [Jaeger-README](https://github.com/jaegertracing/jaeger-client-java/blob/master/jaeger-core/README.md).

By default the sampling rate is 0.1%, this value can be changed by updating
key `samplePercentage` of cluster properties. I.e: `/admin/collections?action=CLUSTERPROP&name=propertyName&samplePercentage=100`.