The Clustering contrib plugin for Solr provides a generic mechanism for plugging in third party clustering implementations.
It currently provides clustering support for search results using the Carrot2 project.

See http://wiki.apache.org/solr/ClusteringComponent for how to get started.

Also, note, some of the Carrot2 libraries cannot be distributed in binary form because they are LGPL.  Thus, you will have
to download those components.  See the build.xml file located in this directory for the location of the libraries.
The libraries you will need are: nni.jar, Colt, PNJ and simple-xml.
