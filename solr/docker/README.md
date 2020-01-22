# Docker image build from source

Solr's official docker images are based on officially released artifacts, but sometimes it is useful to produce Docker images for testing during development. This is what this Dockerfile is for.

The Dockerfile is a simplified version of the Dockerfile in [docker-solr](https://github.com/docker-solr/docker-solr) project since we can skip lots of downloading, checksum validations etc.

TODO:

* Make a gradle task for building the docker image
* It will need to first build tgz and copy it into `solr/docker/` folder
* Investigate whether we instead should compile the image from non-tar build folder?
* Gradle task for publishing snapshot image to a new solr folder under https://hub.docker.com/u/apache