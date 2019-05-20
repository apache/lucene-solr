# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Pull base image.
FROM ubuntu:18.10

# Define working directory.
WORKDIR /home/lucene

# Define default command.
CMD ["bash"]

# We actually pre install some stuff the script would install for you
# We count on end-to-end-test-mode to test that
RUN \
  apt-get update && \
  apt-get install -y openjdk-11-jdk && \
  apt-get install -y python

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/

RUN export JAVA_HOME
  