<?xml version="1.0" encoding="UTF-8"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<xsl:stylesheet version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:str="http://exslt.org/strings"
  extension-element-prefixes="str"
>
  <xsl:param name="eclipse.fileset.sourcefolders"/>
  <xsl:param name="eclipse.fileset.libs"/>
  <xsl:param name="eclipse.fileset.webfolders"/>
  
  <!--
    NOTE: This template matches the root element of any given input XML document!
    The XSL input file is ignored completely.
  --> 
  <xsl:template match="/">
    <classpath>
      <xsl:for-each select="str:split($eclipse.fileset.sourcefolders,'|')">
        <!-- hack to sort the list, starts-with() returns "true" which sorts before "false" if descending: -->
        <xsl:sort select="string(starts-with(text(), 'lucene/core/'))" order="descending" lang="en"/>
        <xsl:sort select="string(starts-with(text(), 'lucene/test-framework/'))" order="descending" lang="en"/>
        <xsl:sort select="string(starts-with(text(), 'lucene/'))" order="descending" lang="en"/>
        <xsl:sort select="string(starts-with(text(), 'solr/core/'))" order="descending" lang="en"/>
        <xsl:sort select="string(starts-with(text(), 'solr/solrj/'))" order="descending" lang="en"/>
        <xsl:sort select="string(starts-with(text(), 'solr/test-framework/'))" order="descending" lang="en"/>
        <xsl:sort select="string(starts-with(text(), 'solr/'))" order="descending" lang="en"/>
        <!-- all others in one group above are sorted by path name: -->
        <xsl:sort select="text()" order="ascending" lang="en"/>
        
        <classpathentry kind="src" path="{.}">
          <!-- make Lucene's resource folders unique (for SPI), but leave the main SPI in default target folder: -->
          <xsl:if test="starts-with(.,'lucene/') and not(starts-with(.,'lucene/core')) and contains(.,'/src/resources')">
            <xsl:attribute name="output">
              <xsl:text>eclipse-build/</xsl:text><xsl:value-of select="position()"/>
            </xsl:attribute>
          </xsl:if>
        </classpathentry>
        <!-- special case for benchmark, we add extra entry after the tests: -->
        <xsl:if test="text()='lucene/benchmark/src/test'">
          <classpathentry excluding="src" including="conf/**" kind="src" path="lucene/benchmark"/>
        </xsl:if>
      </xsl:for-each>

      <xsl:for-each select="str:split($eclipse.fileset.webfolders,'|')">
        <xsl:sort select="text()" order="ascending" lang="en"/>
        <classpathentry kind="src" path="{.}">
          <xsl:attribute name="output">
            <xsl:choose>
              <xsl:when test="contains(.,'solr/webapp/web')">
                <xsl:text>eclipse-build/solr-server/solr-webapp/webapp</xsl:text>
              </xsl:when>
              <xsl:otherwise>
                <xsl:text>eclipse-build/solr-server/</xsl:text><xsl:value-of select="substring(text(), 13)"/>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:attribute>
        </classpathentry>
      </xsl:for-each>

      <!-- the main resources folder is here (see above), so it's listed after the test-framework resources, making preflex-override work: -->
      <classpathentry kind="output" path="eclipse-build/main"/>
      <classpathentry kind="con" path="org.eclipse.jdt.launching.JRE_CONTAINER/org.eclipse.jdt.internal.debug.ui.launcher.StandardVMType/JavaSE-11"/>

      <xsl:for-each select="str:split($eclipse.fileset.libs,'|')">
        <!-- sort the jars by path name: -->
        <xsl:sort select="text()" order="ascending" lang="en"/>
        <classpathentry kind="lib" path="{.}"/>
      </xsl:for-each>
    </classpath>
  </xsl:template>

</xsl:stylesheet>
