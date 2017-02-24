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
>
  <xsl:param name="version"/>
  <xsl:param name="solrJavadocUrl"/>
  
  <!--
    NOTE: This template matches the root element of any given input XML document!
    The XSL input file is ignored completely, but XSL expects one to be given,
    so build.xml passes itself here. The list of module build.xmls is given via
    string parameter, that must be splitted at '|'.
  --> 
  <xsl:template match="/">
    <html>
      <head>
        <title><xsl:text>Apache Solr </xsl:text><xsl:value-of select="$version"/><xsl:text> Documentation</xsl:text></title>
        <link rel="icon" type="image/x-icon" href="images/favicon.ico"/>
        <link rel="shortcut icon" type="image/x-icon" href="images/favicon.ico"/>
      </head>
      <body>
        <div>
          <a href="http://lucene.apache.org/solr/">
            <img src="images/solr.svg" style="width:210px; margin:22px 0px 7px 20px; border:none;" title="Apache Solr Logo" alt="Solr" />
          </a>
          <div style="z-index:100;position:absolute;top:25px;left:226px">
            <span style="font-size: x-small">TM</span>
          </div>
        </div>
        <h1>
          <xsl:text>Apache Solr</xsl:text>
          <span style="vertical-align: top; font-size: x-small">
            <xsl:text>TM</xsl:text>
          </span>
          <xsl:text> </xsl:text>
          <xsl:value-of select="$version"/>
          <xsl:text> Documentation</xsl:text>
        </h1>
        <p>
          <xsl:choose>
            <xsl:when test="$solrJavadocUrl">
              <a href="{$solrJavadocUrl}">Follow this link to view online documentation for Solr <xsl:value-of select="$version"/>.</a>
            </xsl:when>
            <xsl:otherwise>
              No online documentation available for custom builds or SNAPSHOT versions. Run <code>ant documentation</code> from <code>src.tgz</code> package to build docs locally.
            </xsl:otherwise>
          </xsl:choose>
        </p>
      </body>
    </html>
  </xsl:template>

</xsl:stylesheet>
