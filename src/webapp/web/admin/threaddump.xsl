<?xml version="1.0" encoding="utf-8"?>
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
<!-- $Id$ -->
<!-- $URL$ -->

<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">

  <xsl:output
    method="html"
    indent="yes"
    encoding="utf-8"
    media-type="text/html; charset=UTF-8"
    doctype-public="-//W3C//DTD HTML 4.01//EN"
    doctype-system="http://www.w3.org/TR/html4/strict.dtd" />

  <xsl:template match="/">
    <html>
      <head>
        <link rel="stylesheet" type="text/css" href="solr-admin.css"></link>
        <link rel="icon" href="favicon.ico" type="image/ico"></link>
        <link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
        <title>SOLR Info</title>
      </head>
      <body>
        <a href=".">
          <img border="0" align="right" height="78" width="142" src="solr_small.png" alt="SOLR"/>
        </a>
        <h1>Solr Admin (<xsl:value-of select="solr/meta/collection" />)</h1>
        <h2>Thread Dump</h2>
        <div style="margin-top: 1em;">
          <table>
            <xsl:apply-templates/>
          </table>
          <a href=".">Return to Admin Page</a>
        </div>
      </body>
    </html>
  </xsl:template>

  <xsl:include href="meta.xsl"/>

  <xsl:template match="solr/system/jvm">
    <tr>
      <td><xsl:value-of select="name"/> <xsl:value-of select="version"/></td>
    </tr>
  </xsl:template>

  <xsl:template match="solr/system/threadCount">
    <tr>
      <td>
        Thread Count:
        current=<xsl:value-of select="current"/>,
        peak=<xsl:value-of select="peak"/>,
        daemon=<xsl:value-of select="daemon"/></td>
    </tr>
  </xsl:template>

  <xsl:template match="solr/system/threadDump">
    <div>Full Thread Dump:</div>
    <xsl:for-each select="thread">
      <!-- OG: TODO: add suspended/native conditionals -->
      <tr>
        <td style="margin-left: 1em; font-weight: bold;">
          '<xsl:value-of select="name"/>' 
          Id=<xsl:value-of select="id"/>, 
          <xsl:value-of select="state"/> 
          on lock=<xsl:value-of select="lock"/>, 
          total cpu time=<xsl:value-of select="cpuTime"/> 
          user time=<xsl:value-of select="userTime"/>
        </td>
      </tr>
      <xsl:apply-templates select="stackTrace"/>
    </xsl:for-each>
  </xsl:template>

  <xsl:template match="stackTrace">
    <tr>
      <td style="margin-left: 1em;">
        <xsl:for-each select="line">
          <xsl:value-of select="."/><br/>
        </xsl:for-each>
      </td>
    </tr>
  </xsl:template>

</xsl:stylesheet>
