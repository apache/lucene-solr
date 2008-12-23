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
        <a href="">
          <img border="0" align="right" height="78" width="142" src="solr_small.png" alt="SOLR"/>
  </a>
        <h1>Solr Admin (<xsl:value-of select="solr/meta/collection" />)</h1>
        <div style="margin-top: 1em;">
          <h2>Field Analysis</h2>
          <xsl:apply-templates/>
          <a href=".">Return to Admin Page</a>
        </div>
      </body>
    </html>
  </xsl:template>

  <xsl:include href="meta.xsl"/>

  <xsl:template match="solr/analysis/form">
  <form method="POST" action="analysis.jsp">
    <table>
      <tr>
        <td>
        <strong>Field name</strong>
        </td>
        <td>
          <input class="std" name="name" type="text" value="{field}"/>
        </td>
      </tr>
      <tr>
        <td>
        <strong>Field value (Index)</strong>
        <br/>
        verbose output <input name="verbose" type="checkbox" checked="true"/>
        <br/>
        highlight matches <input name="highlight" type="checkbox" checked="true"/>
        </td>
        <td>
        <textarea class="std" rows="8" cols="70" name="val"><xsl:value-of select="fieldIndexValue" /></textarea>
        </td>
      </tr>
      <tr>
        <td>
        <strong>Field value (Query)</strong>
        <br/>
        verbose output <input name="qverbose" type="checkbox" checked="true"/>
        </td>
        <td>
        <textarea class="std" rows="1" cols="70" name="qval"><xsl:value-of select="fieldQueryValue" /></textarea>
        </td>
      </tr>
      <tr>
        <td>
        </td>
        <td>
          <input class="stdbutton" type="submit" value="analyze"/>
        </td>
      </tr>
    </table>
  </form>
</xsl:template>

<xsl:template match="solr/analysis/results/indexAnalyzer">
  <h4>Index Analyzer</h4>
  <xsl:for-each select="factory">
    <h5 style="margin-left: 1em;"><xsl:apply-templates select="@class"/></h5>
    <xsl:apply-templates/>
  </xsl:for-each>
</xsl:template>

<xsl:template match="solr/analysis/results/indexAnalyzer/factory/args">
  <div style="margin-left: 2em; font-weight: bold;">{
  <xsl:for-each select="arg">
    <xsl:apply-templates select="@name"/>=<xsl:value-of select="."/>, 
  </xsl:for-each>
  }</div>
</xsl:template>

<xsl:template match="solr/analysis/results/indexAnalyzer/factory/tokens">
<div style="margin-left: 2em;">
  <table width="auto" class="analysis" border="1">
    <tr>
      <th>text</th>
      <th>type</th>
      <th>position</th>
      <th>start</th>
      <th>end</th>
    </tr>
  <xsl:for-each select="token">
    <tr>
      <td><xsl:value-of select="."/></td>
      <td><xsl:apply-templates select="@type"/></td>
      <td><xsl:apply-templates select="@pos"/></td>
      <td><xsl:apply-templates select="@start"/></td>
      <td><xsl:apply-templates select="@end"/></td>
    </tr>
  </xsl:for-each>
  </table>
</div>
</xsl:template>

<xsl:template match="solr/analysis/results/queryAnalyzer">
  <h4>Query Analyzer</h4>
  <xsl:for-each select="factory">
    <h5 style="margin-left: 1em;"><xsl:apply-templates select="@class"/></h5>
    <xsl:apply-templates/>
  </xsl:for-each>
</xsl:template>

<xsl:template match="solr/analysis/results/queryAnalyzer/factory/args">
  <div style="margin-left: 2em; font-weight: bold;">{
  <xsl:for-each select="arg">
    <xsl:apply-templates select="@name"/>=<xsl:value-of select="."/>, 
  </xsl:for-each>
  }</div>
</xsl:template>

<xsl:template match="solr/analysis/results/queryAnalyzer/factory/tokens">
<div style="margin-left: 2em;">
  <table width="auto" class="analysis" border="1">
    <tr>
      <th>text</th>
      <th>type</th>
      <th>position</th>
      <th>start</th>
      <th>end</th>
    </tr>
  <xsl:for-each select="token">
    <tr>
      <td><xsl:value-of select="."/></td>
      <td><xsl:apply-templates select="@type"/></td>
      <td><xsl:apply-templates select="@pos"/></td>
      <td><xsl:apply-templates select="@start"/></td>
      <td><xsl:apply-templates select="@end"/></td>
    </tr>
  </xsl:for-each>
  </table>
</div>
</xsl:template>

</xsl:stylesheet>
