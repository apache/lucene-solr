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


<!-- 
  Display the luke request handler with graphs
 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns="http://www.w3.org/1999/xhtml" xmlns:svg="http://www.w3.org/2000/svg" version="1.0">
    <xsl:output method="xml" doctype-public="-//W3C//DTD XHTML 1.0 Strict//EN"
        doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd" encoding="UTF-8"/>

    <xsl:variable name="title">Solr Luke Request Handler Response</xsl:variable>

    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml">
            <head>
                <title>
                    <xsl:value-of select="$title"/>
                </title>
                <!-- <xsl:call-template name="svg_ie_workaround"/> -->
                <xsl:call-template name="css"/>
                <meta http-equiv="Content-Type" content="application/xhtml+xml; charset=UTF-8"/>
            </head>
            <body>
                <h1>
                    <xsl:value-of select="$title"/>
                </h1>
                <div class="doc">
                    <ul>
                        <xsl:if test="response/lst[@name='index']">
                            <li>
                                <a href="#index">Index Statistics</a>
                            </li>
                        </xsl:if>
                        <xsl:if test="response/lst[@name='fields']">
                            <li>
                                <a href="#fields">Field Statistics</a>
                            </li>
                            <li>
                                <ul>
                                    <xsl:for-each select="response/lst[@name='fields']/lst">
                                        <li>
                                            <a href="#{@name}">
                                                <xsl:value-of select="@name"/>
                                            </a>
                                        </li>
                                    </xsl:for-each>
                                </ul>
                            </li>
                        </xsl:if>
                        <xsl:if test="response/lst[@name='doc']">
                            <li>
                                <a href="#doc">Document statistics</a>
                            </li>
                        </xsl:if>
                    </ul>
                </div>
                <xsl:if test="response/lst[@name='index']">
                    <h2><a name="index"/>Index statistics</h2>
                    <xsl:apply-templates select="response/lst[@name='index']"/>
                </xsl:if>
                <xsl:if test="response/lst[@name='fields']">
                    <h2><a name="fields"/>Field statistics</h2>
                    <xsl:apply-templates select="response/lst[@name='fields']"/>
                </xsl:if>
                <xsl:if test="response/lst[@name='doc']">
                    <h2><a name="doc"/>Document statistics</h2>
                    <xsl:apply-templates select="response/lst[@name='doc']"/>
                </xsl:if>
            </body>
        </html>
    </xsl:template>

    <xsl:template match="lst">
        <xsl:if test="parent::lst">
            <tr>
                <td colspan="2">
                    <div class="doc">
                        <xsl:call-template name="list"/>
                    </div>
                </td>
            </tr>
        </xsl:if>
        <xsl:if test="not(parent::lst)">
            <div class="doc">
                <xsl:call-template name="list"/>
            </div>
        </xsl:if>
    </xsl:template>

    <xsl:template name="list">
        <xsl:if test="count(child::*)>0">
            <table>
                <thead>
                    <tr>
                        <th colspan="2">
                            <p>
                                <a name="{@name}"/>
                            </p>
                            <xsl:value-of select="@name"/>
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <xsl:choose>
                        <xsl:when
                            test="@name='histogram' and not(system-property('xsl:vendor')='Microsoft')">
                            <tr>
                                <td colspan="2">
                                    <xsl:call-template name="histogram"/>
                                </td>
                            </tr>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:apply-templates/>
                        </xsl:otherwise>
                    </xsl:choose>
                </tbody>
            </table>
        </xsl:if>
    </xsl:template>

    <xsl:template name="histogram">
        <div class="doc">
            <xsl:call-template name="barchart">
                <xsl:with-param name="xoffset">5</xsl:with-param>
                <xsl:with-param name="yoffset">5</xsl:with-param>
                <xsl:with-param name="iwidth">800</xsl:with-param>
                <xsl:with-param name="iheight">600</xsl:with-param>
                <xsl:with-param name="fill">blue</xsl:with-param>
            </xsl:call-template>
        </div>
    </xsl:template>

    <xsl:template name="barchart">
        <xsl:param name="xoffset"/>
        <xsl:param name="yoffset"/>
        <xsl:param name="iwidth"/>
        <xsl:param name="iheight"/>
        <xsl:param name="fill"/>
        <svg:svg viewBox="0 0 {$iwidth} {$iheight}">
            <xsl:if test="system-property('xsl:vendor')='Opera' or system-property('xsl:vendor')='libxslt'">
                <xsl:attribute name="width"><xsl:value-of select="$iwidth"/></xsl:attribute>
                <xsl:attribute name="height"><xsl:value-of select="$iwidth"/></xsl:attribute>
            </xsl:if>
            <xsl:variable name="x" select="$xoffset + 5"/>
            <xsl:variable name="y" select="$yoffset + 5"/>
            <xsl:variable name="width" select="$iwidth - 10"/>
            <xsl:variable name="height" select="$iheight - 30"/>
            <xsl:variable name="max">
                <xsl:for-each select="int">
                    <xsl:sort data-type="number" order="descending"/>
                    <xsl:if test="position()=1">
                        <xsl:value-of select="."/>
                    </xsl:if>
                </xsl:for-each>
            </xsl:variable>
            <xsl:variable name="yRatio" select="$height div $max"/>
            <xsl:variable name="xRatio" select="$width div count(int)"/>
            <svg:g>
                <xsl:for-each select="int">
                    <svg:rect stroke="none" x="{$x + (position() - 1) * $xRatio + 0.1 * $xRatio}"
                        y="{($y + $height) - number(.) * $yRatio}" width="{0.8 * $xRatio}"
                        height="{number(.) * $yRatio}" fill="{$fill}"/>
                    <xsl:variable name="yboost">
                        <xsl:choose>
                            <xsl:when
                                test="($y + $height) - number(.) * $yRatio +40 &gt; $iheight"
                                >-25</xsl:when>
                            <xsl:otherwise>0</xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>
                    <svg:text
                        x="{$x + (position() - 1) * $xRatio + 0.1 * $xRatio + (($xRatio * 0.8) div 2)}"
                        y="{($y + $height) - number(.) * $yRatio +20 + number($yboost)}"
                        text-anchor="middle" style="fill: red; font-size: 8px;">
                        <xsl:value-of select="."/>
                    </svg:text>
                    <svg:text
                        x="{$x + (position() - 1) * $xRatio + 0.1 * $xRatio + (($xRatio * 0.8) div 2)}"
                        y="{$y + $height + 20}" text-anchor="middle" style="fill: black; font-size: 8px;">
                        <xsl:value-of select="@name"/>
                    </svg:text>
                </xsl:for-each>
            </svg:g>
        </svg:svg>
    </xsl:template>

    <xsl:template name="keyvalue">
        <xsl:choose>
            <xsl:when test="@name">
                <tr>
                    <td class="name">
                        <xsl:value-of select="@name"/>
                    </td>
                    <td class="value">
                        <xsl:value-of select="."/>
                    </td>
                </tr>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="."/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="int|bool|long|float|double|uuid|date">
        <xsl:call-template name="keyvalue"/>
    </xsl:template>

    <xsl:template match="arr">
        <tr>
            <td class="name">
                <xsl:value-of select="@name"/>
            </td>
            <td class="value">
                <ul>
                    <xsl:for-each select="child::*">
                        <li>
                            <xsl:apply-templates/>
                        </li>
                    </xsl:for-each>
                </ul>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="str">
        <xsl:choose>
            <xsl:when test="@name='schema' or @name='index' or @name='flags'">
                <xsl:call-template name="schema"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:call-template name="keyvalue"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="schema">
        <tr>
            <td class="name">
                <xsl:value-of select="@name"/>
            </td>
            <td class="value">
                <xsl:if test="contains(.,'unstored')">
                    <xsl:value-of select="."/>
                </xsl:if>
                <xsl:if test="not(contains(.,'unstored'))">
                    <xsl:call-template name="infochar2string">
                        <xsl:with-param name="charList">
                            <xsl:value-of select="."/>
                        </xsl:with-param>
                    </xsl:call-template>
                </xsl:if>
            </td>
        </tr>
    </xsl:template>

    <xsl:template name="infochar2string">
        <xsl:param name="i">1</xsl:param>
        <xsl:param name="charList"/>

        <xsl:variable name="char">
            <xsl:value-of select="substring($charList,$i,1)"/>
        </xsl:variable>
        <xsl:choose>
            <xsl:when test="$char='I'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='I']"/> - </xsl:when>
            <xsl:when test="$char='T'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='T']"/> - </xsl:when>
            <xsl:when test="$char='S'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='S']"/> - </xsl:when>
            <xsl:when test="$char='M'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='M']"/> - </xsl:when>
            <xsl:when test="$char='V'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='V']"/> - </xsl:when>
            <xsl:when test="$char='o'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='o']"/> - </xsl:when>
            <xsl:when test="$char='p'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='p']"/> - </xsl:when>
            <xsl:when test="$char='O'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='O']"/> - </xsl:when>
            <xsl:when test="$char='L'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='L']"/> - </xsl:when>
            <xsl:when test="$char='B'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='B']"/> - </xsl:when>
            <xsl:when test="$char='C'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='C']"/> - </xsl:when>
            <xsl:when test="$char='f'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='f']"/> - </xsl:when>
            <xsl:when test="$char='l'">
                <xsl:value-of select="/response/lst[@name='info']/lst/str[@name='l']"/> -
            </xsl:when>
        </xsl:choose>

        <xsl:if test="not($i>=string-length($charList))">
            <xsl:call-template name="infochar2string">
                <xsl:with-param name="i">
                    <xsl:value-of select="$i+1"/>
                </xsl:with-param>
                <xsl:with-param name="charList">
                    <xsl:value-of select="$charList"/>
                </xsl:with-param>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
    <xsl:template name="css">
        <style type="text/css">
            <![CDATA[
            body { font-family: "Lucida Grande", sans-serif }
            td.name {font-style: italic; font-size:80%; }
            th { font-style: italic; font-size: 80%; background-color: lightgrey; }
            td { vertical-align: top; }
            ul { margin: 0px; margin-left: 1em; padding: 0px; }
            table { width: 100%; border-collapse: collapse; }
            .note { font-size:80%; }
            .doc { margin: 0.5em; border: solid grey 1px; }
            .exp { display: none; font-family: monospace; white-space: pre; }
            ]]>
        </style>
    </xsl:template>
    <xsl:template name="svg_ie_workaround">
        <xsl:if test="system-property('xsl:vendor')='Microsoft'">
            <object id="AdobeSVG" classid="clsid:78156a80-c6a1-4bbf-8e6a-3cd390eeb4e2"/>
            <xsl:processing-instruction name="import">namespace="svg"
            implementation="#AdobeSVG"</xsl:processing-instruction>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
