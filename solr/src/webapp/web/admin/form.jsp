<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%--
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
--%>
<%@include file="header.jsp" %>

<br clear="all">
<form name="queryForm" method="GET" action="../select" accept-charset="UTF-8">
<!-- these are good defaults to have if people bookmark the resulting
     URLs, but they should not show up in the form since they are very
     output type specific.
  -->
<input name="indent" type="hidden" value="on">
<input name="version" type="hidden" value="2.2">

<table>
<tr>
  <td>
	<strong>Solr/Lucene Statement</strong>
  </td>
  <td>
	<textarea rows="5" cols="60" name="q"><%= defaultSearch %></textarea>
  </td>
</tr>
<tr>
  <td>
	<strong>Filter Query</strong>
  </td>
  <td>
	<textarea rows="2" cols="60" name="fq"></textarea>
  </td>
</tr>
<tr>
  <td>
	<strong>Start Row</strong>
  </td>
  <td>
	<input name="start" type="text" value="0">
  </td>
</tr>
<tr>
  <td>
	<strong>Maximum Rows Returned</strong>
  </td>
  <td>
	<input name="rows" type="text" value="10">
  </td>
</tr>
<tr>
  <td>
	<strong>Fields to Return</strong>
  </td>
  <td>
	<input name="fl" type="text" value="*,score">
  </td>
</tr>
<tr>
  <td>
	<strong>Query Type</strong>
  </td>
  <td>
	<input name="qt" type="text" value="standard">
  </td>
</tr>
<tr>
  <td>
	<strong>Output Type</strong>
  </td>
  <td>
	<input name="wt" type="text" value="standard">
  </td>
</tr>
<tr>
  <td>
	<strong>Debug: enable</strong>
  </td>
  <td>
	<input name="debugQuery" type="checkbox" >
  <em><font size="-1">  Note: you may need to "view source" in your browser to see explain() correctly indented.</font></em>
  </td>
</tr>
<tr>
  <td>
	<strong>Debug: explain others</strong>
  </td>
  <td>
	<input name="explainOther" type="text" >
  <em><font size="-1">  Apply original query scoring to matches of this query to see how they compare.</font></em>
  </td>
</tr>
<tr>
  <td>
	<strong>Enable Highlighting</strong>
  </td>
  <td>
	<input name="hl" type="checkbox" >
  </td>
</tr>
<tr>
  <td>
	<strong>Fields to Highlight</strong>
  </td>
  <td>
	<input name="hl.fl" type="text" >
  </td>
</tr>
<tr>
  <td>
  </td>
  <td>
    <input class="stdbutton" type="submit" value="search" onclick="if (queryForm.q.value.length==0) { alert('no empty queries, please'); return false; } else { queryForm.submit(); } ">
  </td>
</tr>
</table>
</form>
<br clear="all">
<em>
This form demonstrates the most common query options available for the
built in Query Types.  Please consult the Solr Wiki for additional
Query Parameters.
</em>

</body>
</html>
