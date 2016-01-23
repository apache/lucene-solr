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
<%-- do a verbatim include so we can use the local vars --%>
<%@include file="_info.jsp"%>
<html>
<head>
<title>DataImportHandler Interactive Development</title>
<link rel="stylesheet" type="text/css" href="solr-admin.css">
<link rel="icon" href="favicon.ico" type="image/ico"></link>
<link rel="shortcut icon" href="favicon.ico" type="image/ico"></link>
<script src="jquery-1.2.3.min.js"></script>
</head>
<body>
<h1>DataImportHandler Development Console</h1>
<%
  String handler = request.getParameter("handler");  // must be specified
%>
<form action="../select" target="result" method="get">
<input type="hidden" name="debug" value="on"/>
<input type="hidden" name="qt" value="<%=handler%>"/>
<table>
  <tr>
    <th>Handler: </th>
    <td><%=handler%> <a href="dataimport.jsp" target="_top">change handler</a></td>
  </tr>
	<tr>
		<td colspan="2">
		<table width="100%">
			<tr>
				<td>
					<select name="command">
						<option value="full-import" selected="selected">full-import</option>
						<option value="delta-import">delta-import</option>
					</select>
				</td>
				<td><strong>Verbose</strong>&nbsp;<input
					name="verbose" type="checkbox"></td>
				<td><strong>Commit</strong>&nbsp;<input
					name="commit" type="checkbox"></td>
				<td><strong>Clean</strong>&nbsp;<input
					name="clean" type="checkbox"></td>
				<td><strong>Start Row</strong>&nbsp;<input
					name="start" size="4" type="text" value="0"></td>
				<td><strong>No. of Rows</strong>&nbsp;<input name="rows"
					type="text" size="4" value="10"></td>
			</tr>
		</table>
		</td>
	<tr>
		<td><strong>data config xml</strong></td>
		<td><input class="stdbutton" type="submit" value="debug now">
		</td>
	</tr>
	<tr>
		<td colspan="2"><textarea id="txtDataConfig" rows="30" cols="80" name="dataConfig"></textarea></td>
    <script type="text/javascript" language="javascript">
        $.get("../select?qt=<%=handler%>&command=show-config", function(data){
            $('#txtDataConfig').attr('value', data);
        });
    </script>
	</tr>
</table>
</form>
<form action="../select" target="result" method="get">
	<input type="hidden" name="clean" value="false">
	<input type="hidden" name="commit" value="true">
  <input type="hidden" name="qt" value="<%=handler%>"/>
	<input class="stdbutton" type="submit" name="command" value="full-import">
	<input class="stdbutton" type="submit" name="command" value="delta-import">
	<input class="stdbutton" type="submit" name="command" value="status">
	<input class="stdbutton" type="submit" name="command" value="reload-config">
	<input class="stdbutton" type="submit" name="command" value="abort">
</form>
<form action="../select" target="result" method="get">
	<input type="hidden" name="q" value="*:*">
	<input type="hidden" name="start" value="0">
	<input type="hidden" name="rows" value="0">
	<input class="stdbutton" type="submit" value="Documents Count">
</form>
<form action="../select" target="result" method="get">
  <input type="hidden" name="qt" value="<%=handler%>"/>
	<input type="hidden" name="verbose" value="true">
	<input type="hidden" name="clean" value="true">
	<input type="hidden" name="commit" value="true">
	<input type="hidden" name="command" value="full-import">
	<input class="stdbutton" type="submit" value="Full Import with Cleaning">
</form>

<a href="index.jsp" target="_parent">Return to Admin Page</a>
</body>
</html>
