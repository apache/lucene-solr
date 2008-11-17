/**
	@constructor
*/
JSDOC.JsPlate = function(templateFile) {
	if (templateFile) this.template = IO.readFile(templateFile);
	
	this.templateFile = templateFile;
	this.code = "";
	this.parse();
}

JSDOC.JsPlate.prototype.parse = function() {
	this.template = this.template.replace(/\{#[\s\S]+?#\}/gi, "");
	this.code = "var output=``"+this.template;

	this.code = this.code.replace(
		/<for +each="(.+?)" +in="(.+?)" *>/gi, 
		function (match, eachName, inName) {
			return "``;\rvar $"+eachName+"_keys = keys("+inName+");\rfor(var $"+eachName+"_i = 0; $"+eachName+"_i < $"+eachName+"_keys.length; $"+eachName+"_i++) {\rvar $"+eachName+"_last = ($"+eachName+"_i == $"+eachName+"_keys.length-1);\rvar $"+eachName+"_key = $"+eachName+"_keys[$"+eachName+"_i];\rvar "+eachName+" = "+inName+"[$"+eachName+"_key];\routput+=``";
		}
	);	
	this.code = this.code.replace(/<if test="(.+?)">/g, "``;\rif ($1) { output+=``");
	this.code = this.code.replace(/<elseif test="(.+?)"\s*\/>/g, "``;}\relse if ($1) { output+=``");
	this.code = this.code.replace(/<else\s*\/>/g, "``;}\relse { output+=``");
	this.code = this.code.replace(/<\/(if|for)>/g, "``;\r};\routput+=``");
	this.code = this.code.replace(
		/\{\+\s*([\s\S]+?)\s*\+\}/gi,
		function (match, code) {
			code = code.replace(/"/g, "``"); // prevent qoute-escaping of inline code
			code = code.replace(/(\r?\n)/g, " ");
			return "``+ ("+code+") +``";
		}
	);
	this.code = this.code.replace(
		/\{!\s*([\s\S]+?)\s*!\}/gi,
		function (match, code) {
			code = code.replace(/"/g, "``"); // prevent qoute-escaping of inline code
			code = code.replace(/(\n)/g, " ");
			return "``; "+code+";\routput+=``";
		}
	);
	this.code = this.code+"``;";

	this.code = this.code.replace(/(\r?\n)/g, "\\n");
	this.code = this.code.replace(/"/g, "\\\"");
	this.code = this.code.replace(/``/g, "\"");
}

JSDOC.JsPlate.prototype.toCode = function() {
	return this.code;
}

JSDOC.JsPlate.keys = function(obj) {
	var keys = [];
	if (obj.constructor.toString().indexOf("Array") > -1) {
		for (var i = 0; i < obj.length; i++) {
			keys.push(i);
		}
	}
	else {
		for (var i in obj) {
			keys.push(i);
		}
	}
	return keys;
};

JSDOC.JsPlate.values = function(obj) {
	var values = [];
	if (obj.constructor.toString().indexOf("Array") > -1) {
		for (var i = 0; i < obj.length; i++) {
			values.push(obj[i]);
		}
	}
	else {
		for (var i in obj) {
			values.push(obj[i]);
		}
	}
	return values;
};

JSDOC.JsPlate.prototype.process = function(data) {
	var keys = JSDOC.JsPlate.keys;
	var values = JSDOC.JsPlate.values;
	
	try {
		eval(this.code);
	}
	catch (e) {
		print(">> There was an error evaluating the compiled code from template: "+this.templateFile);
		print("   The error was on line "+e.lineNumber+" "+e.name+": "+e.message);
		var lines = this.code.split("\r");
		if (e.lineNumber-2 >= 0) print("line "+(e.lineNumber-1)+": "+lines[e.lineNumber-2]);
		print("line "+e.lineNumber+": "+lines[e.lineNumber-1]);
		print("");
	}
	/*debug*///print(this.code);
	return output;
}