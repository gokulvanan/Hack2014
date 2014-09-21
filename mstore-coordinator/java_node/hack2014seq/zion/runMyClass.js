#!/usr/bin/env node

var java = require("../../");
java.classpath.push("java_src");

var MyClass = java.import("com.nearinfinity.nodeJava.MyClass");

var result = MyClass.addNumbersSync("10");
console.log(result);
