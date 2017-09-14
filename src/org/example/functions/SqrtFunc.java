package org.example.functions;

import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.function.FunctionBase1;

public class SqrtFunc extends FunctionBase1 {

	public SqrtFunc() {
		super();
	}

	@Override
	public NodeValue exec(NodeValue value) {
		return NodeValue.makeDouble(Math.sqrt(value.getDouble()));
	}

}