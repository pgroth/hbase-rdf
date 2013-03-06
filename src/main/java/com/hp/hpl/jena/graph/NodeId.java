package com.hp.hpl.jena.graph;

import nl.vu.datalayer.hbase.id.Id;


/**
 * Node subtype which wraps an Id
 *
 */
public class NodeId extends Node {

	public NodeId(Object id) {
		super(id);
	}

	@Override
	public Object visitWith(NodeVisitor v) {
		return null;
	}

	@Override
	public boolean isConcrete() {
		return true;
	}
	
	@Override
	public boolean isId()
    { return true; }

	@Override
	public Id getId() {
		return (Id)label;
	}

	@Override
	public boolean equals(Object o) {
		return label.equals(o);
	}

}
