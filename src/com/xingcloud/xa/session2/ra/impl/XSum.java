package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Aggregation;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.Sum;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XSum extends AbstractAggregation implements Sum {

	RelationProvider relation;
	String columnName;
	Double sum;

	public Aggregation setInput(RelationProvider relation, String columnName) {
        resetInput();
		init();
		this.relation = relation;
		this.columnName = columnName;
		addInput(relation);
		return this;
	}

	public Object aggregate() {
		RowIterator iterator = relation.iterator();
		while(iterator.hasNext()){
			Row row = iterator.nextRow();
			double columnValue = Double.parseDouble(row.get(columnName).toString());
			sum += columnValue;
		}
		return sum;
	}

	public void init() {
		sum = 0d;
	}

	public static void main(String[] args) {
		test();
	}

	private static void test() {
		Relation r = new XTableScan("user").evaluate();
		System.out.println("user = \n"+r);
		XSum xSum = new XSum();
		xSum.setInput(r,"uid");
		System.out.println("sum = "+xSum.aggregate());
	}


}
