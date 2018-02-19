package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Aggregation;
import com.xingcloud.xa.session2.ra.Count;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.RowIterator;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XCount extends AbstractAggregation implements Count {
	RelationProvider relation;

	public XCount() {
	}

	public XCount(RelationProvider relation) {
		setInput(relation);
	}

	public Aggregation setInput(RelationProvider relation) {
		resetInput();
		init();
		this.relation = relation;
		addInput(relation);
		return this;
	}

	public Object aggregate() {

		if(relation instanceof XSelection){
			int count=0;
			RowIterator iterator = relation.iterator();
			while(iterator.hasNext()){
				count++;
				iterator.nextRow();
			}
			return count;
		}else if(relation instanceof XDistinct){


			return null;
		}else{
			throw new IllegalArgumentException("XCount 不支持："+relation.getClass());
		}

	}

	public void init() {

	}

	public static void main(String[] args) {
		test();
	}

	private static void test() {
		Relation r = new XTableScan("user").evaluate();
		System.out.println(r);
		XCount count = new XCount();
		count.setInput(r);

		System.out.println(count.aggregate());
	}

}
