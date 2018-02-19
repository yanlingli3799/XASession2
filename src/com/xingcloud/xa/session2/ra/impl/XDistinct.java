package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XDistinct extends AbstractOperation implements Distinct {

	RelationProvider relation;
	Expression[] expressions;

	public Relation evaluate() {


		if(relation instanceof XSelection){
			relation = ((XSelection) relation).evaluate();
		}else{
			throw new IllegalArgumentException("XDistinct.relation 不支持："+relation.getClass());
		}


		List<String> distinct = new ArrayList<>();// 用于做 Distinct 去重
		List<Object[]> resultRows = new ArrayList<Object[]>();

		RowIterator rowIterator = relation.iterator();
		while(rowIterator.hasNext()){
			Row row = rowIterator.nextRow();

			for (Expression expression : expressions) {
				Object o = expression.evaluate(row).toString();
				if (!distinct.contains(o.toString())) {
					distinct.add(o.toString());
					resultRows.add(row.get());
				}
			}
		}

		return new XRelation(relation.getColumnIndex(),resultRows);

	}

	public Distinct setInput(RelationProvider relation, Expression ... expressions ) {
		resetInput();
        this.relation = relation;
		this.expressions = expressions;
		addInput(relation);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
