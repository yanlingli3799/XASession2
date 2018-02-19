package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.expr.Expression;

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

			// 1.把relation 结果计算出来
			Relation result = ((XSelection) relation).evaluate();

			// 2.todo:去重

			throw new IllegalArgumentException("代码没写完");

		}else{
			throw new IllegalArgumentException("XDistinct.relation 不支持："+relation.getClass());
		}
//		return null;  //TODO method implementation
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
