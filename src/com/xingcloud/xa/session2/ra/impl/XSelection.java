package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.And;
import com.xingcloud.xa.session2.ra.expr.Between;
import com.xingcloud.xa.session2.ra.expr.Equals;
import com.xingcloud.xa.session2.ra.expr.Expression;
import com.xingcloud.xa.session2.ra.expr.Greater;
import com.xingcloud.xa.session2.ra.expr.GreaterEqual;
import com.xingcloud.xa.session2.ra.expr.Less;
import com.xingcloud.xa.session2.ra.expr.LessEqual;
import com.xingcloud.xa.session2.ra.expr.Not;
import com.xingcloud.xa.session2.ra.expr.Or;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XSelection extends AbstractOperation implements Selection{

	RelationProvider relation;
	Expression expression;

	public XSelection() {
	}

	public XSelection(RelationProvider relation, Expression expression) {
		setInput(relation,expression);
	}

	public Selection setInput(RelationProvider relation, Expression e) {
		resetInput();
		this.relation = relation;
		this.expression = e;
		addInput(relation);
		return this;
	}

	public Relation evaluate() {
		RowIterator iterator = relation.iterator();
		List<Object[]> rows = new ArrayList<Object[]>();

		while(iterator.hasNext()){
			Row row = iterator.nextRow();
			// 等于、不等于、大于、大于等于、小于、小于等于、And、Or
			if(expression instanceof Equals
				 || expression instanceof Not
				 || expression instanceof Greater
				 || expression instanceof GreaterEqual
				 || expression instanceof Less
				 || expression instanceof LessEqual
				 || expression instanceof And
				 || expression instanceof Or
				 || expression instanceof Between){
				if((Boolean) expression.evaluate(row)){
					rows.add(row.get());
				}
			}else{

				// todo: expression 可能为null，暂时直接跳过就行了
				rows.add(row.get());
			}
		}

		return new XRelation(relation.getColumnIndex(),rows);
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}
}
