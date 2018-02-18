package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.AggregationExpr;
import com.xingcloud.xa.session2.ra.expr.ColumnValue;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XProjection extends AbstractOperation implements Projection{

	RelationProvider relation;
	Expression[] projections;

	public Relation evaluate() {
		System.out.println("XProjection，relation = " + relation);


		// todo:这部分，是做 select-where 之间的处理
		if(relation instanceof XSelection) {

			if(((XSelection) relation).relation instanceof XTableScan){

				// 1.扫表，得到一个初始 relation
				Relation originRelation = new XTableScan(((XTableScan) ((XSelection) relation).relation).tableName).evaluate();

				// 2.做where条件过滤.todo:多个条件时，expression 是什么样的？
				XSelection xSelection = new XSelection(originRelation,((XSelection) relation).expression);
				originRelation = xSelection.evaluate();

				if(projections.length<=0){
					throw new IllegalArgumentException("参数错误，select 没条件");
				}

				if(projections[0] instanceof ColumnValue){
					// 3.根据 projections 获取新的表头（列名索引）
					Map<String,Integer> resultColumnIndex= new HashMap<>();
					for(int i=0;i<projections.length;i++){
						if(projections[i] instanceof ColumnValue){
							resultColumnIndex.put(((ColumnValue)projections[i]).columnName, i);
						}
					}

					// 4.迭代遍历每一行，生成单行数据，追加到新的 relation 行数据里面
					List<Object[]> resultRows = new ArrayList<Object[]>();
					RowIterator iterator = originRelation.iterator();
					while(iterator.hasNext()){
						Row row = iterator.nextRow();
						Object[] resultRow = new Object[projections.length];
						for(int i=0;i<projections.length;i++){
							resultRow[i] = projections[i].evaluate(row);
						}
						resultRows.add(resultRow);
					}

					// 5. 生成新的 relation 并返回结果
					return new XRelation(resultColumnIndex, resultRows);
				}else if (projections[0] instanceof AggregationExpr){

					if(((AggregationExpr)projections[0]).aggregation instanceof XCount){
						XCount xCount = new XCount(originRelation);
						Map<String,Integer> map = new HashMap<>();
						map.put(xCount.aggregate().toString(),0);
						return new XRelation(map, new ArrayList<>());
					}else if(((AggregationExpr)projections[0]).aggregation instanceof XSum){
						XSum xSum = new XSum(originRelation,((XSum) ((AggregationExpr)projections[0]).aggregation).columnName);
						Map<String,Integer> map = new HashMap<>();
						map.put(xSum.aggregate().toString(),0);
						return new XRelation(map, new ArrayList<>());
					}


				}else{
					throw new IllegalArgumentException("不支持的projections[0]="+projections[0]);
				}


			}else{
				throw new IllegalArgumentException("XSelection.relation != XTableScan");
			}



		}else{
			System.out.println("暂时先只管XSelection，relation="+relation);
		}




		return null;  //TODO method implementation
	}

	public Projection setInput(RelationProvider relation, Expression ... projections) {
        resetInput();
		this.relation = relation;
		this.projections = projections;
		addInput(relation);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}
}
