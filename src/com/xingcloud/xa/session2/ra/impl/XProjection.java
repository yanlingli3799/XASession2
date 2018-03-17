package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.AggregationExpr;
import com.xingcloud.xa.session2.ra.expr.ColumnValue;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XProjection extends AbstractOperation implements Projection{

	RelationProvider relation;
	Expression[] projections;

	public Relation evaluate() {

		// 1.【from子句】扫表，得到一个初始 relation
		// 2.【on子句】暂无
		// 3.【join子句】暂时没看这段逻辑
		// 4.【where子句】做where条件过滤.
		// 5.【group by子句】暂无
		// 6.【with子句】暂无
		// 7.【having子句】暂无
		// 8.【select子句】
		// 9.【distinct子句】
		// 10.【order by子句】


//		if(projections.length<=0){
//			throw new IllegalArgumentException("参数错误，select 格式不正确");
//		}
//
//		Relation originRelation = null;
//		XRelation result = null;
//
//
//		if(relation instanceof XSelection) {
//			// 还有其他的计算，需要做 projections 条件过滤
//			originRelation = ((XSelection)relation).evaluate();
//		} else if(relation instanceof  XDistinct){
//			return ((XDistinct)relation).evaluate();
//		} else if(relation instanceof XJoin){
//			return ((XJoin)relation).evaluate();
//		} else{
//			throw new IllegalArgumentException("暂不支持："+relation.getClass());
//		}
//
//		if(projections[0] instanceof ColumnValue){
//
//			if(((ColumnValue)projections[0]).columnName.equals("*")){
//				return originRelation;
//			}
//
//
//			// 根据 projections 获取新的表头（列名索引）
//			Map<String,Integer> resultColumnIndex= new HashMap<>();
//			for(int i=0;i<projections.length;i++){
//				if(projections[i] instanceof ColumnValue){
//					resultColumnIndex.put(((ColumnValue)projections[i]).columnName, i);
//				}
//			}
//
//			// 迭代遍历每一行，生成单行数据，追加到新的 relation 行数据里面
//			List<Object[]> resultRows = new ArrayList<Object[]>();
//			RowIterator iterator = originRelation.iterator();
//			while(iterator.hasNext()){
//				Row row = iterator.nextRow();
//				Object[] resultRow = new Object[projections.length];
//				for(int i=0;i<projections.length;i++){
//					resultRow[i] = projections[i].evaluate(row);
//				}
//				resultRows.add(resultRow);
//			}
//
//
//			// 生成新的 relation 并返回结果
//			result = new XRelation(resultColumnIndex, resultRows);
//
//
//		}else if (projections[0] instanceof AggregationExpr){
//
//			Object o = ((AggregationExpr)projections[0]).aggregation.aggregate();
//			Map<String,Integer> map = new HashMap<>();
//			map.put(o.toString(),0);
//			result = new XRelation(map, new ArrayList<>());
//		}else{
//			throw new IllegalArgumentException("不支持的projections[0]="+projections[0]);
//		}
//
//		return result;


		if(null == projections || projections.length<=0){
			throw new IllegalArgumentException("参数错误，select 格式不正确");
		}

		Map<String, Integer> resultColumnIndex = new TreeMap<String, Integer>();
		List<Expression> newProjections = new ArrayList<Expression>();

		RowIterator rowIterator = relation.iterator();
		Map<String, Integer> originColumnIndex = ((XRelation.XRow)rowIterator.nextRow()).columnNames;

		// 先对 select(*) 做特殊处理，获取表头
		int colNum=0;
		for (int i = 0; i < projections.length; i++) {
			Expression proj = projections[i];
			// select(*) 特殊处理
			if ((proj instanceof ColumnValue) && (((ColumnValue) proj).columnName.equals("*"))){
				Expression[] projections = new Expression[originColumnIndex.size()];
				for(Map.Entry<String, Integer> entry: originColumnIndex.entrySet()){
					Expression expression = new ColumnValue(entry.getKey());
					projections[entry.getValue()] = expression;
				}
				// 如果是select(*)，则所有的列都要
				for(int j=0; j<originColumnIndex.size();j++){
					newProjections.add(projections[j]);
					StringBuilder sb = new StringBuilder();
					InlinePrint.printExpression(projections[j],sb);
					resultColumnIndex.put(sb.toString(), colNum);
					colNum++;
				}
			}else{
				// 如果不是select(*)，那么 projections 有啥就要啥
				newProjections.add(proj);
				StringBuilder sb = new StringBuilder();
				InlinePrint.printExpression(proj,sb);
				resultColumnIndex.put(sb.toString(), colNum);
				colNum++;
			}
		}


		// 扫描一行，检查有没有聚合操作
		List<Object[]> resultRows = new ArrayList<Object[]>();
		boolean hasAggregation = false;

		// 先扫描一行，确定 newProjections 执行计划里有没有聚合操作
		RowIterator iterator = relation.iterator();
		Row oldRow = iterator.nextRow();
		Object[] newRow = new Object[newProjections.size()];
		for (int i = 0; i < newProjections.size(); i++) {
			Expression proj = newProjections.get(i);
			if (proj instanceof AggregationExpr){
				hasAggregation = true;
			}
			newRow[i] = proj.evaluate(oldRow);
		}
		resultRows.add(newRow);


		// 如果没有聚合操作，那就继续 iterator
		// 注意：前面已经扫描了一行了
		if (! hasAggregation){
			iterator.nextRow();
			while (iterator.hasNext()){
				 oldRow = iterator.nextRow();
				 newRow = new Object[newProjections.size()];
				for (int i = 0; i < newProjections.size(); i++) {
					Expression proj = newProjections.get(i);
					newRow[i] = proj.evaluate(oldRow);
				}
				resultRows.add(newRow);
			};
		}

		// 如果有聚合操作，就不需要再继续扫描了。因为聚合操作结果输出的是单行。
		// 原来的方案是，一进来就检查 Relation 的 instanceof ，是个误区。
		return new XRelation(resultColumnIndex, resultRows);
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
