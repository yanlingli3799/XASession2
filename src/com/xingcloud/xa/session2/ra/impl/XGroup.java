package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Aggregation;
import com.xingcloud.xa.session2.ra.Count;
import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Group;
import com.xingcloud.xa.session2.ra.Projection;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.Sum;
import com.xingcloud.xa.session2.ra.expr.AggregationExpr;
import com.xingcloud.xa.session2.ra.expr.ColumnValue;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XGroup extends AbstractOperation implements Group {


	RelationProvider relation;

	Expression[] groupingExpressions;

	Expression[] projectionExpressions;

	public Relation evaluate() {

		// TODO:检查，select 的列必须是 group 列的子集？？

//		if(relation instanceof XSelection){
//			relation = ((XSelection) relation).evaluate();
//		}else{
//			throw new IllegalArgumentException("XGroup.relation 不支持："+relation.getClass());
//		}


		Map<String, Integer> columnIndex = new TreeMap<String, Integer>();

		// 先按group by 分组
		Map<String,List<Object[]>> groupRowsMap = new HashMap<>();

		RowIterator rowIterator = relation.iterator();
		while(rowIterator.hasNext()){

			XRelation.XRow row = (XRelation.XRow)rowIterator.nextRow();
			columnIndex = row.columnNames;

			StringBuilder key = new StringBuilder();
			for (Expression expression : groupingExpressions) {
				key.append(expression.evaluate(row));
			}

			String groupKey = key.toString();
			if (!groupRowsMap.containsKey(groupKey)){
				groupRowsMap.put(groupKey, new ArrayList<>());
			}
			groupRowsMap.get(groupKey).add(row.get());
		}

		Map<String, List<Object[]>> groupProjectionRows = new HashMap<String, List<Object[]>>();

		// 分组遍历
		for(Map.Entry<String, List<Object[]>> entry:groupRowsMap.entrySet()){

			// 当前组的key和行列表
			String groupString = entry.getKey();
			List<Object[]> rowList = entry.getValue();

			groupProjectionRows.put(groupString, new ArrayList<Object[]>());


			// 生成 relationProvider
			RelationProvider relationProvider = new XRelation(columnIndex,rowList);

			// 聚合操作需要更新一下XCount、XSum、XDistinct里面的relation
			for(Expression expression:projectionExpressions){
				if(expression instanceof AggregationExpr){
					Aggregation aggregation = ((AggregationExpr)expression).aggregation;
					if (aggregation instanceof Sum){
						((XSum) aggregation).updateRelation(relationProvider);
					}else if (aggregation instanceof Count){
						((XCount)aggregation).updateRelation(relationProvider);
					}else if (aggregation instanceof Distinct){
						((XDistinct)aggregation).setInput(relationProvider, ((XDistinct)aggregation).expressions);
					}
				}else if(expression instanceof ColumnValue){
					// 啥都不用管
				}
			}

			// 生成 projection
			XProjection xProjection = new XProjection();
			xProjection.setInput(relationProvider, projectionExpressions);
			XRelation relation = (XRelation)xProjection.evaluate();
			groupProjectionRows.get(groupString).addAll(relation.rows);
		}

		// 结果集
		List<Object[]> rows = new ArrayList<Object[]>();
		for(Map.Entry<String, List<Object[]>> entry:groupProjectionRows.entrySet()){
			rows.addAll(entry.getValue());
		}


		// 结果标题
		Map<String, Integer> resultColumnIndex = new TreeMap<String, Integer>();

		int colNum=0;
		for (Expression proj : projectionExpressions) {
			StringBuilder sb = new StringBuilder();
			InlinePrint.printExpression(proj, sb);
			resultColumnIndex.put(sb.toString(), colNum);
			colNum++;
		}

		return new XRelation(resultColumnIndex,rows);

	}

    public Group setInput(RelationProvider relation, Expression[] groupingExpressions, Expression[] projectionExpressions) {
		resetInput();
        this.relation = relation;
		this.groupingExpressions = groupingExpressions;
		this.projectionExpressions = projectionExpressions;
		addInput(relation);
        return this;
    }

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
