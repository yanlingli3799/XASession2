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

		// TODO:检查，select 的列必须是 group 列的子集。

		if(relation instanceof XSelection){
			relation = ((XSelection) relation).evaluate();
		}else{
			throw new IllegalArgumentException("XGroup.relation 不支持："+relation.getClass());
		}

		System.out.println("测试："+relation);


		Map<String, Integer> columnIndex = new TreeMap<String, Integer>();

//		// group rows
//		Map<String, List<Row>> groupRows = new HashMap<String, List<Row>>();
//		Map<String, Integer> _columnIndex = new TreeMap<String, Integer>();
//		RowIterator iterator = relation.iterator();
//		while (iterator.hasNext()){
//			XRelation.XRow row = (XRelation.XRow)iterator.nextRow();
//			_columnIndex = row.columnNames;
//			String groupString = "";
//			for(Expression expression:groupingExpressions){
//				groupString += (String)expression.evaluate(row);
//			}
//
//			if (!groupRows.containsKey(groupString)){
//				groupRows.put(groupString,new ArrayList<Row>());
//			}
//
//			groupRows.get(groupString).add(row);
//		}
//
//		System.out.println("测试，groupRows="+groupRows.toString());
//
//


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

		System.out.println("测试，groupRowsMap="+groupRowsMap.toString());
		System.out.println("测试，columnIndex="+columnIndex.toString());




		Map<String, List<Object[]>> groupProjectionRows = new HashMap<String, List<Object[]>>();

		// 分组遍历
		for(Map.Entry<String, List<Object[]>> entry:groupRowsMap.entrySet()){

			// 当前组的key和行列表
			String groupString = entry.getKey();
			List<Object[]> rowList = entry.getValue();

			groupProjectionRows.put(groupString, new ArrayList<Object[]>());


			// 生成 relationProvider
			RelationProvider relationProvider = new XRelation(columnIndex,rowList);

			//reinput the aggregation expression's relation provider
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
					// todo:
				}
			}

			// 生成 projection
			XProjection xProjection = new XProjection();
			xProjection.setInput(relationProvider, projectionExpressions);
			XRelation relation = (XRelation)xProjection.evaluate();
			groupProjectionRows.get(groupString).addAll(relation.rows);


		}

		// combine each group result
		List<Object[]> rows = new ArrayList<Object[]>();
		for(Map.Entry<String, List<Object[]>> entry:groupProjectionRows.entrySet()){
			rows.addAll(entry.getValue());
		}


		return new XRelation(columnIndex,rows);


//		// 根据 projections 获取新的表头（列名索引）
//		Map<String,Integer> resultColumnIndex= new HashMap<>();
//		for(int i=0;i<projectionExpressions.length;i++){
//			if(projectionExpressions[i] instanceof ColumnValue){
//				resultColumnIndex.put(((ColumnValue)projectionExpressions[i]).columnName, i);
//			}
//		}

//		System.out.println("测试，表头="+groupMap.toString());


//		List<Relation> relations = new ArrayList<>();
//		for (Map.Entry<String, List<Object[]>> entry : groupMap.entrySet()) {
//			relations.add(new XRelation(relation.getColumnIndex(),entry.getValue()));
//		}
//

//
//		System.out.println("测试，relations="+relations.toString());
//
//
//
//
//		for (Relation re : relations) {
//			List<Object[]> resultRows = new ArrayList<Object[]>();
//
//			// 当前这一组，relation
//			 iterator = re.iterator();
//			while(iterator.hasNext()){
//				Row row = iterator.nextRow();
//				Object[] resultRow = new Object[projectionExpressions.length];
//
//				System.out.println("测试，projectionExpressions="+projectionExpressions.length);
//
//				for(int i=0;i<projectionExpressions.length;i++){
//
//					if(projectionExpressions[i] instanceof ColumnValue){
//						resultRow[i] = projectionExpressions[i].evaluate(row);
//					}else if(projectionExpressions[i] instanceof AggregationExpr){
//
//						Object o = ((AggregationExpr)projectionExpressions[i]).aggregation.aggregate();
//
//						resultRow[i] = o;
//					}
//				}
//				resultRows.add(resultRow);
//			}
//
//			System.out.println("测试，当前一组relation="+new XRelation(resultColumnIndex, resultRows));
//			System.out.println("测试，当前一组projection="+new XProjection().setInput(re, groupingExpressions));
//
//		}
//

//		result = new XRelation(resultColumnIndex, resultRows);


//		return null;  //TODO method implementation
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
