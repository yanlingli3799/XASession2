package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Join;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XJoin extends AbstractOperation implements Join{

	RelationProvider left;
	RelationProvider right;
	String sameKey;// left 和 right 相同的列名
	Map<String, Integer> columnIndex;//join后的索引

	public Relation evaluate() {

		Set<String> lSet = left.getColumnIndex().keySet();
		Set<String> rSet = right.getColumnIndex().keySet();

		Set<String> keySet = new HashSet<>();

		// 求交集 => left 和 right 两个 relation 之间，相同的列名：sameKey
		keySet.clear();
		keySet.addAll(lSet);
		keySet.retainAll(rSet);

		if(keySet.size() == 1){
			sameKey = keySet.iterator().next();
		}else{
			// 只能有一个列名相同
			throw new IllegalArgumentException("nature join have wrong number of same column："+keySet);
		}

		// 求并集 => left 和 right 两个 relation 做 join 运算后的列名及索引：columnIndex
		keySet.clear();
		keySet.addAll(lSet);
		keySet.addAll(rSet);
		columnIndex = new HashMap<>();

		for (String key : keySet) {
			columnIndex.put(key, columnIndex.size());
		}

		keySet.clear();
		keySet = columnIndex.keySet();

		List<Object[]> rows = new ArrayList<Object[]>();

		RowIterator leftIterator = left.iterator();
		while (leftIterator.hasNext()){
			Row lRow = leftIterator.nextRow();

			RowIterator rightIterator = right.iterator();
			while (rightIterator.hasNext()){
				Row rRow = rightIterator.nextRow();

				// 同列名列值相同，则生成一行数据
				if(lRow.get(sameKey).equals(rRow.get(sameKey))){

					Object[] row = new Object[keySet.size()];
					int i = 0;

					for (String column : keySet) {
						Object columnValue = null == lRow.get(column)?rRow.get(column):lRow.get(column);
						row[i] = columnValue;
						i++;
					}

					rows.add(row);
				}else{
					// 若同属性列值不同，则忽略，不放在 join 结果集里。
				}
			}
		}

		return new XRelation(columnIndex, rows);
	}




	public Join setInput(RelationProvider left, RelationProvider right) {
		resetInput();
		this.left = left;
		this.right = right;
		addInput(left);
		addInput(right);
		init();
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

	private void init() {
	}




	public static void main(String[] args) {
		test();
	}

	private static void test() {
		Relation user = new XTableScan("user").evaluate();
		Relation event = new XTableScan("event").evaluate();
		System.out.println("user = \n"+user);
		System.out.println("event = \n"+event);

		XJoin xJoin = new XJoin();
		xJoin.setInput(user,event);
		System.out.println("join result = \n"+xJoin.evaluate());

	}

}
