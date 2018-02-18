package com.xingcloud.xa.session2.ra.expr;

import com.xingcloud.xa.session2.ra.Aggregation;
import com.xingcloud.xa.session2.ra.Row;
import com.xingcloud.xa.session2.ra.impl.XCount;
import com.xingcloud.xa.session2.ra.impl.XSum;

/**
 * Author: mulisen
 * Date:   2/7/13
 */

public class AggregationExpr implements Expression {

	public Aggregation aggregation;

	public AggregationExpr(Aggregation aggregation) {
		this.aggregation = aggregation;
	}


	public Object evaluate(Row input) {
		if(aggregation instanceof XCount){
			System.out.println("是XCount");
		}else if (aggregation instanceof XSum){
			System.out.println("是XSum");
		}else{
			throw new IllegalArgumentException("不支持的aggregation"+aggregation);
		}
		return aggregation.aggregate();
	}

	public void init(){
		aggregation.init();
	}


}
