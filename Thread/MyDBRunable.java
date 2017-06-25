package org.morefly.mycattest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class MyDBRunable implements Runnable {

	private List<Integer> idsList= null;
	private Connection conn1 = null;
	private Connection conn2 = null;
	private String wxpaysql;
	private String alipaysql;
	private String mchId;

	public MyDBRunable(List<Integer> idsList, Connection conn1, Connection conn2, String wxpaysql, String alipaysql, String mchId) {
		super();
		this.idsList = idsList;
		this.conn1 = conn1;
		this.conn2 = conn2;
		this.wxpaysql = wxpaysql;
		this.alipaysql = alipaysql;
		this.mchId = mchId;
	}

	public void run() {
		PreparedStatement wxpay = null;
		PreparedStatement alipay = null;
		try {
			wxpay = conn1.prepareStatement(wxpaysql);
			alipay = conn2.prepareStatement(alipaysql);

			for (Integer integer : idsList) {
				// 微信支付表
				wxpay.setInt(1, integer);
				wxpay.setString(2, mchId);
				wxpay.setString(3, "qwesadasdcewqwa" + mchId);
				wxpay.setString(4, "hsdfgdfsgfdvzazd" + mchId);
				wxpay.setString(5, "01");
				wxpay.addBatch();

				// 支付宝
				alipay.setInt(1, integer);
				alipay.setString(2, mchId);
				alipay.setString(3, "qwhsdfgasdfas" + mchId);
				alipay.setString(4, "gfsgfdgsdfgfg" + mchId);
				alipay.setString(5, "01");
				alipay.addBatch();
			}

			long str2 = System.currentTimeMillis(); // 获取开始时间
			alipay.executeBatch();
			long end2 = System.currentTimeMillis(); // 获取结束时间
			System.out.println("支付宝商户数据插入完毕耗时:" + ((end2 - str2)/1000)+"s");

			long str3 = System.currentTimeMillis(); // 获取开始时间
			wxpay.executeBatch();
			long end3 = System.currentTimeMillis(); // 获取开始时间
			System.out.println("微信商户数据插入完毕耗时:" + ((end3 - str3)/1000)+"s");
 
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (wxpay != null) {
					wxpay.close();
				}
				if (alipay != null) {
					alipay.close();
				}
				conn1.close();
				conn2.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}

}
