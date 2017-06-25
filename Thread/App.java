package org.morefly.mycattest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 程序
 */
public class App {

	private static String URL = "jdbc:mysql://127.0.0.1:8066/busi_db?useUnicode=true&characterEncoding=UTF8";
	//private static String URL = "jdbc:mysql://192.168.0.101:3306/db1?useUnicode=true&characterEncoding=UTF8";
	private static String USERNAME = "app";
	private static String PASSWORD = "123456";

	public static void main(String[] args) {
		String sql = "insert into mch_order(mch_id, store_id,clerk_id,mch_order_sn,amount,pay_way,status,time_add,time_finished) values (?,?,?,?,?,?,?,?,?)";
		String wxpaysql = "insert into wxpay_order(mch_order_id,pay_order_sn,openid,sub_openid,pay_type) values (?,?,?,?,?)";
		String alipaysql = "insert into alipay_order(mch_order_id,pay_order_sn,openid,buyer_id,pay_type) values (?,?,?,?,?)";

		ExecutorService exec = Executors.newCachedThreadPool();
		List<Map<String, Future<List<Integer>>>> results = new ArrayList<Map<String, Future<List<Integer>>>>();
		// 控制250个商户
		int threacount=0;//控制线程数量
		for (int i = 1; i <= 250; i++) {
			Map<String, Future<List<Integer>>> mchIds = new HashMap<String, Future<List<Integer>>>();
			mchIds.put(i + "", exec.submit(new InsertCall(i, getConnction(), sql)));
			results.add(mchIds);
			threacount++;
		}

		System.out.println("商户数据插入完毕");
		
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threacount);  
		
		for (Map<String, Future<List<Integer>>> fuMap : results) {
			try {
				for (String mchId : fuMap.keySet()) {
					//商户下对应的订单
					List<Integer> idsList = fuMap.get(mchId).get();
					//System.out.println(idsList.size());
					fixedThreadPool.execute(new MyDBRunable(idsList, getConnction(),getConnction(), wxpaysql, alipaysql, mchId));	
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		System.out.println("订单明细插入完毕");
		exec.shutdown();
		fixedThreadPool.shutdown();
		
	}

	/**
	 * 获取连接
	 * 
	 * @return
	 */
	public static Connection getConnction() {

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
			return connection;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

}
