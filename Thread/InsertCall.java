package org.morefly.mycattest;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import com.mysql.jdbc.Statement;

public class InsertCall implements Callable<List<Integer>> {
	private int mchCode;
	private Connection conn = null;
	private String sql;

	public InsertCall(int mchCode, Connection conn, String sql) {
		super();
		this.mchCode = mchCode;
		this.conn = conn;
		this.sql = sql;
	}

	public List<Integer> call() throws Exception {

		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			for (int j = 1; j <= 1000; j++) {
				System.out.println("中间数据：--->" + mchCode + "---" + j);
				// 主表
				pstmt.setInt(1, mchCode);
				pstmt.setInt(2, mchCode);
				pstmt.setInt(3, mchCode);
				pstmt.setString(4, j + "" + mchCode);
				pstmt.setBigDecimal(5, new BigDecimal(100 * mchCode));
				pstmt.setString(6, "00");
				pstmt.setString(7, "00");
				pstmt.setString(8, new Date().toLocaleString());
				pstmt.setString(9, new Date().toLocaleString());
				pstmt.addBatch(); // 添加一次预定义参数

			
			}

			long str1 = System.currentTimeMillis(); // 获取开始时间
			pstmt.executeBatch();
			long end1 = System.currentTimeMillis(); // 获取结束时间
			System.out.println("商户号数据插入完毕耗时:" + ((end1 - str1)/1000)+"s");
			conn.setAutoCommit(false);
			conn.commit();
			rs = pstmt.getGeneratedKeys(); // 获取结果
			List<Integer> list = new ArrayList<Integer>();
			while (rs.next()) {
				list.add(rs.getInt(1));// 取得ID
			}
			
			
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null){
					rs.close();
				}
				if(pstmt != null){
					pstmt.close();
				}
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return null;
	}

}
