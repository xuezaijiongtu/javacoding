import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description 对库中浏览器类型匹配错误的UA进行修改
 * @author 学在囧途
 * @Time 2014.06.05
 * */
public class BrowserTypeFix {
	private String dbUrl;
	private String dbUser;
	private String dbPassword;
	private Connection Connection;
	private String[] number = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"};
	private String[] tablePrefix = {"syn_uads_user_agent_", "uads_user_agent_"};
	
	/**
	 * @description 构造函数
	 * @param String dbUrl, String dbUser, String dbPassword
	 * */
	BrowserTypeFix(){
		this.dbUrl = "jdbc:mysql://127.0.0.1:3306/UADS?allowMultiQueries=true&characterEncoding=utf-8";;
		this.dbUser = "root";
		this.dbPassword = "123456";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.Connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {	
		long startMili=System.currentTimeMillis();// 当前时间对应的毫秒数
		System.out.println("开始 ...");
		
		BrowserTypeFix browserTypeFix = new BrowserTypeFix();
		
		List<Thread> threads = new ArrayList<Thread>();
		//获取浏览器关键字和关键字的type_code
		final ArrayList<HashMap<String, Object>> keywordList = browserTypeFix.getKeywordList();
		for(final String tableType : browserTypeFix.tablePrefix){

			for(final String tableNum : browserTypeFix.number){
				try {
					threads.add(new Thread(tableType + tableNum){
						public void run(){
							BrowserTypeFix browserTypeFix = new BrowserTypeFix();
							try {
								changeTypeCode(browserTypeFix, keywordList, tableType, tableNum);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					});
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		for(Thread t: threads){
			t.start();
		}
		for(Thread t: threads){
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		long endMili=System.currentTimeMillis();
		System.out.println("结束 ..."+endMili);
		System.out.println("脚本总耗时为："+ ((endMili-startMili)/1000) +"秒");
	}
	
	private static void changeTypeCode(BrowserTypeFix browserTypeFix, ArrayList<HashMap<String, Object>> keywordList, String tableType, String tableNum) throws SQLException {
		//获取此UA的浏览器类型type_code
		String tableName = tableType + tableNum;
		String countSql = "SELECT COUNT(*) AS num FROM " + tableName;
		PreparedStatement pstmt = browserTypeFix.Connection.prepareStatement(countSql);
		ResultSet num = pstmt.executeQuery();
		int sum = 0;
		while (num.next()) {
			sum = num.getInt("num");
		}
		int pageSize = 500;
		int page = (int) Math.floor(sum / pageSize) + 1;
		int count = 0;
		for (int i = 0; i < page + 1; i++) {
			String dataSql = "SELECT id, ua, browser_type FROM " + tableName + " WHERE browser_type != 0 LIMIT " + count + ", " + pageSize;
			PreparedStatement stmt = browserTypeFix.Connection.prepareStatement(dataSql);
			ResultSet rs = stmt.executeQuery();
		    while (rs.next()) {
		        String id = rs.getString("id");
		        String ua = rs.getString("ua");
		        String browserType = rs.getString("browser_type");
		        String typeCode = browserTypeFix.getTypeCode(keywordList, ua);
		        if (typeCode != null) {
		            if(!typeCode.equals(browserType)){
		            	//更新UA对应的type_code
		            	//System.out.println("ID为:" + id + " UA为:" + ua + "数据库中浏览器类型为:" + browserType + "应该更改为:" + typeCode);
		            	String content = "ID为:" + id + " UA为:" + ua + "数据库中浏览器类型为:" + browserType + "应该更改为:" + typeCode + "\n";
		            	File file = new File("D://user-agent//" + tableName + ".txt");
		            	browserTypeFix.writeToTxtByOutputStream(file, content);
		            }
		        }
		    }
		    rs.close();
		    count += pageSize;
		}
	}
	
	 public void writeToTxtByOutputStream(File file, String content){
		  BufferedOutputStream bufferedOutputStream = null;
		  try {
		   bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file, true));
		   bufferedOutputStream.write(content.getBytes());
		  } catch (FileNotFoundException e) {
		   e.printStackTrace();
		  } catch(IOException e ){
		   e.printStackTrace();
		  }finally{
		   try {
		    bufferedOutputStream.close();
		   } catch (IOException e) {
		    e.printStackTrace();
		   }
		  }
		 }
	
	/**
	 * @description 关键字查找，返回匹配关键字的类型码
	 * @param List<Map<String, Object>> keywordList, String ua
	 * @return
	 * */
	public String getTypeCode(ArrayList<HashMap<String, Object>> keywordList, String ua){
		String typeCode = "";
		ArrayList<HashMap<String, Object>> hitKeywordList = new ArrayList<HashMap<String, Object>>();
		//查找进行关键字与UA匹配，不区分大小写
		for(HashMap<String, Object> keywordMsg : keywordList){
			if(ua.toLowerCase().contains(keywordMsg.get("ua_keyword").toString().toLowerCase())){
				hitKeywordList.add(keywordMsg);
			}
		}
		typeCode = this.getLongKeywordTypeCode(hitKeywordList);
		if(typeCode != null){
			return typeCode;
		}else{
			return null;
		}
	}
	
	/**
	 * @description 找出关键词list中最长关键词，返回其type_code
	 * @param
	 * @return
	 * */
	public String getLongKeywordTypeCode(ArrayList<HashMap<String, Object>> hitKeywordList){
		if(hitKeywordList.size() != 0){
			String longKeyword = "";
			String tempKeyword = "";
			String typeCode = "";
			for(Map<String, Object> keywordMsg : hitKeywordList){
				tempKeyword = keywordMsg.get("ua_keyword").toString();
				if(tempKeyword.length() > longKeyword.length()){
					longKeyword = tempKeyword;
					typeCode = keywordMsg.get("type_code").toString();
				}
			}
			return typeCode;
		}
		return null;
	}
	
	/**
	 * @description 获取关键词及关键词代码
	 * @param
	 * @return
	 * */
	public ArrayList<HashMap<String, Object>> getKeywordList(){
		ArrayList<HashMap<String, Object>> keywordList = new ArrayList<HashMap<String, Object>>();
		String sql = "SELECT type_code, ua_keyword FROM uads_browser_type WHERE	status = 'A'";
		try {
			PreparedStatement pstmt = this.Connection.prepareStatement(sql);
			ResultSet Result = pstmt.executeQuery();
			while (Result.next()) {
				HashMap<String, Object> keywordMsg = new HashMap<String, Object>();
				//过滤掉非UC浏览器关键字
				if(Result.getString("type_code").equals("0")){
					keywordMsg.put("ua_keyword", Result.getString("ua_keyword"));
					keywordMsg.put("type_code", Result.getString("type_code"));
					keywordList.add(keywordMsg);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return keywordList;
	}
	
	/**
	 * @description 获取表数据总条数
	 * @param tableName 表名
	 * @return
	 * */
	public long getDataNum(String tableName){
		long num = 0;
		String sql = "SELECT COUNT(*) AS num FROM " + tableName;
		try {
			PreparedStatement pstmt = this.Connection.prepareStatement(sql);
			ResultSet Result = pstmt.executeQuery();
			while (Result.next()) {
				num = Long.parseLong(Result.getString("num"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return num;
	}
	

}
