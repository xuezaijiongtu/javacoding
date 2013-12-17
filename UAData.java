package uc;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
/**
 *@Description:UA信息获取操作类(从数据库中筛选出UA等信息写入文件) 
 *@Author:学在囧途(刘兴)
 *@Time:2013.12.17
 */
public class UAData {
	private String url  = "jdbc:mysql://localhost:3306/database";  
    private String user = "root";  
    private String pwd  = "123456";  
      
    public static void main(String[] args){  
        UAData myAct = new UAData();
        HashMap<Integer,String> UAmap = new HashMap<Integer,String>();
        try{  
            Class.forName("com.mysql.jdbc.Driver").newInstance();  
            Connection conn = DriverManager.getConnection(myAct.url, myAct.user, myAct.pwd);  
            ResultSet tablename = conn.getMetaData().getTables(null, null, "syn_uads_user_agent_%", new String[]{"TABLE"});
            Statement stmt  = conn.createStatement();//prepareStatement
            //根据ModelId和language优先级整合数据
            queryModelIdWithLanguage(UAmap, stmt);
            //整理数据并写入文件
            fixedUADatas(UAmap, tablename, stmt);
            //压缩文件部分
            zipFiles();
            conn.close();  
        }catch(Exception ex){  
            System.out.println("Error:"+ex.toString());  
        }  
    }

	/**
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void zipFiles() throws FileNotFoundException, IOException {
		File fn = new File("/home/ucpack/ua.data");
		FileInputStream fis = new FileInputStream(fn);
		BufferedInputStream bis = new BufferedInputStream(fis);
		byte[] buf = new byte[1024];
		int len;
		FileOutputStream fos = new FileOutputStream("/home/ucpack/"+fn.getName()+".zip");
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		ZipOutputStream zos = new ZipOutputStream(bos);//压缩包
		ZipEntry ze = new ZipEntry(fn.getName());//这是压缩包名里的文件名
		zos.putNextEntry(ze);//写入新的 ZIP 文件条目并将流定位到条目数据的开始处

		while((len=bis.read(buf))!=-1)
		{
		   zos.write(buf,0,len);
		   zos.flush();
		}
		bis.close();
		zos.close();
		fn.delete();
	}

	/**
	 * <p>该功能是用于UA数据修订</p>
	 * @param UAmap 保存需要修订的数据
	 * @param tablename
	 * @param stmt
	 * @throws IOException
	 * @throws SQLException 
	 * @author 刘兴LX 2013年12月17日 下午6:03:21  
	 */
	private static void fixedUADatas(HashMap<Integer, String> UAmap,
			ResultSet tablename, Statement stmt) throws IOException,
			SQLException {
		FileFixUAUtils wrong_uas = new FileFixUAUtils("/home/ucpack/", "ua.data");
		while(tablename.next()){
			String table = tablename.getString(3);
			Pattern Patt = Pattern.compile("syn_uads_user_agent_[0-9]{1,3}");  
		    Matcher m = Patt.matcher(table);  
		    if(m.matches()){
		    	table = m.group();
		    	ResultSet num   = stmt.executeQuery("SELECT COUNT(model_id) AS num FROM "+ table +" WHERE model_id <> 0 AND status = 'A'");
		       	int sum = 0;
		    	while(num.next()){
		    		 sum = num.getInt("num");
		    	}
				int page = (int)Math.floor(sum/100000) + 1;
		    	int count = 0;
		    	for(int i = 0; i < page + 1; i++){
			    	//ResultSet rs    = stmt.executeQuery("SELECT model_id, ua FROM "+ table +" WHERE model_id <> 0 AND status = 'A' LIMIT "+count+", "+(count+100000));
		    		ResultSet rs    = stmt.executeQuery("SELECT model_id, ua FROM "+ table +" WHERE model_id <> 0 AND status = 'A' LIMIT 10000");
		    		while(rs.next()){  
			            int model_id  = rs.getInt("model_id");  
			            String ua = rs.getString("ua");
			            String property = UAmap.get(model_id).toString();
			            wrong_uas.writeContent(ua+"`"+property); 
			        }
			        rs.close();
			        count += 100000;
		    	}
		    }
		}
		wrong_uas.flushContent();
	}

	/**
	 * @param UAmap
	 * @param stmt
	 * @throws SQLException
	 */
	private static void queryModelIdWithLanguage(HashMap<Integer, String> UAmap, Statement stmt) throws SQLException {
		ResultSet model_property = stmt.executeQuery("SELECT model_id, language, property_content FROM syn_uads_mobile_model_property WHERE property_code = 'NAME' AND property_content <>''");
		while(model_property.next()){  
		    int model_id  = model_property.getInt("model_id");  
		    String language = model_property.getString("language");
		    String property_content = model_property.getString("property_content");
		    if(language.equals("english")){
		    	UAmap.put(model_id, property_content);
		    }else if(language.equals("chinese") && !UAmap.containsKey(model_id)){
		        	UAmap.put(model_id, property_content);
		    }
		}  
		model_property.close();
	}  
}

