package adjuster_homework;

import org.json.simple.*;
import org.json.simple.parser.*;
import java.net.*;
import java.io.*;
import java.sql.*;


public class Homework {
	private static final String campaignAPI = "http://homework.ad-juster.com/api/campaigns";
	private static final String creativeAPI = "http://homework.ad-juster.com/api/creatives";
	private static final String sqlurl	  	= "jdbc:sqlserver://localhost:1433;DatabaseName=testData;";
	private static final String username  	= "testuser";
	private static final String password  	= "123";
	private static final String file		= "output.csv";
	private static boolean DEBUG = true;
	
	
	public static void main(String [] args)
	{
		/* Problem 1: Write a HTTP Client, in Java, that pulls the client’s data from the API, 
		 * and save it locally to a database of choice.
		 */
		JSONArray campaignJSON = Homework.fetchJSON(campaignAPI);
		JSONArray creativeJSON = Homework.fetchJSON(creativeAPI);
		Homework.insertJSON(campaignJSON, creativeJSON);
		
		/* Problem 2: Write a database command/query (using your database of choice) to calculate 
		 * total clicks and views at the campaign level per child creatives.
		 */
		
		/* Solution:
		 *
         * (id, clicks, views)
         * SELECT parentId as id, SUM(clicks) as clicks, SUM(views) as views
         * FROM campaigns, creatives 
         * WHERE campaigns.id = parentId
         * GROUP BY parentId
         * ORDER BY parentId ASC
         * 
         * (name, cpm, id, clicks, views)
         * SELECT name, cpm, result.id, clicks, views
         * FROM CAMPAIGNS, 
         * 	(SELECT parentId as id, SUM(clicks) as clicks, SUM(views) as views
         * 	 FROM campaigns, creatives
         * 	 WHERE campaigns.id = parentId
         * 	 GROUP BY parentId) as RESULT
         * WHERE campaigns.id = result.id
         * ORDER BY result.id ASC;
         */
		
		String problem2Query = 
				  "SELECT name, cpm, result.id, clicks, views "
				+ "FROM CAMPAIGNS, "
				+ " (SELECT parentId as id, SUM(clicks) as clicks, SUM(views) as views "
				+ "  FROM campaigns, creatives "
				+ "  WHERE campaigns.id = parentId "
				+ "  GROUP BY parentId) as RESULT "
				+ "WHERE campaigns.id = result.id "
				+ "ORDER BY result.id";

		/* Problem 3: Output the results from Problem 2 in a CSV file. */
		Homework.writeToCSV(problem2Query);
	}

	public static JSONArray fetchJSON(String url) 
	{
		String result = "";
		JSONArray arr = null;
		try {
			URL link = new URL(url);
			HttpURLConnection conn = (HttpURLConnection) link.openConnection();
			conn.setRequestMethod("GET");
			
			try (BufferedReader in = new BufferedReader(
					new InputStreamReader(conn.getInputStream()))) {
				String line;
				StringBuilder response = new StringBuilder();
				
				while((line = in.readLine()) != null) {
					response.append(line);
					response.append(System.lineSeparator());
				}
				
				result = response.toString();
				
				in.close();
			}
			
			JSONParser parser = new JSONParser();
			arr = (JSONArray) parser.parse(result); 
			
			if(DEBUG) System.out.println(url + " yielded " + arr.size() + " results.");

			conn.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return arr;
	}
	
	public static void insertJSON(JSONArray campaigns, JSONArray creatives)
	{
		int campaignSize = campaigns.size();
		int creativeSize = creatives.size();

		Connection conn = null;
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			conn = DriverManager.getConnection(sqlurl, username, password);

			Statement stmt = conn.createStatement();
			
			// Clear existing campaign/creative data
			stmt.executeUpdate("DELETE FROM CAMPAIGNS");
			stmt.executeUpdate("DELETE FROM CREATIVES");
			
			// Insert into campaign table
			for (int i = 0; i < campaignSize; i++)
			{
				JSONObject curr = (JSONObject)campaigns.get(i);
				String sqlUpdate = "INSERT INTO CAMPAIGNS "
						   + "SELECT * "
						   + "FROM OPENJSON('" + curr.toJSONString().replace("'", "''")
						   + "')"
						   + "WITH ("
						   + "cpm money, "
						   + "name varchar(255), "
						   + "id int, "
						   + "startDate date"
						   + ")";
				stmt.executeUpdate(sqlUpdate);
			}
			
			if(DEBUG) System.out.println("Inserted " + campaignSize + " rows into CAMPAIGNS");
			
			// Insert into creative table
			for (int i = 0; i < creativeSize; i++)
			{
				JSONObject curr = (JSONObject)creatives.get(i);
				String sqlUpdate = "INSERT INTO CREATIVES "
						   + "SELECT * "
						   + "FROM OPENJSON('" + curr.toJSONString().replace("'", "''")
						   + "')"
						   + "WITH ("
						   + "clicks int, "
						   + "id int, "
						   + "parentId int, "
						   + "views int"
						   + ")";
				stmt.executeUpdate(sqlUpdate);
			}
			
			if(DEBUG) System.out.println("Inserted " + creativeSize + " rows into CREATIVES");
			
			conn.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void writeToCSV(String query)
	{
		Connection conn = null;
		try {
			int lines = 0;
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			conn = DriverManager.getConnection(sqlurl, username, password);

			Statement stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery(query);

			FileWriter fstream = new FileWriter(file);
			BufferedWriter out = new BufferedWriter(fstream);

			while(rset.next()) {
				lines++;
				out.write(Integer.toString(rset.getInt("id")) 	  + ", ");
				out.write(Integer.toString(rset.getInt("clicks")) + ", ");
				out.write(Integer.toString(rset.getInt("views"))  + ", ");
				out.write(Integer.toString(rset.getInt("cpm") * rset.getInt("views") / 1000 ));
				out.newLine();
			}
			if (DEBUG) { 
				System.out.println("Finished writing " + lines + " lines into " + file);
			}
			
			out.close();
			conn.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


