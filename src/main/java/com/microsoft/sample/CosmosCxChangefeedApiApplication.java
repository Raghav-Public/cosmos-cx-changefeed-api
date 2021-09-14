package com.microsoft.sample;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

@SpringBootApplication
public class CosmosCxChangefeedApiApplication {

	public static void main(String[] args) {
		
		SpringApplication.run(CosmosCxChangefeedApiApplication.class, args);
		
		//use UTC
		String startDate = "2021-08-31 21:36:01";
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		
		if(args.length > 0) {
			startDate = args[0];
		}
		try {
			CosmosCx cosmosCx = new CosmosCx();
			Session session = cosmosCx.getSession();
			
			//LocalDateTime now = LocalDateTime.now().plusHours(5).minusMinutes(10);
			
			LocalDateTime now = LocalDateTime.parse(startDate, dtf);
			
			String query = "SELECT * FROM ks1.t1 where COSMOS_CHANGEFEED_START_TIME()='" 
        			+ now+ "'";
			byte[] token = null; 
			
			System.out.println(query); 
			while(true)
			{
				SimpleStatement st = new  SimpleStatement(query);
				st.setFetchSize(100);
				if(token!=null)
					st.setPagingStateUnsafe(token);
       		 
				ResultSet result = session.execute(st);
				
				token = result.getExecutionInfo().getPagingState().toBytes();
				for(Row row:result)
				{
					System.out.println(row.getInt("userid") + " | " 
										+ row.getString("name") + "| " 
										+ row.getString("email") + " | "
										+ row.getTimestamp("lmdate"));
				}
			}
		}
		catch(Exception exp) {
			System.out.print(exp.getMessage());
		}
	
	}

}
