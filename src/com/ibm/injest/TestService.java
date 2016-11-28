package com.ibm.injest;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import com.messagehub.samples.MessageHubJavaSample;

@ApplicationPath("rest")
@Path("sharadtestsample")
public class TestService  extends Application{
	  private String topic="SampleTopic";
	  private String ftpPath="";
	  private String line="";
	@GET
	@Path("getNumArray")
	@Produces("application/json")
	public List<String> getSampleData(@QueryParam("value") String value) {
		return Arrays.asList(new String[] { "one", "two", "three", "four", value });
	}

	@GET
	@Path("getString")
	@Produces("application/json")
	public String getString() {
		System.out.println("Test called");
		return "Test";
	}

	  @GET
	  @Path("fetchData")
	  @Produces("application/json")
	  public String fetchData() {
		  System.out.println("fetch data called");
		   try{
			    MessageHubJavaSample proxy=new MessageHubJavaSample(topic);
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream("data/all.cars");
	        	InputStreamReader r = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(r);
	        	while((line=br.readLine())!=null){
	        		 proxy.InjestData(line);
	        		 System.out.println("Injesting Data:: " +line);
	        	}			    
			   
		   }catch(Exception t){t.printStackTrace();}
         
		   return "Success";

	  } 
	  
	  @GET
	  @Path("fetchDCData")
	  @Produces("application/json")
	  public String fetchDCData(@QueryParam("region")String region) {
		  System.out.println("fetch data called");
		     String temp=region.trim();
			 System.out.println("temp:: " +temp + " equalIgnoreCase:: " +temp.equals("USA"));
			  if(region.equalsIgnoreCase("ATL")){
				  topic="atl";
				  ftpPath="/data/atl/all.cars";
				  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
				  pushData(topic,ftpPath);
			   }else if(region.equalsIgnoreCase("USA")){
					  topic="usa";
					  ftpPath="/data/usa/FL_insurance_sample.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
			   }else if(region.equalsIgnoreCase("IND")){
					  topic="ind";
					  ftpPath="/data/ind/Sacramentorealestatetransactions.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
			   }else if(region.equalsIgnoreCase("PAK")){
					  topic="pak";
					  ftpPath="/data/pak/SalesJan2009.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
			   }
   
		   return "Success";

	  } 
	  
	  public void pushData(String topic,String datapath){
		   try{
			    MessageHubJavaSample proxy=new MessageHubJavaSample(topic);
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream(ftpPath);
	        	InputStreamReader r = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(r);
	        	System.out.println("Inside Push Data:: " +fileStream);
	        	while((line=br.readLine())!=null){
	        		System.out.println("Injesting Data:: " +line);
	        		 proxy.InjestData(line);        		 
	        	}			    
			   System.out.println("Outside While");
		   }catch(Exception t){t.printStackTrace();}
	  }
}
