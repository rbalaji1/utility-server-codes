package utility;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class Write {
  public static void main(String[] args) throws IOException, URISyntaxException 
   {
     
	  
	  appliance a = new appliance();
	  
	  ArrayList<appliance> ap = new ArrayList<appliance>();
	  ap = a.getdata();
	  String data = "";
	  for(int i=0; i<ap.size(); i++){
		  
		  data = data + ap.get(i).getUsername() +"\t" + ap.get(i).getAppliancename() + "," + ap.get(i).getPower() + "," + ap.get(i).getStarttime() + "," + ap.get(i).getDeadline() + "," + ap.get(i).getRuntime() + "\n";
		  
	  }
	  
	  PrintWriter out = new PrintWriter("/Users/ragav2/Documents/utilityip/input.txt");
	  
	  out.println(data);
	  out.close();
	  
	  
	  
      Configuration configuration = new Configuration();
      
      InputStream inputStream = new BufferedInputStream(new FileInputStream("/Users/ragav2/Documents/utilityip/input.txt"));
      //3. Get the HDFS instance
      FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
      //4. Open a OutputStream to write the data, this can be obtained from the FileSytem
      OutputStream outputStream = hdfs.create(new Path("hdfs://localhost:54310/Users/ragav2/hdata/utilityip/input.txt"),
      new Progressable() {  
              @Override
              public void progress() {
         System.out.println("....");
              }
                    });
      try
      {
        IOUtils.copyBytes(inputStream, outputStream, 4096, false); 
      }
      finally
      {
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
      } 
  }
}