package utility;

import java.sql.*;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utility.server.Map2.Reduce2;
import utility.server.Map3.Reduce3;
import utility.server.Map4.Reduce4;

public class server extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();

		conf.addResource(new Path(
				"/Users/ragav2/hadoop-1.2.1/conf/core-site.xml"));
		conf.addResource(new Path(
				"/Users/ragav2/hadoop-1.2.1/conf/hdfs-site.xml"));
		Job job = new Job(conf, "job");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * FileSystem fs = FileSystem.get(conf); FSDataInputStream inputStream =
		 * fs.open(new Path(args[0]));
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

	

		// job.waitForCompletion(true);
		job.waitForCompletion(true);

		
		
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:54310"),
				conf);
		Path hd = new Path("hdfs://localhost:54310/Users/ragav2/hdata/op/part-r-00000");
		Path local = new Path("/Users/ragav2/Documents/utilityop/powerintime.txt");
		hdfs.copyToLocalFile(false, hd, local);
		
		
		
		
		// -------------------------------------------------------------------------------------------------------------------------------------------

		Configuration conf2 = new Configuration();
		conf2.addResource(new Path(
				"/Users/ragav2/hadoop-1.2.1/conf/core-site.xml"));
		conf2.addResource(new Path(
				"/Users/ragav2/hadoop-1.2.1/conf/hdfs-site.xml"));
		Job job2 = new Job(conf2, "job1");

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * FileSystem fs = FileSystem.get(conf); FSDataInputStream inputStream =
		 * fs.open(new Path(args[0]));
		 */
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		// job.waitForCompletion(true);

		job2.waitForCompletion(true);
		
		
		FileSystem hdfs2 = FileSystem.get(new URI("hdfs://localhost:54310"),
				conf);
		Path hd2 = new Path("hdfs://localhost:54310/Users/ragav2/hdata/op1/part-r-00000");
		Path local2 = new Path("/Users/ragav2/Documents/utilityop1/totalpower.txt");
		hdfs.copyToLocalFile(false, hd2, local2);
		
		
		
		
		
		readfile r = new readfile();
		int[] power = new int[24];
		power = r.gettotalpower();
		int[] threshold = new int[24];
		for(int i=0; i<24; i++){
			threshold[i] = 1120;
		}
		boolean s = true;
		for(int i=0; i<24; i++){
			if(power[i] > threshold[i])
				s=false;
		
		}
		
		
		if(s == true){
			ArrayList<String> names = new ArrayList<String>();
			
			appliance update = new appliance();
			names = update.getStatus();
			String uname;
			for(int i=0; i<names.size(); i++){
			int[] start = update.getcurrents(names.get(i));
			String[] aname = update.getAname(names.get(i));
			uname =  names.get(i);
			
			
			update.putdata(start, aname, uname);
			}
			
			
			
		}
		
		
		
		if(s == false){
		

		// -------------------------------------------------------------------------------------------------------------------------------------------

		Configuration conf3 = new Configuration();
		conf3.addResource(new Path(
				"/Users/ragav2/hadoop-1.2.1/conf/core-site.xml"));
		conf3.addResource(new Path(
				"/Users/ragav2/hadoop-1.2.1/conf/hdfs-site.xml"));
		Job job3 = new Job(conf3, "job2");

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setMapperClass(Map3.class);
		job3.setReducerClass(Reduce3.class);

		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		// job.waitForCompletion(true);

		job3.waitForCompletion(true);
		
		
		FileSystem hdfs3 = FileSystem.get(new URI("hdfs://localhost:54310"),
				conf);
		Path hd3 = new Path("hdfs://localhost:54310/Users/ragav2/hdata/op2/part-r-00000");
		Path local3 = new Path("/Users/ragav2/Documents/utilityop2/splitperuser.txt");
		hdfs.copyToLocalFile(false, hd3, local3);
		
		
		

		// -------------------------------------------------------------------------------------------------------------------------------------------

		Configuration conf4 = new Configuration();
		conf4.addResource(new Path(
				"/Users/ragav2/hadoop-1.2.1/conf/core-site.xml"));
		conf4.addResource(new Path(
				"/Users/ragav2/hadoop-1.2.1/conf/hdfs-site.xml"));
		Job job4 = new Job(conf4, "job2");

		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);

		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);

		job4.setMapperClass(Map4.class);
		job4.setReducerClass(Reduce4.class);

		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job4, new Path(args[3]));
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));
		// job.waitForCompletion(true);
	
		

		job4.waitForCompletion(true);
		
		
		FileSystem hdfs4 = FileSystem.get(new URI("hdfs://localhost:54310"),
				conf);
		Path hd4 = new Path("hdfs://localhost:54310/Users/ragav2/hdata/op3/part-r-00000");
		Path local4 = new Path("/Users/ragav2/Documents/utilityop3/newstart.txt");
		hdfs.copyToLocalFile(false, hd4, local4);
		
		
		
		readfile rr = new readfile();
		int[] start = rr.getnewstart();
		String[] uuname = rr.getname();
		String[] aname = rr.getappname();
		System.out.println(start.length + "   " + uuname.length + "    " + aname.length);

		
		appliance uppdate = new appliance();
		uppdate.putdata(start, aname, uuname);
		
		
		
		
		
		
		
		
		}
		
		

		return 0;
	}

	// -------------------------------------------------------------------------------------------------------------------------------------------
	// -------------------------------------------------------------------------------------------------------------------------------------------

	// ---------------------------------------------------------------------------
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		private Text comp = new Text();
		private Text val = new Text();
		private String name;
		private String v;

		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			if (!(value.toString().equals(null) || value.toString().equals(""))) {

				String[] line = value.toString().split("\t");

				for (int i = 0; i < line.length; i++) {
					if (i == 0)
						name = line[i];

					if (i == 1)
						v = line[i];
				}

				if (!(name.equals("") || name.equals(null)))
					comp.set(name);

				val.set(v);
				// System.out.println(comp);
				output.write(comp, val);
				// System.out.println("MAP1");
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		private Text array;
		private Text name;
		private String appliancename;
		private String power;
		private String starttime;
		private String currentstart;
		private String currentend;
		private String deadline;
		private String runtime;
		private ArrayList<String> start = new ArrayList<String>();
		private ArrayList<String> run = new ArrayList<String>();
		private ArrayList<String> dead = new ArrayList<String>();
		private ArrayList<String> end = new ArrayList<String>();
		private ArrayList<String> powerr = new ArrayList<String>();
		private int x;
		private int y;
		private int z;
		private String data;

		public void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {

			start.clear();
			end.clear();
			powerr.clear();
			dead.clear();
			// System.out.println("reducer START");
			// System.out.println(key);
			// System.out.println(values);
			int[] totalpower = new int[24];
			for (int i = 0; i < 24; i++) {
				totalpower[i] = 0;
			}

			while (values.iterator().hasNext()) {
				String[] temp = values.iterator().next().toString().split(",");

				for (int i = 0; i < temp.length; i++) {
					if (i == 0)
						appliancename = temp[i];
					if (i == 1)
						power = temp[i];
					if (i == 2)
						starttime = temp[i];
					if (i == 3)
						currentstart = temp[i];
					if (i == 4)
						currentend = temp[i];
					if (i == 5)
						deadline = temp[i];
					if (i == 6)
						runtime = temp[i];

				}

				start.add(currentstart);
				end.add(currentend);
				powerr.add(power);
				run.add(runtime);
				dead.add(deadline);

			}

			for (int j = 1; j < start.size(); j++) {
				// for (int i = 1; i <= 24; i++) {

				x = (Integer.parseInt(start.get(j)) / 100);

				//System.out.println(x);
				y = Integer.parseInt(end.get(j))/100;
				//System.out.println(y);
				z = Integer.parseInt(powerr.get(j));
				// System.out.println(key + "   " +x+"   "+y+"   "+z);
				for (int k = x; k < y; k++) {

					//System.out.println(k-1);
					totalpower[k-1] = totalpower[k-1] + z;

				}

			}

			// }
			// System.out.println("aaa");
			data = "";
			for (int k = 0; k < 24; k++) {

				data = data + totalpower[k] + ",";

			}

			// System.out.println(key);
			// System.out.println(data);
			output.write(key, new Text(data));
			// System.out.println("REDUCE1");

		}

	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		// private Text key = new Text();
		private Text val = new Text();
		private String name1;
		private String v;
		private Text k = new Text();

		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {

			String[] line1 = value.toString().split("\t");

			for (int i = 0; i < line1.length; i++) {
				if (i == 0)
					name1 = line1[i];

				if (i == 1)
					v = line1[i];

			}

			String[] line = v.split(",");

			for (int i = 0; i < 24; i++) {

				k.set(new Text(Integer.toString(i + 1)));
				val.set(new Text(line[i]));
				output.write(k, val);
				// System.out.println("MAP2");
			}

		}

		public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

			private Text array;
			private Text name;
			private String data;

			public void reduce(Text key, Iterable<Text> values, Context output)
					throws IOException, InterruptedException {

			

				int value = 0;

				while (values.iterator().hasNext()) {

					value += Integer.parseInt(values.iterator().next()
							.toString());
				}

				// output.write(key, new Text(Integer.toString(value)));
				// System.out.println(key + "    " + value);
				// System.out.print(value+",");
				int i = 0;
				while (Integer.parseInt(key.toString()) != i) {

					if (i == 23)
						break;

					i++;

				}

				
					data = Integer.toString(value);

				output.write(key, new Text(data));
			}

		}
	}

	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		// private Text key = new Text();
		private Text val = new Text();
		private String name;
		private String v;
		private Text comp = new Text();

		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("\t");

			for (int i = 0; i < line.length; i++) {
				if (i == 0)
					name = line[i];

				if (i == 1)
					v = line[i];

			}

			if (!(name.equals("") || name.equals(null)))
				comp.set(name);

			val.set(v);

			output.write(comp, val);

		}

		public static class Reduce3 extends Reducer<Text, Text, Text, Text> {

			private Text array;
			private Text name;
			private String data;

			public void reduce(Text key, Iterable<Text> values, Context output)
					throws IOException, InterruptedException {

				int value = 0;
				data = "";

				while (values.iterator().hasNext()) {

					data = data + values.iterator().next() + "---";
				}

				// output.write(key, new Text(Integer.toString(value)));
				// System.out.println(key + "    " + data);
				// System.out.print(value+",");

				output.write(key, new Text(data));
			}

		}
	}

	public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {
		// private Text key = new Text();
		private Text array;
		private Text name;
		private String s, r, d, e, power;

		private ArrayList<String> start = new ArrayList<String>();
		private ArrayList<String> run = new ArrayList<String>();
		private ArrayList<String> dead = new ArrayList<String>();
		private ArrayList<String> end = new ArrayList<String>();
		private ArrayList<String> powerr = new ArrayList<String>();
		private int x;
		private int y;
		private int z;
		private String n;
		private String data;
		private Text val = new Text();
		private String v;
		private Text comp = new Text();
		private ArrayList<appliance> app = new ArrayList<appliance>();
		private HashMap<Integer, Double> price = new HashMap<Integer, Double>();
		private ArrayList<String> names = new ArrayList<String>();

		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {


			data = "";
			String[] line = value.toString().split("\t");
			for (int i = 0; i < line.length; i++) {

				if (i == 0)
					n = line[i];
				if (i == 1)
					data = line[i];
			}

			appliance a = new appliance();
			names = a.getStatus();
			for(int j = 0; j<names.size(); j++){
			
				if (n.equalsIgnoreCase(names.get(j))) {
					
					// sch.comp(app, price);
					output.write(new Text(n), new Text(data));

				}
			}

		}

		public static class Reduce4 extends Reducer<Text, Text, Text, Text> {

			ArrayList<appliance> app = new ArrayList<appliance>();
			private HashMap<Integer, Double> price = new HashMap<Integer, Double>();
			schedule sch = new schedule();
			readfile p = new readfile();
			private int[] finalbest;
			private String[] appname;

			// String data = "";

			public void reduce(Text key, Iterable<Text> values, Context output)
					throws IOException, InterruptedException {

				String data = "";

				app.clear();
				while (values.iterator().hasNext()) {

					// System.out.println();
					String[] line1 = values.iterator().next().toString()
							.split("---");
					// System.out.println(line1.length);
					appname = new String[line1.length];
					for (int i = 0; i < line1.length; i++) {
						appliance a = new appliance();
						String[] line2 = line1[i].split(",");
						for (int j = 0; j < line2.length; j++) {

							if (j == 0){
								a.setAppliancename(line2[j]);
								appname[i] = line2[j];
							}
							if (j == 1)
								a.setPower(Integer.parseInt(line2[j]));
							if (j == 2)
								a.setStarttime(Integer.parseInt(line2[j]));
							if (j == 3)
								a.setCurrentstart(Integer.parseInt(line2[j]));
							if (j == 4)
								a.setCurrentend(Integer.parseInt(line2[j]));
							if (j == 5)
								a.setDeadline(Integer.parseInt(line2[j]));
							if (j == 6)
								a.setRuntime(Integer.parseInt(line2[j]));
							if(j == 7)
								a.setjobType(line2[j]);
							a.setUsername(key.toString());

						}
						app.add(a);
					}

				}

				finalbest = new int[app.size()];
				price = p.run();
				finalbest = sch.comp1(app, price);
				System.out.println(finalbest.length);
				//System.out.println(finalbest.length + "   " + appname.length);
				for (int i = 0; i < finalbest.length; i++)
					data = data + appname[i] + "-" + finalbest[i] + ",";

				output.write(key, new Text(data));
			}

		}
	}

	public static void main(String[] args) throws Exception {



		while(true){
		
		int itt = 0;
		
		appliance a = new appliance();
		ArrayList<String> names = new ArrayList<String>();
		ArrayList<String> namedone = new ArrayList<String>();
		ArrayList<appliance> ap = new ArrayList<appliance>();
		ArrayList<appliance> app = new ArrayList<appliance>();
		
		boolean b = true;
		
	/*	do{
			
			Thread.sleep(300000);
			
			if(!a.getStatus().isEmpty())
				b = false;
			
			
			
		}while(b);
		*/
		

		System.out.println("waiting");
		
		while(a.getStatus().isEmpty()){
			Thread.sleep(1000);
    		
    	}

		itt = a.getnumber();
		
		while(itt > 0){
			System.out.println("DONE FOR 1 user");
		names = a.getStatus();
		System.out.println(itt + "    "+ names.size());
		
		
		String data = "";
		
		//System.out.println(names.size());
    	for(int i=0; i<names.size() ; i++){
    		System.out.println(names.get(i));

    			ap = a.getdata(names.get(i));
    			for(int j=0; j<ap.size(); j++){
    			//namedone.add(ap.get(i).getUsername());
    			data = data + ap.get(j).getUsername() + "\t"
    					+ ap.get(j).getAppliancename() + "," + ap.get(j).getPower()
    					+ "," + ap.get(j).getStarttime() + ","
    					+ ap.get(j).getHsstarttime() + ","
    					+ (ap.get(j).getHsstarttime() + ap.get(j).getRuntime()*100) + "," + ap.get(j).getDeadline()
    					+ "," + ap.get(j).getRuntime() + "," + ap.get(j).getJobType() + "\n";
    			}

    	}
		
		
		
		app = a.getdata();
		boolean st = true;
		
		//System.out.println(app.size());
		for (int i = 0; i < app.size(); i++) {

			st = true;
			for(int j=0; j<names.size(); j++){
				
				if(app.get(i).getUsername().equalsIgnoreCase(names.get(j)))
					st = false;
				
			}
			//System.out.println(st);
			if(st){
			//System.out.println(i);
			data = data + app.get(i).getUsername() + "\t"
					+ app.get(i).getAppliancename() + "," + app.get(i).getPower()
					+ "," + app.get(i).getStarttime() + ","
					+ app.get(i).getCurrentstart() + ","
					+ app.get(i).getCurrentend() + "," + app.get(i).getDeadline()
					+ "," + app.get(i).getRuntime() + "," + app.get(i).getJobType() + "\n";
			}
			//System.out.println(data);

		}

		PrintWriter out = new PrintWriter(
				"/Users/ragav2/Documents/ip/input.txt");

		out.println(data);
		out.close();

		Configuration configuration = new Configuration();

		InputStream inputStream = new BufferedInputStream(new FileInputStream(
				"/Users/ragav2/Documents/ip/input.txt"));
		// 3. Get the HDFS instance
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:54310"),
				configuration);
		hdfs.delete(new Path(
				"hdfs://localhost:54310/Users/ragav2/hdata/op"), true);
		hdfs.delete(new Path(
				"hdfs://localhost:54310/Users/ragav2/hdata/op1"), true);
		hdfs.delete(new Path(
				"hdfs://localhost:54310/Users/ragav2/hdata/op2"), true);
		hdfs.delete(new Path(
				"hdfs://localhost:54310/Users/ragav2/hdata/op3"), true);
	
		OutputStream outputStream = hdfs
				.create(new Path(
						"hdfs://localhost:54310/Users/ragav2/hdata/ip/input.txt"),
						new Progressable() {
							@Override
							public void progress() {
								System.out.println("....");
							}
						});
		try {
			IOUtils.copyBytes(inputStream, outputStream, 4096, false);
		} finally {
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
		}

		ToolRunner.run(new Configuration(), new server(), args);
		itt--;
		Thread.sleep(5000);

	}
	}
	}

}
