package utility;


import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

public class appliance {
	
	
	private int taskid;
	private String username;
	private String appliancename;
	private int power;
	private int starttime;
	private int currentstart;
	private int currentend;
	private int deadline;
	private int runtime;
	private int jobcompleted;
	private double projectedcomp;
	private int powerconsumed;
	private String jobType;
	private int usstarttime;
	private int hsstarttime;
	Random r = new Random();
	
	//private String[] names = { "r","t","a","b","c" };
	//private String[] aname = { "tv", "ac", "fridge", "oven", "dryer" };
	
	
	
	public appliance(){
		
		//this.taskid = t;
		//this.username = names[t];	
	}
	
	
	
	private static String password = "12020210749343";
	private static String usrname = "root";
	private static String constring = "jdbc:mysql://localhost:3306/utility";
	private static Connection con;
	private static Statement st;
	private static ResultSet rs;
	
	
	
	
	
	public String getUsername(int tid) {
		
		String uname = null;
		
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select username from grid where taskid =" + tid);
			while (rs.next()) {
					uname = rs.getString("username");
	        }
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		return uname;
		
	}
	
	
	public void setUsername(String username) {
		this.username = username;
	}
	
	
	public void setjobType(String jobType) {
		this.jobType = jobType;
	}
	
	public String getJobType() {
		return jobType;
	}	
	
	public String getAppliancename(int tid) {
		String aname = null;
		
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select appliancename from grid where taskid =" + tid);
			while (rs.next()) {
					aname = rs.getString("appliancename");
	        }
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		return aname;
	}
	
	
	
	
	public String[] getAname(String uname) {
		ArrayList<String> a = new ArrayList<String>();	
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select appliancename from grid where username ='" + uname + "'");
			while (rs.next()) {
					a.add(rs.getString("appliancename"));
	        }
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		
		String[] aname = new String[a.size()];
		for(int i=0; i<a.size(); i++){
			
			aname[i] = a.get(i);
			
		}
		
		return aname;
	}
	
	
	
	
	
	
	
	public void setAppliancename(String appliancename) {
		this.appliancename = appliancename;
	}
	
	
	public int getPower(int tid) {
		int p = 0;
		
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select power from grid where taskid =" + tid);
			while (rs.next()) {
					p = rs.getInt("power");
	        }
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		return p;
	}
	
	
	
	
	/*
	public int getPower(int tid) {
		int p = 0;
		
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select power from grid where taskid =" + tid);
			while (rs.next()) {
					p = rs.getInt("power");
	        }
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		return p;
	}
	*/
	
	
	
	
	public int[] getcurrents(String name) {
		ArrayList<Integer> sta = new ArrayList<Integer>();
	
		
		try {
			sta.clear();
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select currentstart from grid where username ='" + name + "'");
			while (rs.next()) {
					sta.add(rs.getInt("currentstart"));
					
	        }
			//i=0;
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		
		int[] p = new int[sta.size()];
		for(int i=0; i<sta.size();i++){
			
			p[i] = sta.get(i);
		}
		return p;
	}
	
	
	
	public void setPower(int power) {
		this.power = power;
	}
	
	
public ArrayList<appliance> getdata() {
	
	//appliance a = new appliance();
	ArrayList<appliance> aparray = new ArrayList<appliance>(); 
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select * from grid ");
			while (rs.next()) {
					appliance a = new appliance();
					a.setAppliancename(rs.getString("appliancename"));
					a.setUsername(rs.getString("username"));
					a.setPower(rs.getInt("power"));
					a.setDeadline(rs.getInt("deadline"));
					a.setCurrentstart(rs.getInt("currentstart"));
					a.setCurrentend(rs.getInt("currentend"));
					a.setRuntime(rs.getInt("runtime"));
					a.setStarttime(rs.getInt("starttime"));
					a.setjobType(rs.getString("jobtype"));
					a.setHsstarttime(rs.getInt("hsstarttime"));
					a.setUsstarttime(rs.getInt("usstarttime"));
					aparray.add(a);
					//System.out.println(a.getStarttime());
	        }
			
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		
		//System.out.println(aparray.get(2).getStarttime());
		/*System.out.println("applaince check");
		for(appliance x : aparray){
			System.out.println(x.getStarttime());
			
			//System.out.println(start[i]);
    	}
		*/
		return aparray;
		
	}
	



public ArrayList<appliance> getdata(String uname) {
	
	//appliance a = new appliance();
	ArrayList<appliance> aparray = new ArrayList<appliance>(); 
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select * from grid where username ='" + uname+"'");
			while (rs.next()) {
					appliance a = new appliance();
					a.setAppliancename(rs.getString("appliancename"));
					a.setUsername(rs.getString("username"));
					a.setPower(rs.getInt("power"));
					a.setDeadline(rs.getInt("deadline"));
					a.setRuntime(rs.getInt("runtime"));
					a.setStarttime(rs.getInt("starttime"));
					a.setjobType(rs.getString("jobtype"));
					a.setHsstarttime(rs.getInt("hsstarttime"));
					a.setUsstarttime(rs.getInt("usstarttime"));
					aparray.add(a);
					//System.out.println(a.getStarttime());
	        }
			
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		
		//System.out.println(aparray.get(2).getStarttime());
		/*System.out.println("applaince check");
		for(appliance x : aparray){
			System.out.println(x.getStarttime());
			
			//System.out.println(start[i]);
    	}
		*/
		return aparray;
		
	}
	
	


public ArrayList<String> getStatus() {
	
	//appliance a = new appliance();
	int commit = 1;
	Date date;
	Time tt;
	Timestamp t;
	String n;

	ArrayList<String> nn = new ArrayList<String>();
	ArrayList<String> names = new ArrayList<String>(); 
	ArrayList<String> realnames = new ArrayList<String>(); 
	ArrayList<Time> times = new ArrayList<Time>();
	ArrayList<Time> ctimes = new ArrayList<Time>();
	ArrayList<Date> dates = new ArrayList<Date>();
	ArrayList<Date> cdates = new ArrayList<Date>();
	HashMap<Time, String> map = new HashMap<Time, String>();
	//System.out.println("asasasas");
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select username, committime from grid where utilitycommit =" + commit);
			while (rs.next()) {
				
				
				//n = rs.getString("username");
				//tt = rs.getTime("committime");
				//map.put(n, tt);
				
					//System.out.println("sadasdasdas");
					names.add(rs.getString("username"));
					//dates.add(rs.getDate("committime"));
					times.add(rs.getTime("committime"));
					//tt = rs.getTime("committime");
					// = new Date(t.getTime());
					//System.out.println(tt);
					
					//System.out.println(a.getStarttime());
	        }
			
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		
		//System.out.println(aparray.get(2).getStarttime());
		/*System.out.println("applaince check");
		for(appliance x : aparray){
			System.out.println(x.getStarttime());
			
			//System.out.println(start[i]);
    	}
		*/
		
		for(int i=0; i<names.size() ; i++){
    		if(i==0){
    			
    			realnames.add(names.get(i));
    			ctimes.add(times.get(i));
    			//cdates.add(dates.get(i));
    			
    		}
   
    		if((i != 0) && !(names.get(i).equalsIgnoreCase(names.get(i-1)))){
    			
    			realnames.add(names.get(i));
    			ctimes.add(times.get(i));
    			//cdates.add(dates.get(i));
    			
    		}
    		
    	}
		
		//System.out.println();
		
		for(int i = 0; i<ctimes.size(); i++){
			map.put(ctimes.get(i), realnames.get(i));
			
		}
		//System.out.println(map.size());
		
		//Time min = ctimes.get(0);		
	
		Collections.sort(ctimes);
		//System.out.println("AFTER SORT");
		if(ctimes.isEmpty()){
			return nn;
		}
		else{
		n = map.get(ctimes.get(0));
		
		nn.add(n);
		//System.out.println(n);
		

		
		return nn;
		}
	}


public int getnumber() {
	
	//appliance a = new appliance();
	int commit = 1;
	int total = 0;
	ArrayList<String> names = new ArrayList<String>(); 
	ArrayList<String> realnames = new ArrayList<String>(); 
		
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			rs = st.executeQuery(" select username from grid where utilitycommit =" + commit);
			while (rs.next()) {
					
					names.add(rs.getString("username"));
					
					//System.out.println(a.getStarttime());
	        }
			
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		
		//System.out.println(aparray.get(2).getStarttime());
		/*System.out.println("applaince check");
		for(appliance x : aparray){
			System.out.println(x.getStarttime());
			
			//System.out.println(start[i]);
    	}
		*/
		
		for(int i=0; i<names.size() ; i++){
    		if(i==0){
    			
    			realnames.add(names.get(i));
    			
    		}
   
    		if((i != 0) && !(names.get(i).equalsIgnoreCase(names.get(i-1)))){
    			
    			realnames.add(names.get(i));
    			
    		}
    		
    	}
		return realnames.size();
		
	}


	
	
	public int getStarttime() {
		return starttime;
	}
	
	public String getUsername() {
		return username;
	}
	
	
	public String getAppliancename() {
		return appliancename;
	}
	public void setStarttime(int startime) {
		this.starttime = startime;
	}
	public int getDeadline() {
		return deadline;
	}
	public void setDeadline(int deadline) {
		this.deadline = deadline;
	}
	public int getRuntime() {
		return runtime;
	}
	public void setRuntime(int runtime) {
		this.runtime = runtime;
	}
	public int getJobcompleted() {
		return jobcompleted;
	}
	public void setJobcompleted(int jobcompleted) {
		this.jobcompleted = jobcompleted;
	}
	public double getProjectedcomp() {
		return projectedcomp;
	}
	public void setProjectedcomp(double projectedcomp) {
		this.projectedcomp = projectedcomp;
	}
	public int getPowerconsumed() {
		return powerconsumed;
	}
	
	public int getPower() {
		return power;
	}
	
	public void setPowerconsumed(int powerconsumed) {
		this.powerconsumed = powerconsumed;
	}


	public int getCurrentstart() {
		return currentstart;
	}


	public void setCurrentstart(int currentstart) {
		this.currentstart = currentstart;
	}


	public int getCurrentend() {
		return currentend;
	}


	public void setCurrentend(int currentend) {
		this.currentend = currentend;
	}
	
	public int getUsstarttime() {
		return usstarttime;
	}


	public void setUsstarttime(int usstarttime) {
		this.usstarttime = usstarttime;
	}
		
	
	
	
	
	
public void putdata(int[] start, int[] end, String[] app, String[] uname) {
		
		for(int i=0; i<start.length; i++){
		try {
			con = DriverManager.getConnection(constring,usrname,password); 
			st = con.createStatement();
			st.executeUpdate(" UPDATE grid  SET committime = NOW(), currentstart="+start[i]+", currentend="+end[i]+" WHERE username='"+uname[i]+"' AND appliancename='"+app[i]+"'");
			
		}
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally{
			 if(st != null && con != null)
				try {
					st.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
		}
		}
		
	}


public void putdata(int[] start,String[] app, String[] uname) {
	
	for(int i=0; i<start.length; i++){
	try {
		con = DriverManager.getConnection(constring,usrname,password); 
		st = con.createStatement();
		st.executeUpdate(" UPDATE grid  SET committime = NOW(), usstarttime="+start[i]+", utilitycommit=0 WHERE username='"+uname[i]+"' AND appliancename='"+app[i]+"'");
		
	}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	finally{
		 if(st != null && con != null)
			try {
				st.close();
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	}
	}
	
}






public void putdata(int[] start,String[] app, String uname) {
	
	for(int i=0; i<start.length; i++){
	try {
		con = DriverManager.getConnection(constring,usrname,password); 
		st = con.createStatement();
		st.executeUpdate(" UPDATE grid  SET usstarttime="+start[i]+", commitstat=0 WHERE username='"+uname+"' AND appliancename='"+app[i]+"'");
		
	}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	finally{
		 if(st != null && con != null)
			try {
				st.close();
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	}
	}
}
	




public int getHsstarttime() {
	return hsstarttime;
}


public void setHsstarttime(int hsstarttime) {
	this.hsstarttime = hsstarttime;
}






public static void main(String[] args){
	
	ArrayList<String> name = new ArrayList<String>();
	//System.out.println("asasdas");
	appliance a = new appliance();
	name = a.getStatus();
	for(int i=0; i<name.size(); i++){
		System.out.println(name.get(i));
	}
	
	
}

	
	
	
	
	
	
	

}
