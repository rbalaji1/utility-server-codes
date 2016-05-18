package utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class schedule {

	private int st;
	private int rt;
	private int dl;
	private double stprice;
	private int power;
	private int in;
	private String jt;

	private double temp = 10000;
	private boolean frozen = false;
	private double de = 0.0;
	private double newcf = 0.0;
	private double totalcf = 0.0;

	public double calcf(ArrayList<appliance> a, int[] start,
			HashMap<Integer, Double> p, int[] init) {

		totalcf = 0.0;

		 double addpenalty = 0 ;	
		 double penalty = 1;
		 
		double[] cf = new double[a.size()];
		for (int i = 0; i < a.size(); i++) {
			cf[i] = 0.0;

			st = start[i];

			rt = a.get(i).getRuntime();

			dl = a.get(i).getDeadline();

			power = a.get(i).getPower();
			
			jt = a.get(i).getJobType();
			
			in = init[i];

			if(jt.equals("soft")){
				//System.out.println("Soft calculation");
				addpenalty =0 ;
				
				for (int j = 0; j < rt; j++) {
					// System.out.println(st);
					cf[i] = cf[i] + (p.get(st + (j * 100)) * power);

				}
				
				if( st >= in && st <= dl)
				{
					if(!((rt*100) < (dl- st)))
					{
						addpenalty = (((rt*100) - (dl - st))/100) * penalty;
					}
				}
				else if( st >= dl){
						addpenalty =  rt * penalty ;
				}
				
				
				cf[i] = cf[i] + addpenalty;
				
			}
			else{
	

					for (int j = 0; j < rt; j++) {		
						
						cf[i] = cf[i] + (p.get(st + (j * 100)) * power);
		
					}
			}
			totalcf = totalcf + cf[i];

		}

		return totalcf;

	}

	public int[] changesch(int[] start, int[] runt, int[] deadt, int[] init, String[] jobty) {

		Random r = new Random();
		int next = 0;
		int d = 0, rn = 0, s = 0, in = 0;
		int val = 0;
		int ran = 0;
		String jb;

		for (int i = 0; i < start.length; i++) {
			s = start[i] / 100;
			in = init[i] / 100;

			rn = runt[i] / 100;

			d = deadt[i] / 100;
			jb = jobty[i];
			
			if(jb.equals("soft")){
				d = 24;  
			}

			val = d - rn + 1;
			// System.out.println(in + "    -    " +val);
			ran = (in + r.nextInt(val - in)) * 100;
			// System.out.println("    "+ran);

			start[i] = ran;
			// System.out.println(start[i]);

			val = 0;
		}

		return start;
	}
	
	
	
	
	public boolean checkpower(int[] start, int[] dead, int[] run, int[] power){
		
		readfile read = new readfile();
		
		boolean stat = true;
		
		int[] powerfor1 = new int[24];
		int[] powerindv = new int[24];
		int[] prevpower = new int[24];
		int[] totalpower = new int[24];
		
		int threshold[] = new int[24];
		
		for(int i=0; i<24; i++){
			powerfor1[i] = 0;
			powerindv[i] = 0;
			totalpower[i] = 0;
			prevpower[i] = 0;
			threshold[i] = 1120;
		}
		int s = 0, d = 0, r = 0, p = 0;
		
		
		prevpower = read.gettotalpower();
		powerindv = read.getindvpower();
		
	
		//System.out.println(start.length);
		for(int i=0; i<start.length;i++){
			
			s = start[i]/100;
			d = dead[i]/100;
			r = run[i]/100;
			p = power[i];
			for(int j = s; j<(s + r); j++){
				
				
				powerfor1[j] = powerfor1[j] + p;
				//System.out.println(powerfor1[j]);
				
			}
			
		}
		for(int j=0; j<24; j++){
			//System.out.println(totalpower[j]);
			
			totalpower[j] = (prevpower[j] - powerindv[j]) + powerfor1[j];
		}
		
		for(int j=0; j<24; j++){
			//System.out.println(totalpower[j] + "     " + threshold[j]);
			
			if(totalpower[j] > threshold[j])
				stat = false;
			
		}
		return stat;
		
			
	}
	
	
	
	public int[] comp1(ArrayList<appliance> a, HashMap<Integer, Double> p) {

		int[] start = new int[a.size()];
		int[] runt = new int[a.size()];
		int[] deadt = new int[a.size()];
		int[] init = new int[a.size()];
		String[] app = new String[a.size()];
		String[] uname = new String[a.size()];
		
		int[] newstart = new int[a.size()];
		int[] best = new int[a.size()];
		int[] bests = new int[a.size()];
		int[] beste = new int[a.size()];
		int[] power = new int[a.size()];
		int[] finalbest = new int[a.size()];
		String[] jobty = new String[a.size()];
		int loop = 0;
		double ex = 0.0;
		double ncf = 0.0;
		double cc = 0.0;
		double finalcf = 0.0;
		int rand = 0;

		boolean status = true;
		
		double bestc = 0.0;
		Random r = new Random();

		for (int i = 0; i < a.size(); i++) {
			start[i] = a.get(i).getStarttime();
			init[i] = a.get(i).getStarttime();
			newstart[i] = a.get(i).getStarttime();
			
			app[i] = a.get(i).getAppliancename();
			
			uname[i] = a.get(i).getUsername();

			runt[i] = a.get(i).getRuntime() * 100;
			deadt[i] = a.get(i).getDeadline();
			power[i] = a.get(i).getPower();
			jobty[i] = a.get(i).getJobType();
		}

		cc = calcf(a, start, p, init);
		// System.out.println(cc);

		while (temp > 0.1) {
			for (int m = 0; m < 100; m++) {

				newstart = changesch(newstart, runt, deadt, init, jobty);
				ncf = calcf(a, newstart, p, init);

				de = ncf - cc;
				

				if (de < 0) {
					
					if(checkpower(newstart, deadt, runt, power)){
						
						bests = newstart;
						cc = ncf;
						finalbest = newstart;
						finalcf = ncf;
						
					}
					else{
						
						bests = newstart;
						cc = ncf;
					}
					
				}
				/*else {
					
					ex = Math.exp(de / temp);
					rand = r.nextInt();
					//System.out.println(de + "  "+ temp + "  " + ex + "    -    " + rand + "     =   "+ cc);
					if (rand < ex) {
						best = newstart;
						cc = ncf;
					} else{
						best = best;
						cc = cc;
					}
					
				}*/
				loop++;

			}
			temp = temp-0.1;
			
		}

		for (int i = 0; i < bests.length; i++) {
			beste[i] = bests[i] + runt[i];
			if(finalbest[i] == 0)
				status = false;
		}
		
		if(status == false)
			finalbest = bests;
		
		
		
		for (int i = 0; i < bests.length; i++) {
			System.out.println(finalbest[i]);
		}
		System.out.println(cc);
		/*
		appliance x = new appliance();
		
		x.putdata(bests, beste, app, uname);
		
		System.out.println(loop);*/
		System.out.println(cc);
		return finalbest;
	}

	
	
	
	

	public void comp(ArrayList<appliance> a, HashMap<Integer, Double> p) {

		int[] start = new int[a.size()];
		int[] runt = new int[a.size()];
		int[] deadt = new int[a.size()];
		int[] init = new int[a.size()];
		String[] app = new String[a.size()];
		String[] uname = new String[a.size()];
		
		int[] newstart = new int[a.size()];
		int[] best = new int[a.size()];
		int[] bests = new int[a.size()];
		int[] beste = new int[a.size()];
		int[] power = new int[a.size()];
		int[] finalbest = new int[a.size()];
		String[] jobty = new String[a.size()];
		int loop = 0;
		double ex = 0.0;
		double ncf = 0.0;
		double cc = 0.0;
		double finalcf = 0.0;
		int rand = 0;

		boolean status = true;
		
		double bestc = 0.0;
		Random r = new Random();

		for (int i = 0; i < a.size(); i++) {
			start[i] = a.get(i).getStarttime();
			init[i] = a.get(i).getStarttime();
			newstart[i] = a.get(i).getStarttime();
			
			app[i] = a.get(i).getAppliancename();
			
			uname[i] = a.get(i).getUsername();

			runt[i] = a.get(i).getRuntime() * 100;
			deadt[i] = a.get(i).getDeadline();
			power[i] = a.get(i).getPower();
			jobty[i] = a.get(i).getJobType();
		}

		cc = calcf(a, start, p, init);
		// System.out.println(cc);

		while (temp > 0.1) {
			for (int m = 0; m < 100; m++) {

				newstart = changesch(newstart, runt, deadt, init, jobty);
				ncf = calcf(a, newstart, p, init);

				de = ncf - cc;
				

				if (de < 0) {
					
					if(checkpower(newstart, deadt, runt, power)){
						
						bests = newstart;
						cc = ncf;
						finalbest = newstart;
						finalcf = ncf;
						
					}
					else{
						
						bests = newstart;
						cc = ncf;
					}
					
				}
				/*else {
					
					ex = Math.exp(de / temp);
					rand = r.nextInt();
					//System.out.println(de + "  "+ temp + "  " + ex + "    -    " + rand + "     =   "+ cc);
					if (rand < ex) {
						best = newstart;
						cc = ncf;
					} else{
						best = best;
						cc = cc;
					}
					
				}*/
				loop++;

			}
			temp = temp-0.1;
			
		}

		for (int i = 0; i < bests.length; i++) {
			beste[i] = bests[i] + runt[i];
			if(finalbest[i] == 0)
				status = false;
		}
		
		if(status == false)
			finalbest = bests;
		
		
		
		for (int i = 0; i < bests.length; i++) {
			System.out.println(finalbest[i]);
		}
		System.out.println(cc);
		/*
		appliance x = new appliance();
		
		x.putdata(bests, beste, app, uname);
		
		System.out.println(loop);
		System.out.println(cc);*/
	}

}
