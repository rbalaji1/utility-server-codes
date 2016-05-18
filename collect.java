package utility;

import java.util.ArrayList;
import java.util.HashMap;

public class collect {

	
    
    public static void main(String[] args) {
    	    	
    	schedule sch = new schedule();
    	
    	appliance app = new appliance();
    	readfile p = new readfile();
    	ArrayList<String> names = new ArrayList<String>();

    	ArrayList<appliance> aparray = new ArrayList<appliance>();
    	ArrayList<appliance> aparray1 = new ArrayList<appliance>();
    	HashMap<Integer, Double> price = new HashMap<Integer, Double>();

    		aparray = app.getdata("r");
    		aparray1 = aparray;
    		price = p.run();
    		sch.comp(aparray, price);
    	for(int i = 0; i<aparray.size(); i++){
		//	System.out.println(aparray.get(i).getStarttime());
			
    	}
    	

    	
	}
    
    
	
	
	
}
