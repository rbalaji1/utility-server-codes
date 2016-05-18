package utility;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class readfile {

	public static void main(String[] args) {

		readfile obj = new readfile();
		obj.run();

	}

	public int[] gettotalpower() {

		String File = "/Users/ragav2/Documents/utilityop1/totalpower.txt";
		BufferedReader br = null;
		String line = "";
		int h = 0;
		int pow = 0;
		int[] totalpower = new int[24];
		for (int i = 0; i < 24; i++) {

			totalpower[i] = 0;
		}

		int j;

		try {

			br = new BufferedReader(new FileReader(File));
			// System.out.println("==========");
			for (int i = 0; i < 24; i++) {

				String txt = br.readLine();
				// System.out.println(txt);

				String[] data = txt.split("\t");
				for (int k = 0; k < data.length; k++) {
					if (k == 0)
						h = Integer.parseInt(data[k]);
					if (k == 1)
						pow = Integer.parseInt(data[k]);

				}
				// if(){
				totalpower[h - 1] = pow;
				// System.out.println(h);
				// System.out.println(totalpower[i]);
			}

			/*
			 * for(int l =0; l<24; l++){ System.out.println(totalpower[l]); }
			 */

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return totalpower;

	}

	public int[] getnewstart() {

		String File = "/Users/ragav2/Documents/utilityop3/newstart.txt";
		BufferedReader br = null;
		String line = "";
		String name = "";
		int h = 0;
		int pow = 0;
		ArrayList<Integer> newstart = new ArrayList<Integer>();
		ArrayList<String> names = new ArrayList<String>();

		try {

			br = new BufferedReader(new FileReader(File));
			// System.out.println("==========");
			String txt;
			while((txt = br.readLine()) != null){

				//String txt = br.readLine();
				// System.out.println(txt);
				newstart.clear();
				String[] data = txt.split("\t");
				
				for (int k = 0; k < data.length; k++) {
					if (k == 0){
						name = data[k];
						names.add(name);
					}
					if (k == 1) {

						

						String[] st = data[k].split(",");
						for (int j = 0; j < st.length; j++) {
							String[] stt = st[j].split("-");
							for(int l =0; l<stt.length; l++){
								
								if(l==1)
									newstart.add(Integer.parseInt(stt[l]));
								
								
							}

							

						}

							

						
					}

				}
			
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		int[] news = new int[newstart.size()];
		for( int i =0; i< newstart.size(); i++){
			news[i] = newstart.get(i);
		}
		return news;

	}
	
	
	
	
	public String[] getname() {

		String File = "/Users/ragav2/Documents/utilityop3/newstart.txt";
		BufferedReader br = null;
		String line = "";
		String name = "";
		int h = 0;
		int pow = 0;
		ArrayList<Integer> newstart = new ArrayList<Integer>();
		ArrayList<String> names = new ArrayList<String>();

		try {

			br = new BufferedReader(new FileReader(File));
			// System.out.println("==========");
			String txt;
			while((txt = br.readLine()) != null){

				//String txt = br.readLine();
				// System.out.println(txt);

				newstart.clear();
				names.clear();
				String[] data = txt.split("\t");
				
				for (int k = 0; k < data.length; k++) {
					if (k == 0){
						name = data[k];
						//names.add(name);
					}
					if (k == 1) {

						String[] st = data[k].split(",");
						for (int j = 0; j < st.length; j++) {
							names.add(name);
							String[] apname = st[j].split("-");
							for(int l =0; l<apname.length; l++){
								
								//if(l==0)
									//anames.add(apname[l]);
								
								
							}

							

						}
					}

				}
			
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		
		String[] n = new String[names.size()];
		for( int i =0; i< names.size(); i++){
			n[i] = names.get(i);
		}
		return n;

	}
	
	
	
	
	
	public String[] getappname() {

		String File = "/Users/ragav2/Documents/utilityop3/newstart.txt";
		BufferedReader br = null;
		String line = "";
		String name = "";
		int h = 0;
		int pow = 0;
		ArrayList<Integer> newstart = new ArrayList<Integer>();
		ArrayList<String> names = new ArrayList<String>();
		ArrayList<String> anames = new ArrayList<String>();

		try {

			br = new BufferedReader(new FileReader(File));
			// System.out.println("==========");
			String txt;
			while((txt = br.readLine()) != null){

				//String txt = br.readLine();
				// System.out.println(txt);

				newstart.clear();
				anames.clear();
				names.clear();
				String[] data = txt.split("\t");
				
				
				for (int k = 0; k < data.length; k++) {
					if (k == 0){
						name = data[k];
						names.add(name);
					}
					if (k == 1) {

						String[] st = data[k].split(",");
						for (int j = 0; j < st.length; j++) {
							String[] apname = st[j].split("-");
							for(int l =0; l<apname.length; l++){
								
								if(l==0)
									anames.add(apname[l]);
								
								
							}

							

						}
					}

				}
			
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		String[] an = new String[anames.size()];
		for( int i =0; i< anames.size(); i++){
			an[i] = anames.get(i);
		}
		return an;
	}
	
	

	
	

	public String[] getString() {

		String File = "/Users/ragav2/Documents/utilityop1/totalpower.txt";
		BufferedReader br = null;
		String line = "";
		int h = 0;
		int pow = 0;
		String stat = "";
		int[] totalpower = new int[24];
		for (int i = 0; i < 24; i++) {

			totalpower[i] = 0;
		}
		String[] status = new String[24];
		for (int i = 0; i < 24; i++) {

			status[i] = "";
		}

		int j;

		try {

			br = new BufferedReader(new FileReader(File));
			// System.out.println("==========");
			for (int i = 0; i < 24; i++) {

				String txt = br.readLine();
				// System.out.println(txt);

				String[] data = txt.split("\t");
				for (int k = 0; k < data.length; k++) {
					if (k == 0)
						h = Integer.parseInt(data[k]);
					if (k == 1)
						pow = Integer.parseInt(data[k]);
					if (k == 2)
						stat = data[k];

				}
				// if(){
				totalpower[h - 1] = pow;
				status[h - 1] = stat;
				// System.out.println(h);
				// System.out.println(totalpower[i]);
			}

			/*
			 * for(int l =0; l<24; l++){ System.out.println(totalpower[l]); }
			 */

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return status;

	}

	public int[] getindvpower() {

		String File = "/Users/ragav2/Documents/utilityop/powerintime.txt";
		BufferedReader br = null;
		String line = "";
		String name = "";
		String powers = "";

		int h = 0;

		int[] indvpower = new int[24];
		for (int i = 0; i < 24; i++) {

			indvpower[i] = 0;
		}

		try {

			// System.out.println("==========");
			br = new BufferedReader(new FileReader(File));
			for (int i = 0; i < 5; i++) {
				String txt = br.readLine();

				String[] data = txt.split("\t");

				for (int k = 0; k < data.length; k++) {
					if (k == 0)
						name = data[k];
					if (k == 1)
						powers = data[k];

				}

				if (name.equals("r")) {
					String[] pow = powers.split(",");
					for (int j = 0; j < 24; j++) {
						indvpower[j] = Integer.parseInt(pow[j]);
						// System.out.println(indvpower[j]);

					}

				}

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return indvpower;

	}

	public HashMap<Integer, Double> run() {

		String csvFile = "/Users/ragav2/Downloads/20151201-da.csv";
		BufferedReader br = null;
		String line = "";
		ArrayList<String> keya = new ArrayList<String>();
		String[] key = new String[26];
		String[] value = new String[26];
		HashMap<Integer, Double> price = new HashMap<Integer, Double>();
		int j;

		try {

			br = new BufferedReader(new FileReader(csvFile));
			for (int i = 0; i < 4; i++) {
				String txt = br.readLine();
				// System.out.println(txt);
				if (i == 2)
					key = txt.split(",");
				if (i == 3)
					value = txt.split(",");
			}

			for (int i = 1; i < 25; i++) {

				price.put(Integer.parseInt(key[i]),
						Double.parseDouble(value[i]));

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return price;
	}

}