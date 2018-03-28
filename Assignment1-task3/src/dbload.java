
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

//dbload class,load dataset file to heap file
public class dbload {
	char buffer[];  //page buffer
	int pageSize;   //page size
	int count;      //record count
	
	public static void main(String[] args) {  //entry function
		if(args.length!=3) {
			System.out.println("command format error");
			return;
		}
		if(!args[0].equals("-p")) {
			System.out.println("command format error");
			return;
		}
		File dbSetFile=new File(args[2]);
        if(!dbSetFile.exists()) {
        	System.out.println("error,data set file is not exist");
            return;
        }
		dbload loader=new dbload();
		loader.pageSize=Integer.parseInt(args[1]);
		loader.init();   //loader initialize
		loader.loadToHeap(args[2]);   //load from dataset file to heap file
	}
	
	void init(){   //initialize page buffer
		buffer=new char[pageSize];
		for(int i=0;i<pageSize;i++)
    		buffer[i]=0;
	}

	void loadToHeap(String dbSetFileName) {//load from dataset file to heap file
        String str="",str1="",str2 ="",str3="",str4 ="",str5="",str6 ="",str7="",str8="";
        try {
        	long startTime=System.currentTimeMillis();   //start time
            BufferedReader reader = new BufferedReader(new FileReader(dbSetFileName));  //dataset file
            count=0;
            int numPerPage=pageSize/275;        //the number of records in one page
            BufferedWriter writer = new BufferedWriter(new FileWriter("heap."+pageSize));//heap file
            reader.readLine();    //read headline
            int pageNumber=0;
            int recordNumber=0;     
            while((str = reader.readLine())!=null){
                int pos=str.indexOf("\t");
                str=str.substring(pos+1);
                pos=str.indexOf("\t");
                str1=str.substring(0,pos);   //read fields of a record
                str=str.substring(pos+1);
                pos=str.indexOf("\t");
                str2=str.substring(0,pos);
                str=str.substring(pos+1);
                pos=str.indexOf("\t");
                str3=str.substring(0,pos);
                str=str.substring(pos+1);
                pos=str.indexOf("\t");
                str4=str.substring(0,pos);
                str=str.substring(pos+1);
                pos=str.indexOf("\t");
                str5=str.substring(0,pos);
                str=str.substring(pos+1);
                pos=str.indexOf("\t");
                str6=str.substring(0,pos);
                str=str.substring(pos+1);
                pos=str.indexOf("\t");
                str7=str.substring(0,pos);
                str8=str.substring(pos+1); 
                int from=count*275;
                copyToBuffer(from,str1);    //copy fields to buffer
                copyToBuffer(from+200,str2);
                copyToBuffer(from+212,str3);
                copyToBuffer(from+222,str4);
                copyToBuffer(from+232,str5);
                copyToBuffer(from+242,str6);
                copyToBuffer(from+252,str7);
                copyToBuffer(from+255,str8);
                count++;
                if(count==numPerPage) {    //if read one page        	
                	writer.write(buffer);  //write to heap file
                	for(int i=0;i<pageSize;i++)  //reset buffer 
                		buffer[i]=0;
                	count=0;
                	pageNumber++;  
                }
            }
            recordNumber=pageNumber*numPerPage+count;  //total record number
            if(count!=0) {  //if the last page is not full
            	writer.write(buffer);
            	pageNumber++;
            }
            writer.close();
            reader.close();            
            long endTime=System.currentTimeMillis(); //end time
            long totalTime=endTime-startTime;
            System.out.println("the number of records loaded is "+recordNumber);
            System.out.println("the number of pages used is "+pageNumber);
            System.out.println("the number of milliseconds to create the heap file is "+totalTime);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	void copyToBuffer(int from,String str) {//copy the string str to the page buffer from the 'from' position 
		int len=str.length();		
		for(int i=0;i<len;i++) 
			buffer[from+i]=str.charAt(i);		
	}
}
