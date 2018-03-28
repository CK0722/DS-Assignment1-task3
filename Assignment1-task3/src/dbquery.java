
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

//dbquery class,query text in the heap file
public class dbquery {
	char buffer[];   //page buffer
	int pageSize;    //page size
	String text;     //search text
	
	public static void main(String[] args) {//entry function
		if(args.length<2) {
			System.out.println("command format error");
			return;
		}
		File heapFile=new File("heap."+args[args.length-1]);
        if(!heapFile.exists()) {
        	System.out.println("error,heap file is not exist");
            return;
        }
		dbquery query=new dbquery();
		query.text="";
		for(int i=0;i<args.length-1;i++)    //get search text from the arguments
			query.text=query.text+args[i]+" ";
		int length=query.text.length()-1;
		query.text=query.text.substring(0, length);
		query.pageSize=Integer.parseInt(args[args.length-1]);
		query.init();       
		query.queryHeap();         //query text in the heap file
	}
	void init(){   //initialize the buffer
		buffer=new char[pageSize];
		for(int i=0;i<pageSize;i++)
    		buffer[i]=0;
	}
	void queryHeap() {//query text from the heap file
		try {
			long startTime=System.currentTimeMillis();   //start time
            BufferedReader reader = new BufferedReader(new FileReader("heap."+pageSize));
            while(reader.read(buffer,0,pageSize)!=-1){   //read one page from the heap file 
            	queryPage();        //query text from the page
                for(int i=0;i<pageSize;i++)  //reset buffer
            		buffer[i]=0;
            }
            reader.close();
            long endTime=System.currentTimeMillis();     //end time
            long totalTime=endTime-startTime;
            System.out.println("the total time taken to do all the search operations in milliseconds is "+totalTime);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}	
	void queryPage() {//query text from the page buffer
		int numPerPage=pageSize/275;  //number of records in one page
		String name;
		int pos1;
		int pos2;
		for(int i=0;i<numPerPage;i++) {
			name="";
			pos1=i*275;//record start position in the page buffer
			pos2=pos1+200;
			for(int j=pos1;j<pos2;j++) {
				if(buffer[j]==0)
					break;
				name=name+buffer[j];
			}
			if(name.contains(text)) { //if the BN_NAME contains the search text				
				pos2=pos1+275;
				for(int k=pos1;k<pos2;k++) {//print the record
					if(buffer[k]!=0)
						System.out.print(buffer[k]);
					else System.out.print(" ");
				}
				System.out.println();
			}
		}
	}
}
