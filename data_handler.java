import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.List;
import java.util.Enumeration;
import java.net.URI; 
class Data_handler {
	public static void main (String argsp[])throws Exception
	{
		BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/Users/parth/Desktop/Advanced_DB/final/mnist_train.txt"))));
            String x;   
            PrintWriter writer = new PrintWriter("trainsmall.txt", "UTF-8");
            int c1 = 0;
        while((x = r.readLine())!= null) {
        	if (c1 <= 500)
        	{
        	writer.println(x);
        	}
        	c1 += 1 ; 
          }
          r.close();
writer.close();

        }		
	}
