import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements java.io.Serializable
{
	Vector<Hashtable> enteries;
	int maxNoEnteries;//get maxNoEnteries from file
	int noOfEnteries;
	
	public Page()
	{
		noOfEnteries = 0;
        try {
            String configFilePath = "resources/DBApp.config";
            FileInputStream propsInput = new FileInputStream(configFilePath);
            Properties prop = new Properties();
            prop.load(propsInput);
            maxNoEnteries = Integer.parseInt(prop.getProperty("maxNoEnteriesimumRowsCountinPage"));
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	public static void main(String[]args)
	{
		Page page = new Page();
		System.out.println(page.maxNoEnteries);
	}
	
}
