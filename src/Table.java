import java.io.Serializable;
import java.util.Vector;

public class Table implements java.io.Serializable{
	public Vector<String> pageNames; //
	public String filepath; //location all pages
	public String name; //table name
	

	
	public Table(String name, String filepath)
	{
		this.name = name;
		this.filepath = filepath;
		this.pageNames = new Vector<String>();
	}
}
