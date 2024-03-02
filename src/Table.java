import java.util.Vector;

public class Table {
	Vector<Page> pages;
	String name;
	int noOfPages;
	
	public Table(String name)
	{
		this.name = name;
		noOfPages = 0;
	}
}
