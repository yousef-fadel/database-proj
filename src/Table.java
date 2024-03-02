import java.util.Vector;

public class Table {
	Vector<Page> pages;
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getNoOfPages() {
		return noOfPages;
	}

	public void setNoOfPages(int noOfPages) {
		this.noOfPages = noOfPages;
	}

	private String name;
	private int noOfPages;
	
	public Table(String name)
	{
		this.name = name;
		noOfPages = 0;
	}
}
