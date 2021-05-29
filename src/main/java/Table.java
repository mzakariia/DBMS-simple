import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Table implements Serializable {

	/**
	 * 
	 */
	private String tableName, clusteringKeyColumn, clusteringKeyType, path;
	private int maxPageSize, nextPageIdx = 1;
	private Vector<String> pages;
	private Vector<Object> maxKey;

	/*
	 * Constructor
	 */
	/**
	 * Constructor for the table
	 * @param strTableName the name of the table
	 * @param strClusteringKeyColumn the name of the column that would be used as a primary and clustering key
	 * @param htblColNameType key-value pairs of column names and their data types
	 * @param strMainDir path of the main directory of the DB engine to save resources
	 * @param intMaxPageSize the maximum number of tuples allowed in a page
	 * @throws IOException when save is not successful
	 */
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			String strMainDir, int intMaxPageSize) throws IOException {

		this.path = strMainDir + "data/" + strTableName + "/";
		this.tableName = strTableName;
		this.clusteringKeyColumn = strClusteringKeyColumn;
		this.clusteringKeyType = htblColNameType.get(strClusteringKeyColumn);
		this.maxPageSize = intMaxPageSize;

		this.pages = new Vector<String>();
		this.maxKey = new Vector<Object>();

		createDirectories();
		save();
	}

	/*
	 * Main methods
	 */
	/**
	 * Inserts the new tuple in its appropriate position in the table. Has support for overflow pages
	 * @param htblColNameValue key-value pairs representing the tuple to be inserted
	 * @throws IOException when save/load is not successful
	 * @throws ClassNotFoundException when load is not successful
	 * @throws DBAppException if a tuple already exists with the clustering key wanted to be inserted
	 */
	public void insertWithOF(Hashtable<String, Object> htblColNameValue)
			throws IOException, ClassNotFoundException, DBAppException {
		if (pages.size() == 0) { // first insert so we create a new page and we insert in it blindly
			Page page = createPage();
			page.insert(htblColNameValue);

			// adds `page reference` at the end of the pages vector
			pages.add(tableName + "_" + nextPageIdx + ".class");

			// updates the max key vector
			maxKey.add(htblColNameValue.get(clusteringKeyColumn));

			nextPageIdx++;
			
		} else { // if not first insert then we need to find correct insert page using binary
					// search
			// gets inserted key as comparable
			Comparable value = getComparable(htblColNameValue.get(clusteringKeyColumn), clusteringKeyType);

			// binary search using max key in each page
			int lo = 0, hi = pages.size() - 1, res = -1;
			while (lo <= hi) {
				int mid = lo + (hi - lo) / 2;
				// gets max key in page as comparable
				Comparable max = getComparable(maxKey.get(mid), clusteringKeyType);
				if (value.compareTo(max) <= 0) {
					res = mid;
					hi = mid - 1;
				} else {
					lo = mid + 1;
				}
			}

			if (res != -1) { // if a page with a greater key is found then it is the insert page
				// load target page and get the insert index within page
				Page page = getPage(res);
				int insIdx = page.getInsertIdx(htblColNameValue.get(clusteringKeyColumn));

				// inserts the tuple in its position within page and gets the kicked out tuple
				// if the page was already full
				Tuple outTuple = page.insert(htblColNameValue, insIdx);

				// updates the max key of the page by getting the last tuple in the sorted page
				maxKey.set(res, page.getLast().getClusteringKeyValue());
				
				if (outTuple != null) { // if there is a tuple which was kicked out of the full page
					if (res == pages.size() - 1) { // if last page was the page that kicked out the tuple then create a
													// new page to insert tuple

						// create a new page and insert blindly
						Page newPage = createPage();
						newPage.insert(outTuple.getValues());

						// updates the max key in the table
						maxKey.add(outTuple.getClusteringKeyValue());

						pages.add(tableName + "_" + nextPageIdx + ".class");

						nextPageIdx++;
					} else { // if page in `middle` of table was the page that kicked the tuple
						// load in next page and check if it contains a free position
						Page nextPage = getPage(res + 1);

						if (!nextPage.isFull()) { // if next page contains a free space then insert at the beginning of
													// the page
							nextPage.insert(outTuple.getValues(), 0);
						} else { // if next page is full then create an overflow page to insert the kicked out
									// tuple

							// create new page and insert in it blindly
							Page newPage = createPage();
							newPage.insert(outTuple.getValues());

							// update the max key table
							maxKey.add(res + 1, outTuple.getClusteringKeyValue());
							pages.add(res + 1, tableName + "_" + nextPageIdx + ".class");
							nextPageIdx++;
						}
					}
				}
			} else { // if no page with greater key is found then insert in last page

				// load last page in table
				Page page = getPage(pages.size() - 1);
				if (!page.isFull()) { // if page has empty space then insert at its end and update maxKey
					page.insert(htblColNameValue);
					maxKey.set(maxKey.size() - 1, htblColNameValue.get(clusteringKeyColumn));
				} else { // if last page is full then create a new page at the end and insert tuple in it
							// blindly
					Page newPage = createPage();
					newPage.insert(htblColNameValue);
					pages.add(tableName + "_" + nextPageIdx + ".class");
					maxKey.add(htblColNameValue.get(clusteringKeyColumn));
					nextPageIdx++;
				}
			}
		}

		// saves the table to disk after every insert
		save();
	}

	/**
	 * updates the tuple with the passed clustering key. Supports binary search
	 * @param clusteringKeyValue value of the clustering key of the tuple that needs to be updated
	 * @param htblColNameValue key-value pairs of each column name and its new value
	 * @throws ClassNotFoundException when loading is not successful
	 * @throws IOException when I/O failure occurs
	 */
	public void updateBS(Comparable clusteringKeyValue, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {
		// binary search using the clustering key column to find page index if it exists
		int lo = 0, hi = pages.size() - 1, res = -1;
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			Comparable max = getComparable(maxKey.get(mid), clusteringKeyType);
			if (clusteringKeyValue.compareTo(max) <= 0) {
				res = mid;
				hi = mid - 1;
			} else {
				lo = mid + 1;
			}
		}

		if (res != -1) { // if page index is found then load it to memory and update it
			Page page = getPage(res);
			page.update(clusteringKeyValue, htblColNameValue);
		} else { // if not then no tuple exists with this clustering key
			System.out.println("No such record exist");
		}
	}

	/**
	 * Deletes all tuples in table with matching criteria passed. Supports binary search if clustering key is passed, otherwise linear search is used
	 * @param htblColNameValue key-value pairs representing the criteria of deletion
	 * @throws IOException when I/O failure occurs
	 * @throws ClassNotFoundException when loading fails
	 */
	public void deleteBS(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException {
		if (htblColNameValue.containsKey(clusteringKeyColumn)) { // do binary search if clustering key value is provided
			// binary search using clustering key value
			int lo = 0, hi = pages.size() - 1, res = -1;
			Comparable value = getComparable(htblColNameValue.get(clusteringKeyColumn), clusteringKeyType);
			while (lo <= hi) {
				int mid = lo + (hi - lo) / 2;
				Comparable max = getComparable(maxKey.get(mid), clusteringKeyType);
				if (value.compareTo(max) <= 0) {
					res = mid;
					hi = mid - 1;
				} else {
					lo = mid + 1;
				}
			}
			
			if (res != -1) { // a page that should contain the tuple exists
				// loag page and delete the tuple in it if it exists
				Page page = getPage(res);
				page.delete(htblColNameValue.get(clusteringKeyColumn));
				
				// if page becomes empty after deletion then delete the page from disk
				if (page.isEmpty()) {
					deletePages(res, 1);
				}
			}
		} else { // do linear search
			ObjectInputStream ois = null;
			// loop over available pages
			for (int i = 0; i < pages.size(); ++i) {
				// load page to memory
				ois = new ObjectInputStream(new FileInputStream(path + pages.get(i)));
				Page page = (Page) ois.readObject();
				
				// delete tuples in page with corresponding values
				page.delete(htblColNameValue);
				
				// if page becomes empty after deletion then delete the page from disk
				if (page.isEmpty()) {
					deletePages(i, 1);
					i--;
				}
				ois.close();
			}
		}
	}
	

	/*
	 * HELPER METHODS
	 */

	private void deletePages(int stIdx, int count) throws IOException {
		for (int i = 0; i < count; ++i) {
			File f = new File(path + pages.get(stIdx));
			f.delete();
			maxKey.remove(stIdx);
			pages.remove(stIdx);
		}
		save();
	}

	private void save() throws IOException {
		File f = new File(path + tableName + ".class");
		if(f.exists()) {
			f.delete();
		}
		f.createNewFile();
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path + tableName + ".class"));
		oos.writeObject(this);
		oos.close();
	}

	private void createDirectories() {
		File file = new File(this.path);
		file.mkdirs();
	}

	private Comparable getComparable(Object o, String type) {
		Comparable res = null;
		switch (type) {
		case "java.lang.Integer":
			res = (Integer) o;
			break;
		case "java.lang.String":
			res = (String) o;
			break;
		case "java.lang.Double":
			res = (Double) o;
			break;
		case "java.util.Date":
			res = (Date) o;
			break;
		default:
			break;
		}
		return res;
	}

	private Page createPage() {
		return new Page(maxPageSize, path + tableName + "_" + nextPageIdx + ".class", clusteringKeyColumn,
				clusteringKeyType);
	}

	private Page getPage(int idx) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path + pages.get(idx)));
		Page page = (Page) ois.readObject();
		ois.close();
		return page;
	}

	private boolean tupleExists(Object clusteringKeyValue) throws ClassNotFoundException, IOException {
		// binary search using max key in each page
		int lo = 0, hi = pages.size() - 1, res = -1;
		Comparable value = getComparable(clusteringKeyValue, clusteringKeyType);
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			// gets max key in page as comparable
//			Comparable max = getComparable(maxKey.get(pages.get(mid)), clusteringKeyType);
			Comparable max = getComparable(maxKey.get(mid), clusteringKeyType);
			if (value.compareTo(max) <= 0) {
				res = mid;
				hi = mid - 1;
			} else {
				lo = mid + 1;
			}
		}

		if (res != -1) { // if page exists then check page for tuple
			Page page = getPage(res);
			return page.tupleExists(value);
		} else {
			return false;
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(
				"########################### TABLE " + tableName + " ###########################\n");
		for (String pageName : pages) {
			try {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path + pageName));
				Page page = (Page) ois.readObject();
				sb.append(page.toString());
				ois.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}
	/*
	 * MAIN METHOD FOR TESTING
	 */

	public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException {
//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		htblColNameType.put("DOB", "java.util.Date");
//
//		Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
//		htblColNameMin.put("id", "0");
//		htblColNameMin.put("name", "A");
//		htblColNameMin.put("gpa", "0");
//		htblColNameMin.put("DOB", "1935-01-01");
//
//		Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
//		htblColNameMax.put("id", "10000");
//		htblColNameMax.put("name", "ZZZZZZZZZZ");
//		htblColNameMax.put("gpa", "4.0");
//		htblColNameMax.put("DOB", "2035-12-31");
//
//		Table table = new Table("test_table", "id", htblColNameType, "src/main/resources/", 50);

//		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("id", 6);
//		htblColNameValue.put("name", "TestName1");
//		htblColNameValue.put("gpa", 1.3);
//		htblColNameValue.put("DOB", new Date());
//		table.insert(htblColNameValue);
//
//		htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("id", 4);
//		htblColNameValue.put("name", "TestName2");
//		htblColNameValue.put("gpa", 1.3);
//		htblColNameValue.put("DOB", new Date());
//		table.insert(htblColNameValue);
//
//		htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("id", 1);
//		htblColNameValue.put("name", "TestName3");
//		htblColNameValue.put("gpa", 1.3);
//		htblColNameValue.put("DOB", new Date());
//		table.insert(htblColNameValue);
//
//		htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("id", 2);
//		htblColNameValue.put("name", "TestName6");
//		htblColNameValue.put("gpa", 1.3);
//		htblColNameValue.put("DOB", new Date());
//		table.insert(htblColNameValue);
//
//		htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("id", 3);
//		htblColNameValue.put("name", "TestName4");
//		htblColNameValue.put("gpa", 1.3);
//		htblColNameValue.put("DOB", new Date());
//		table.insert(htblColNameValue);
//
//		htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("id", 5);
//		htblColNameValue.put("name", "TestName5");
//		htblColNameValue.put("gpa", 1.3);
//		htblColNameValue.put("DOB", new Date());
//		table.insert(htblColNameValue);

//		for (int i = 0; i < 150; ++i) {
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//			int rand = (int) (Math.random() * 250);
//			int rand = i;
//			htblColNameValue.put("id", rand);
//			htblColNameValue.put("name", "TestName" + rand);
//			htblColNameValue.put("gpa", 1.3);
//			htblColNameValue.put("DOB", new Date());
//			System.out.println(rand);
//			table.insertWithOF(htblColNameValue);
//		}

//		for (int i = 0; i < 50; ++i) {
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//			int rand = 50;
//			htblColNameValue.put("id", rand);
//			htblColNameValue.put("name", "TestName" + rand);
//			htblColNameValue.put("gpa", 1.4);
//			htblColNameValue.put("DOB", new Date());
//			table.insertWithOF(htblColNameValue);
//		}
//
//		for (int i = 51; i <= 100; ++i) {
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//			int rand = i;
//			htblColNameValue.put("id", rand);
//			htblColNameValue.put("name", "TestName" + rand);
//			htblColNameValue.put("gpa", 1.3);
//			htblColNameValue.put("DOB", new Date());
//			table.insertWithOF(htblColNameValue);
//		}
//
//		System.out.println(table);
//		
//		Hashtable<String, Object> htblUpdate = new Hashtable<String, Object>();
//		htblUpdate.put("name", "updatedValue2");
//		htblUpdate.put("gpa", 0);
//
//		table.updateBS(new Integer(51), htblUpdate);
//
//		System.out.println(table);
//		Hashtable<String, Object> delete = new Hashtable<String, Object>();
//		delete.put("gpa", 1.4);

//		table.deleteBS(delete);

//		System.out.println(table);
	}

}
