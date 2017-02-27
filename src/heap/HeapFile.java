package heap;

import bufmgr.BufMgr;
import diskmgr.DiskMgr;
import global.Minibase;
import global.PageId;
import global.RID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by michaelschloss on 2/21/17.
 */
public class HeapFile
{
	//TODO: Make custom data structure
	private HashMap<RID, HFPage> hashMap;
	private HeapFileMap heapFileMap;

	private DiskMgr diskMgr;
	private BufMgr bufMgr;

	public HeapFile(String name) throws InvalidUpdateException
	{
		diskMgr = Minibase.DiskManager;
		bufMgr = Minibase.BufferManager;
		if (diskMgr == null || bufMgr == null) throw new InvalidUpdateException();
		PageId first = diskMgr.get_file_entry(name);

		if (first == null)
		{
			first = diskMgr.allocate_page();
			diskMgr.add_file_entry(name, first);
			heapFileMap = new HeapFileMap(first);
		}
		else
		{
			System.out.println("First not Null: " + first);
			heapFileMap = new HeapFileMap(first);
		}


		hashMap = new HashMap<>();
	}

	public RID insertRecord(byte[] record) throws InvalidUpdateException
	{

		if (record.length > 1024) throw new InvalidUpdateException();

		//First, do a check to see if any already made HFPages have free space for it
		if (hashMap.size() != 0)
		{
			Set<Map.Entry<RID, HFPage>> entrySet = hashMap.entrySet();
			for (Map.Entry<RID, HFPage> entry : entrySet)
			{
				if (entry.getValue().getFreeSpace() >= record.length)
				{
					RID rid = entry.getValue().insertRecord(record);
					hashMap.put(rid, entry.getValue());
					return rid;
				}
			}
		}

		HFPage hfPage = new HFPage();
		RID rid = hfPage.insertRecord(record);

		hashMap.put(rid, hfPage);

		return rid;
	}

	public Tuple getRecord(RID rid)
	{
		HFPage page = hashMap.get(rid);
		byte[] data = page.selectRecord(rid);
		return new Tuple(data, 0, data.length);
	}

	public boolean updateRecord(RID rid, Tuple newRecord)
	{
		if (hashMap.get(rid) == null) return false;
		HFPage page = hashMap.get(rid);
		page.updateRecord(rid, newRecord);
		return true;
	}

	public boolean deleteRecord(RID rid)
	{
		if (hashMap.get(rid) == null) return false;
		HFPage page = hashMap.get(rid);
		page.deleteRecord(rid);
		return true;
	}

	public int getRecCnt() //get number of records in the file public HeapScan openScan()
	{
		return 0;
	}

	public static void main(String argv[])
	{
		System.out.println("Creating database...\nReplacer: " + "CLOCK"); //CLOCK replacement policy

		String dbpath = "/tmp/"+"hptest"+System.getProperty("user.name")+".minibase-db"; ;

		/** Default database size (in pages). */
		int DB_SIZE = 10000;

		/** Default buffer pool size (in pages) */
		int BUF_SIZE = 100;

		/** Default look ahead size */
		int LAH_SIZE = 10;

		new Minibase(dbpath, DB_SIZE, BUF_SIZE, LAH_SIZE, "CLOCK", false);

		try
		{
			HeapFile heapFile = new HeapFile("Blah");
		}
		catch (InvalidUpdateException e)
		{
			e.printStackTrace();
		}
	}
}

class HeapFileMap
{
	private final PageId header;
	private ArrayList<HFPage> freePages = new ArrayList<>();
	private ArrayList<HFPage> allPages = new ArrayList<>();

	HeapFileMap(PageId header)
	{
		this.header = header;
		HFPage first = new HFPage();
		Minibase.BufferManager.pinPage(header, first, false);
		System.out.print("first HFPage.  Maybe Something.  " );
		first.print();
		allPages.add(first);

	}

	PageId freePageThatFitsSize(int length)
	{
		return new PageId();
	}
}