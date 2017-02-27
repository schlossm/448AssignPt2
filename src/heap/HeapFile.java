package heap;

import bufmgr.BufMgr;
import diskmgr.DiskMgr;
import global.Minibase;
import global.PageId;
import global.RID;

import java.util.ArrayList;
import java.util.HashMap;

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
			heapFileMap = new HeapFileMap(first);
		}

		hashMap = new HashMap<>();
	}

	public RID insertRecord(byte[] record) throws InvalidUpdateException
	{
		if (record.length > 1024) throw new InvalidUpdateException();

		HFPage freePage = heapFileMap.freePageThatFitsSize(record.length);
		RID rid = freePage.insertRecord(record);
		heapFileMap.flushPage(rid.pageno);
		return rid;
	}

	public Tuple getRecord(RID rid) throws InvalidUpdateException
	{
		HFPage page = heapFileMap.getPage(rid.pageno);
		byte[] data = page.selectRecord(rid);
		return new Tuple(data, 0, data.length);
	}

	public boolean updateRecord(RID rid, Tuple newRecord) throws InvalidUpdateException
	{
		HFPage page = heapFileMap.getPage(rid.pageno);
		try
		{
			page.selectRecord(rid);
		}
		catch (Exception ignored)
		{
			return false;
		}

		page.updateRecord(rid, newRecord);
		heapFileMap.flushPage(rid.pageno);
		return true;
	}

	public boolean deleteRecord(RID rid) throws InvalidUpdateException
	{
		HFPage page = heapFileMap.getPage(rid.pageno);
		try
		{
			page.selectRecord(rid);
		}
		catch (Exception ignored)
		{
			return false;
		}
		page.deleteRecord(rid);
		heapFileMap.flushPage(rid.pageno);
		return true;
	}

	public int getRecCnt() //get number of records in the file public HeapScan openScan()
	{
		return heapFileMap.getNumRecords();
	}
}

class HeapFileMap
{
	private ArrayList<PageId> freePages = new ArrayList<>();
	private ArrayList<PageId> allPages = new ArrayList<>();

	HeapFileMap(PageId header)
	{
		HFPage first = new HFPage();
		Minibase.BufferManager.pinPage(header, first, false);
		if (first.firstRecord() == null)
		{
			//Hard code these values since everything defaults to 0.
			first.setPrevPage(new PageId());
			first.setShortValue((short) 1024, 2);
			first.setShortValue((short) 1004, 4);
			first.setCurPage(header);
			freePages.add(header);
			Minibase.BufferManager.flushPage(header);
		}
		allPages.add(header);

		Minibase.BufferManager.unpinPage(header, true);
	}

	int getNumRecords()
	{
		int count = 0;
		for (PageId pageId : allPages)
		{
			HFPage newPage = new HFPage();
			Minibase.BufferManager.pinPage(pageId, newPage, false);
			count += newPage.getSlotCount();
		}
		return count;
	}

	HFPage freePageThatFitsSize(int length) throws InvalidUpdateException
	{
		for (PageId pageId : freePages)
		{
			HFPage page = new HFPage();
			Minibase.BufferManager.pinPage(pageId, page, false);
			if (page.getFreeSpace() >= length)
			{
				return page;
			}
			Minibase.BufferManager.unpinPage(pageId, false);
		}

		PageId newPageId = Minibase.DiskManager.allocate_page();
		HFPage newPage = new HFPage();
		Minibase.BufferManager.pinPage(newPageId, newPage, false);
		newPage.setPrevPage(allPages.get(allPages.size() - 1));
		newPage.setCurPage(newPageId);
		newPage.setShortValue((short) 1024, 2);
		newPage.setShortValue((short) 1004, 4);

		freePages.add(newPage.getCurPage());
		allPages.add(newPage.getCurPage());

		return newPage;
	}

	void flushPage(PageId pageNum) throws InvalidUpdateException
	{
		if (!allPages.contains(pageNum))
		{
			throw new InvalidUpdateException();
		}
		HFPage pageToReturn = new HFPage();
		Minibase.BufferManager.pinPage(pageNum, pageToReturn, false);
		if (pageToReturn.getFreeSpace() == 0)
		{
			freePages.remove(pageNum);
		}
		else if (!freePages.contains(pageNum))
		{
			freePages.add(pageNum);
		}
		Minibase.BufferManager.flushPage(pageNum);
	}

	HFPage getPage(PageId pageNum) throws InvalidUpdateException
	{
		if (!allPages.contains(pageNum))
		{
			throw new InvalidUpdateException();
		}

		HFPage pageToReturn = new HFPage();
		Minibase.BufferManager.pinPage(pageNum, pageToReturn, false);
		return pageToReturn;
	}
}