package heap;

import bufmgr.BufMgr;
import chainexception.ChainException;
import diskmgr.DiskMgr;
import global.Minibase;
import global.PageId;
import global.RID;

import java.util.ArrayList;

/**
 * Created by michaelschloss on 2/21/17.
 */
public class HeapFile
{
	HeapFileMap heapFileMap;

	public HeapFile(String name) throws InvalidUpdateException
	{
		DiskMgr diskMgr = Minibase.DiskManager;
		BufMgr bufMgr = Minibase.BufferManager;
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
	}

	PageId first()
	{
		return heapFileMap.header;
	}

	RID firstRID()
	{
		HFPage page = heapFileMap.first();
		if (page.getSlotCount() == 0)
		{
			return null;
		}

		return new RID(page.getCurPage(), 0);
	}

	private int pageNum = 0;
	private int rNum = 0;

	private int tempPageNum = -1;
	private int tempRNum = -1;

	RID getTempNextRID()
	{
		if (tempPageNum == -1 && tempRNum == -1)
		{
			tempPageNum = pageNum;
			tempRNum = rNum;
		}
		tempRNum++;
		HFPage page = null;
		try
		{
			page = heapFileMap.getPage(new PageId(tempPageNum));
		}
		catch (InvalidUpdateException e)
		{
			e.printStackTrace();
			tempPageNum = -1;
			tempRNum = -1;
			return null;
		}
		if (page.getSlotCount() > tempRNum)
		{
			final int pageNo = tempPageNum;
			final int rNo = tempRNum;

			Minibase.BufferManager.unpinPage(new PageId(tempPageNum), false);

			tempPageNum = -1;
			tempRNum = -1;

			return new RID(new PageId(pageNo), rNo);
		}
		else {
			Minibase.BufferManager.unpinPage(new PageId(tempPageNum), false);
			tempPageNum++;
			tempRNum = -1;
			return getTempNextRID();
		}
	}

	RID getNextRID(RID rid)
	{
		pageNum = rid.pageno.pid;
		rNum = rid.slotno;
		rNum++;
		HFPage page;
		try
		{
			page = heapFileMap.getPage(new PageId(pageNum));

		}
		catch (InvalidUpdateException e)
		{
			return null;
		}
		if (page.getSlotCount() > rNum)
		{
			return new RID(new PageId(pageNum), rNum);
		}
		else
		{
			Minibase.BufferManager.unpinPage(new PageId(pageNum), false);
			pageNum++;
			rNum = -1;
			return getNextRIDRecursion(rid);
		}
	}

	private RID getNextRIDRecursion(RID rid)
	{
		pageNum = rid.pageno.pid;
		rNum = rid.slotno;
		rNum++;
		HFPage page = null;
		try
		{
			page = heapFileMap.getPage(new PageId(pageNum));
		}
		catch (Exception e)
		{
			return null;
		}
		if (page.getSlotCount() > rNum)
		{
			return new RID(new PageId(pageNum), rNum);
		}
		else {
			Minibase.BufferManager.unpinPage(new PageId(pageNum), false);
			pageNum++;
			rNum = -1;
			return getNextRIDRecursion(new RID(new PageId(pageNum), rNum));
		}
	}

	void reset()
	{
		pageNum = 0;
		rNum = 0;
		tempPageNum = -1;
		tempRNum = -1;
	}

	public RID insertRecord(byte[] record) throws ChainException
	{
		if (record.length > 1004) throw new SpaceNotAvailableException("No space available");

		HFPage freePage = heapFileMap.freePageThatFitsSize(record.length);
		RID rid = freePage.insertRecord(record);
		if (rid == null)
		{
			throw new InvalidUpdateException();
		}
		heapFileMap.flushPage(rid.pageno);
		return rid;
	}

	public Tuple getRecord(RID rid) throws InvalidUpdateException
	{
		HFPage page = heapFileMap.getPage(rid.pageno);
		byte[] data = page.selectRecord(rid);
		Minibase.BufferManager.unpinPage(page.getCurPage(), false);
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

		if (page.selectRecord(rid).length != newRecord.getTupleByteArray().length)
		{
			throw new InvalidUpdateException();
		}

		try
		{
			page.updateRecord(rid, newRecord);
		}
		catch (Exception ignored)
		{
			throw new InvalidUpdateException();
		}
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

	public HeapScan openScan()
	{
		return new HeapScan(this);
	}
}

class HeapFileMap
{
	PageId header;
	private ArrayList<PageId> freePages = new ArrayList<>();
	ArrayList<PageId> allPages = new ArrayList<>();

	HeapFileMap(PageId header)
	{
		this.header = header;
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

	HFPage first()
	{
		HFPage newPage = new HFPage();
		Minibase.BufferManager.pinPage(allPages.get(0), newPage, false);
		Minibase.BufferManager.unpinPage(allPages.get(0), false);
		return newPage;
	}

	int getNumRecords()
	{
		int count = 0;
		for (PageId pageId : allPages)
		{
			HFPage newPage = new HFPage();
			Minibase.BufferManager.pinPage(pageId, newPage, false);
			count += newPage.getSlotCount();
			Minibase.BufferManager.unpinPage(pageId, false);
		}
		return count;
	}

	HFPage freePageThatFitsSize(int length) throws InvalidUpdateException
	{
		for (PageId pageId : freePages)
		{
			HFPage page = new HFPage();
			Minibase.BufferManager.pinPage(pageId, page, false);
			if (page.getFreeSpace() > length)
			{
				Minibase.BufferManager.unpinPage(pageId, false);
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
		Minibase.BufferManager.flushPage(newPageId);

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
		if (pageToReturn.getFreeSpace() == 0)
		{
			freePages.remove(pageNum);
		}
		else if (!freePages.contains(pageNum))
		{
			freePages.add(pageNum);
		}
		try
		{
			Minibase.BufferManager.unpinPage(pageNum, true);
		}
		catch (Exception ignored) { }
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