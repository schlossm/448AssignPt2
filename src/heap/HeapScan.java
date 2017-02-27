package heap;

import global.Minibase;
import global.PageId;
import global.RID;

/**
 * Created by michaelschloss on 2/26/17.
 */
public class HeapScan
{
	PageId directoryPosition;
	RID recordPosition;
	HeapFile heapFile;
	HFPage firstPage;

	protected HeapScan(HeapFile hf)
	{
		hf.reset();
		directoryPosition = hf.first();
		recordPosition = hf.firstRID();
		firstPage = new HFPage();
		Minibase.BufferManager.pinPage(directoryPosition, firstPage, false);
		heapFile = hf;
	}

	protected void finalize()
	{
		try
		{
			close();
		}
		catch (InvalidUpdateException ignored) { }
	}

	public void close() throws InvalidUpdateException
	{
		for (PageId pageId : heapFile.heapFileMap.allPages)
		{
			boolean error = false;
			while (!error)
			{
				try
				{
					Minibase.BufferManager.unpinPage(pageId, false);
				}
				catch (Exception ignored)
				{
					error = true;
				}
			}
		}

		heapFile.reset();
	}

	public boolean hasNext() throws InvalidUpdateException
	{
		boolean bool = heapFile.getTempNextRID() != null;
		if (!bool) close();
		return bool;
	}

	public Tuple getNext(RID rid) throws InvalidUpdateException
	{
		RID rid1 = heapFile.getNextRID(rid);
		try
		{
			rid.slotno = rid1.slotno;
			rid.pageno = rid1.pageno;
			Tuple record = heapFile.getRecord(rid1);
			Minibase.BufferManager.unpinPage(rid.pageno, false);
			return record;
		}
		catch (Exception e)
		{
			return null;
		}
	}
}
