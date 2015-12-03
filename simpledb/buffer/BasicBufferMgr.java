package simpledb.buffer;

import simpledb.file.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private int numAvailable;
   private Map<Block,Buffer> bufferPoolMap; // Modified BufferPool
   private int numAllocated;
   
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
	   bufferPoolMap = new HashMap<Block, Buffer>();
	   numAvailable = numbuffs; 
	   numAllocated = numbuffs;
    
   }
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
	   
	   for(Buffer buff : bufferPoolMap.values()) 
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         if(buff.block()!= null){
        	 
        	 bufferPoolMap.remove(buff.block());
        	 
         }
         
         buff.assignToBlock(blk);
         bufferPoolMap.put(buff.block(), buff);
         
      }
      if (!buff.isPinned())
      {
         numAvailable--;
      }
      buff.pin();
      return buff;
   }
   
   
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      
      if (buff == null)
         return null;
      if(buff.block()!= null){
     	 
     	 bufferPoolMap.remove(buff.block());
      }
      
      buff.assignToNew(filename, fmtr);
      bufferPoolMap.put(buff.block(), buff);
      
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk){
      try{
    	 
         Buffer buff = bufferPoolMap.get(blk);
            return buff;
      }
      catch(Exception e){
    	  return null;
    	  
      }
      
   }
   
   private Buffer chooseUnpinnedBuffer() {
	   
		  if(numAllocated > 0) // if buffer is not full 
		  {
			  Buffer buff = new Buffer();
			  numAllocated--;
			  return buff;
		  }
		  else
		  {
			  int min = -1;
			  Buffer lsn_buff = null;
			  for(Buffer buff : bufferPoolMap.values())  // LRM replacement strategy
			  {
				  if(!buff.isPinned())
				  {
					  int lsn = buff.getLogSequenceNumber();
					  if((lsn != -1 && min == -1) || (lsn != -1 && min != -1 && lsn < min))
					  {
						  min = lsn;
						  lsn_buff = buff;
					  }
				  }
			  }
			  if (min == -1){
				  
				  for(Buffer buff : bufferPoolMap.values())
				  {
					  if(!buff.isPinned())
					  {
						  int lsn = buff.getLogSequenceNumber();
						  lsn_buff = buff;
					  }
				  	}
			  }
			  return lsn_buff; // buffer to replace
		  }
   }
   
   
   public boolean containsMapping(Block blk){
	   return bufferPoolMap.containsKey(blk);
   }
   
   public Buffer getMapping(Block blk){
	   return bufferPoolMap.get(blk);
   }
   
   public void getStatistics(){ // get useful statistics about the buffers
	   int i = 0;
	   for(Block b:bufferPoolMap.keySet()){
		   System.out.println("Buffer number "+ i);
		   System.out.println("Pin Count: "+bufferPoolMap.get(b).getPinCount());
		   System.out.println("Unpin Count: "+bufferPoolMap.get(b).getUnpinCount());
		   System.out.println("Read Count: "+bufferPoolMap.get(b).getReadCount());
		   System.out.println("Write Count: "+bufferPoolMap.get(b).getWriteCount());
		   i++;
	   }
   }
   
}
