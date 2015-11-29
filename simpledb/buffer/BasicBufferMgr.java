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
   //private Buffer[] bufferpool;
   private int numAvailable;
   private Map<Block,Buffer> bufferPoolMap; // Custom
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
	   bufferPoolMap = new HashMap<Block, Buffer>(); // Custom
	   System.out.println("Called BasicBufferMgr constructor");
	   numAvailable = numbuffs; // Custom 
	   numAllocated = numbuffs;
      /*bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();*/
   }
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      //for (Buffer buff : bufferpool)
	   for(Buffer buff : bufferPoolMap.values()) // Custom
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
        	 System.out.println("removed: " + buff.block().toString());
         }
         
         buff.assignToBlock(blk);
         bufferPoolMap.put(buff.block(), buff);
         System.out.println("added: " + buff.block().toString());
      }
      if (!buff.isPinned())
      {
         numAvailable--;
      }
      buff.pin();
      System.out.println("pinned: " + buff.block().toString());
      //System.out.println("numavailable: " + numAvailable);
      /*for(Buffer buffer : bufferPoolMap.values())
      {
    	  System.out.println(buffer.block().toString());
      }
      System.out.println("------------");*/
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
      System.out.println("pinNew");
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
      System.out.println("Unpinned buffer:" + buff.block().toString());
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
      //for (Buffer buff : bufferpool)
      try{
    	 //System.out.println("In findExistingBuffer ");
         Buffer buff = bufferPoolMap.get(blk);  // Custom
         
            return buff;
      }
      catch(Exception e){
    	  
    	  return null;
    	  
      }
      
   }
   
   private Buffer chooseUnpinnedBuffer() {
	      //for (Buffer buff : bufferpool)
		  if(numAllocated > 0)
		  {
			  Buffer buff = new Buffer();
			  numAllocated--;
			  System.out.println("numallocated is greater than 0");
			  System.out.println("returning unoccupied buffer");
			  System.out.println("lsn of that buffer is: " + buff.getLogSequenceNumber());
			  return buff;
		  }
		  else
		  {
			  int min = -1;
			  Buffer lsn_buff = null;
			  for(Buffer buff : bufferPoolMap.values())  // Custom
			  {
				  if(!buff.isPinned())
				  {
					  int lsn = buff.getLogSequenceNumber();
					  System.out.println("lsn : " + lsn+" min "+ min);
					  //if(min == -1 || (lsn < min && lsn != -1))
					  if((lsn != -1 && min == -1) || (lsn != -1 && min != -1 && lsn < min))
					  {
						  min = lsn;
						  lsn_buff = buff;
					  }
				  }
			  }
			  if (min == -1){
				  System.out.println("random block entered ");
				  for(Buffer buff : bufferPoolMap.values())  // Custom
				  {
					  if(!buff.isPinned())
					  {
						  int lsn = buff.getLogSequenceNumber();
						  System.out.println("lsn : " + lsn+" min "+ min);
						  lsn_buff = buff;
					  }
				  	}
			  }
			  System.out.println("returning buffer occupied by: " + lsn_buff.block().toString());
			  System.out.println("lsn of that buffer is: " + lsn_buff.getLogSequenceNumber());
			  return lsn_buff;
	         //if (!buff.isPinned())
	         //return buff;
		  }
	      //return null;
   }
   
   public boolean containsMapping(Block blk){
	   return bufferPoolMap.containsKey(blk);
   }
   
   public Buffer getMapping(Block blk){
	   return bufferPoolMap.get(blk);
   }
   
   public void getStatistics(){
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
