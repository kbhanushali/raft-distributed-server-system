package poke.server.managers;

import io.netty.channel.Channel;

import javax.imageio.ImageIO;  

import com.google.protobuf.ByteString;

import poke.comm.Image.Header;
import poke.comm.Image.PayLoad;
import poke.comm.Image.Ping;
import poke.comm.Image.Request;
import poke.comm.Image.Tile;
import poke.server.managers.src.Grayscale;
import poke.server.resources.database_connectivity;

import java.awt.image.BufferedImage;  
import java.io.*;  
//import java.nio.file.Files;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.awt.*;  
  
public class ImageManager { 
	
	static int tileWidth,tileHeight, parentImageWidth, parentImageHeight;
	private BufferedImage image;
	static String[] tileName;
	static int tileCounter = 0;
	database_connectivity db;
	private static HashMap<Integer, Channel> connections;
	private static HashMap<String, BufferedImage> imageMap = new HashMap<String, BufferedImage>();
	private static HashMap<Integer, Integer> imageCountMap = new HashMap<Integer, Integer>();
	private static HashMap<Integer, Integer> processedImageCountMap = new HashMap<Integer, Integer>();
	static String target_dir="/Users/Krishna/git/raft-distributed-server-system/images/";
	int imageId;
	Request req;
	BufferedImage imgs[];
	
	public ImageManager()
	{
		db = new database_connectivity();
		imageId = 0;

	}
	
    public void processImage(BufferedImage img, Request req) throws IOException{  
  
    	//if tile are processed. Dump them into imgId/processed/ folder and increment the count.
    	this.req = req;
    	this.image = img;
    	this.imageId = req.getPayload().getImgId();
    	
    	if(req.getHeader().getPhase() == 2)
		{
			writeFile();
			SplitImage();
			sendToAllNodes();
		}
		else if(req.getHeader().getPhase() == 3)
		{
			//
			if(WriteFileforProcessing())
			{
				String	tileId = req.getTile().getTileSuffix();
				sendProcessedToLeader(Grayscale.applyMask(target_dir+imageId+"/toprocess/"+tileId+".png"));
			}
			else 
			if(req.getHeader().getPhase() == 4)
			{
//				int imageId = req.getPayload().getImgId();
				if(writeProcessedFile())
				{
					processedImageCountMap.put(imageId, processedImageCountMap.get(imageId)+1);
					if(processedImageCountMap.get(imageId)==imageCountMap.get(imageId))
					{
						imageMerge();
						broadcastToAllNodes();
					}
				}
			}   
		}
    }
    
    	
    
    
    
    

	
	/* Splits the image into 4x4 matrix
	 * writes image in target_dir+imageId folder
	 */
	void SplitImage() throws IOException{
		
		//int imageId = req.getPayload().getImgId();
		File file = new File(target_dir+imageId+".png"); 
        FileInputStream fis = new FileInputStream(file);  
        //tileName=file.getName().split("\\.");
        
        image = ImageIO.read(fis); //reading the image file  
  
        int rows = 4; //You should decide the values for rows and cols variables  
        int cols = 4;  
        tileCounter = rows * cols;
        parentImageWidth =image.getWidth();
        parentImageHeight =image.getHeight();
        tileWidth = image.getWidth() / cols; // determines the chunk width and height  
        tileHeight = image.getHeight() / rows;  
        int count = 0;  
        imgs = new BufferedImage[tileCounter]; //Image array to hold image tiles  
        for (int x = 0; x < rows; x++) {  
            for (int y = 0; y < cols; y++) {  
                //Initialize the image array with image chunks  
                imgs[count] = new BufferedImage(tileWidth, tileHeight, image.getType());  
  
                // draws the image chunk  
                Graphics2D gr = imgs[count].createGraphics();  
                gr.drawImage(image, 0, 0, tileWidth, tileHeight, tileWidth * y, tileHeight * x, tileWidth * y + tileWidth, tileHeight * x + tileHeight, null);  
                ImageIO.write(imgs[count++], "png", new File(target_dir+imageId+"/"+y+x+".png"));
                gr.dispose();  
            }  
        }  
        imageCountMap.put(imageId, tileCounter);
        
	}

	/*It has 2 stages. Creating an Image HashMap and
	 *  then Storing image.
	 */
	public void imageMerge() {
		int localCounter =this.CreateImageHashmap();
		if((localCounter) == tileCounter)
			finalMerge();	  
	}


	/* Stage 1 of Merge image
	 * creates a hashmap
	 * might not need this at all. OR CHANGE IS REQUIRED.
	 */
	private  int CreateImageHashmap() {
		//Creating Local hashmap
			int localCounter =0;
			File dir = new File(target_dir+this.imageId+"/processed/");
		    File[] files = dir.listFiles();
			int xindex=0;
			int yindex=0;
			
			for(File f: files)
			{
				System.out.println(f.getName());
				String temp = xindex+""+yindex+".png";
				if(f.exists() && (f.getName().equals(temp)))
				{
					try {
						FileInputStream fis = new FileInputStream(f);
						image = ImageIO.read(fis);
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					String key =xindex+""+yindex;
					imageMap.put(key,image);
					if(yindex==3)
						xindex =(xindex+1)%4;
					yindex=(yindex+1)%4;
					System.out.println(xindex+" "+yindex);			
				}
			}
			System.out.println("localCounter"+localCounter+"chunkCounter"+tileCounter);
			return localCounter;
	}
	
	
	
	/*The final image merge method is used to draw the 
	 * image chunks into the bigger image.
	 */
	private  void finalMerge() {
		BufferedImage result = new BufferedImage(parentImageWidth,parentImageHeight,BufferedImage.TYPE_INT_RGB);
			Graphics g = result.getGraphics();
		Set<String> keys = imageMap.keySet();
		
		int x=0, orignalX=0;
		int y=0, orignalY=0;
		
		for(String e : keys){
			System.out.println(e);
			BufferedImage bi = imageMap.get(e);
			int originalIndex[]=extractPosition(e);
			orignalX=originalIndex[0];
			orignalY=originalIndex[1];
			x= orignalX*tileWidth;
			y= orignalY*tileHeight;
			System.out.println(x+" "+y+ " " +orignalX+" " +orignalY);
			g.drawImage(bi, x, y, null);
		}
		try {
			ImageIO.write(result, "png", new File(target_dir+imageId+"manipulated.png"));
		} catch (IOException e1) {
			e1.printStackTrace();
		}  
		
	}

	/*This method extracts position of image and return 
	 * the x and y index in an Array.
	 */
	private static int[] extractPosition(String e) {
		
		int index[]= new int[2];
		for (int i = 0;i < e.length(); i++){
			index[i] = Character.getNumericValue(e.charAt(i));
		}
		return index;
	}
	
	
	
/****************************************************
	 * File writing to system logic
*****************************************************/	
	
	/*Incoming message of Phase 3 is persisted to file system.
	 * the input folder is imageId+"/toprocess/"
	 */
		private boolean WriteFileforProcessing() {
			boolean rtn = false;
			//int imageId = req.getPayload().getImgId();
			String path = target_dir+imageId+"/toprocess/";
			try {
				 String	tileId = req.getTile().getTileSuffix();
				//Writing image to file system and img path to the database.
					File f = new File(path);
					if(f.mkdir())
					{
						ImageIO.write(image, "png", new File(path+tileId+".png"));
					}	
					String query = "insert into CMPE_275.Data values ("
						+ ElectionManager.getInstance().getNodeId() + ","
						+ ElectionManager.getInstance().getTermId() + ","
						+ imageId + ", '"+path+tileId
						+ ".png','" + req.getHeader().getCaption() + "')";
					db.execute_query(query);
					rtn = true;
				} catch (SQLException e) {
					e.printStackTrace();
					rtn = false;
				} catch (IOException e) {
					e.printStackTrace();
					rtn = false;
				}
			return rtn;		
		}


	/* writes processed image to file system
	 * Used in phase 4.
	 * File is stored in the imageId+"/processed/ folder
	 */
	public boolean writeProcessedFile()
	{
		boolean rtn = false;
		//int imageId = req.getPayload().getImgId();
		String path = target_dir+imageId+"/processed/";
		try {
			 String	tileId = req.getTile().getTileSuffix();
			//Writing image to file system and img path to the database.
				File f = new File(path);
				if(f.mkdir())
				{
					ImageIO.write(this.image, "png", new File(path+tileId+".png"));
				}	
				String query = "insert into CMPE_275.Data values ("
					+ ElectionManager.getInstance().getNodeId() + ","
					+ ElectionManager.getInstance().getTermId() + ","
					+ imageId + ", '"+path+tileId
					+ ".png','" + req.getHeader().getCaption() + "')";
				db.execute_query(query);
				rtn = true;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				rtn = false;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				rtn = false;
			}
		return rtn;
	} 
	
	
	/* writes original image to file system.
	 * used in phase 2
	 */
		public boolean writeFile()
		{
			boolean rtn = false;
			int imageId = req.getPayload().getImgId();
			String path = target_dir;
			try {
				File f = new File(path);
				if(f.mkdir()){
				  ImageIO.write(this.image, "png", new File(path+imageId+".png"));
				}	
				String query = "insert into CMPE_275.Data values ("
					+ ElectionManager.getInstance().getNodeId() + ","
					+ ElectionManager.getInstance().getTermId() + ","
					+ imageId + ", '"+path+imageId
					+ ".png','" + req.getHeader().getCaption() + "')";
				db.execute_query(query);
				rtn = true;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					rtn = false;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					rtn = false;
				}
			return rtn;
		}
		
		
		
		
/****************************************************
		 * File sending logic
*****************************************************/
		
	/*phase 4 message going to leader
     * it is an image tile
     * it has been converted to grayscale.
     */
	    private void sendProcessedToLeader(String path)
	    {
			
			File f= new File(path);
				tileName=f.getName().split("\\.");
				int x= Integer.parseInt(tileName[0].split("")[0]);
				int y= Integer.parseInt(tileName[0].split("")[1]);
			 try {
				BufferedImage processedImage = ImageIO.read(f);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ImageIO.write( processedImage, "jpg", baos );
				baos.flush();
				byte[] imageInByte = baos.toByteArray();
				
				Request.Builder newReq = Request.newBuilder();
				Header.Builder newHeader = Header.newBuilder();
				PayLoad.Builder newPayload = PayLoad.newBuilder();
				Ping.Builder newPing = Ping.newBuilder();
				Tile.Builder newTile = Tile.newBuilder();
				
				newHeader.setClientId(req.getHeader().getClientId());
				newHeader.setClusterId(req.getHeader().getClusterId());
				newHeader.setCaption(req.getHeader().getCaption());
				newHeader.setIsClient(false);
				newHeader.setPhase(4);
				
				newPing.setIsPing(req.getPing().getIsPing());
				newPayload.setData(ByteString.copyFrom(imageInByte));
				newPayload.setIsTile(true);
				newPayload.setImgId(req.getPayload().getImgId());
				
				newTile.setTileSuffix(tileName[0]);
				newTile.setTileX(x);
				newTile.setTileY(y);
				newTile.setIsProcessed(true);
				
				newReq.setHeader(newHeader);
				newReq.setPing(newPing);
				newReq.setPayload(newPayload);
				newReq.setTile(newTile);
				
				Request request = newReq.build();
				ConnectionManager.unicast(request);
			 	} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			  }

	    }

 /*Send image to every nodes with in cluster
  * there is no image here
  */
			private void broadcastToAllNodes() {
		    		File f = new File(target_dir+imageId+"manipulated.png");
					
//						
					 try {
						BufferedImage originalImage = ImageIO.read(f);
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						ImageIO.write( originalImage, "png", baos );
						baos.flush();
						byte[] imageInByte = baos.toByteArray();
						
						Request.Builder newReq = Request.newBuilder();
						Header.Builder newHeader = Header.newBuilder();
						PayLoad.Builder newPayload = PayLoad.newBuilder();
						Ping.Builder newPing = Ping.newBuilder();
						
						newHeader.setClientId(req.getHeader().getClientId());
						newHeader.setClusterId(req.getHeader().getClusterId());
						newHeader.setCaption(req.getHeader().getCaption());
						newHeader.setIsClient(false);
						newHeader.setPhase(5);
						
						newPing.setIsPing(req.getPing().getIsPing());
						newPayload.setData(ByteString.copyFrom(imageInByte));
						newPayload.setIsTile(false);
						newPayload.setImgId(imageId);
						
						
						newReq.setHeader(newHeader);
						newReq.setPing(newPing);
						newReq.setPayload(newPayload);
						
						Request request = newReq.build();
						
						ConnectionManager.broadcast(request);
						System.out.println("Sending to all clusters");
						
						//Send the messages to all the Nodes with in the cluster.
					  ConnectionManager.broadcastToClient(request);
					  } catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
					  }
		    	     
		    	
			}
		
	/*Sends tiles to all nodes with in cluster
     * in a round robin fashion
     * iterates over connections hashmap.
     */
		private void sendToAllNodes() {
	    		File dir = new File(target_dir+imageId+"/");
			    File[] files = dir.listFiles();
				connections =ConnectionManager.getAllConnections(false);
				Object[] a=connections.keySet().toArray();
				int i=0;
				for(File f: files)
				{
					tileName=f.getName().split("\\.");
					int x= Integer.parseInt(tileName[0].split("")[0]);
					int y= Integer.parseInt(tileName[0].split("")[1]);
				 try {
					BufferedImage originalImage = ImageIO.read(f);
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					ImageIO.write( originalImage, "jpg", baos );
					baos.flush();
					byte[] imageInByte = baos.toByteArray();
					
					Request.Builder newReq = Request.newBuilder();
					Header.Builder newHeader = Header.newBuilder();
					PayLoad.Builder newPayload = PayLoad.newBuilder();
					Ping.Builder newPing = Ping.newBuilder();
					Tile.Builder newTile = Tile.newBuilder();
					
					newHeader.setClientId(req.getHeader().getClientId());
					newHeader.setClusterId(req.getHeader().getClusterId());
					newHeader.setCaption(req.getHeader().getCaption());
					newHeader.setIsClient(false);
					newHeader.setPhase(3);
					
					newPing.setIsPing(req.getPing().getIsPing());
					newPayload.setData(ByteString.copyFrom(imageInByte));
					newPayload.setIsTile(true);
					newPayload.setImgId(req.getPayload().getImgId());
					
					newTile.setTileSuffix(tileName[0]);
					newTile.setTileX(x);
					newTile.setTileY(y);
					newTile.setIsProcessed(false);
					
					newReq.setHeader(newHeader);
					newReq.setPing(newPing);
					newReq.setPayload(newPayload);
					newReq.setTile(newTile);
					
					Request request = newReq.build();
					
					//ConnectionManager.broadcast(request);
					System.out.println("Sending to all nodes");
	    			ConnectionManager.unicast(request, (Integer) a[i]);
	    			i=(i+1)%connections.size();
				  } catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
				  }
	    	     }
	    	
		}

}  
