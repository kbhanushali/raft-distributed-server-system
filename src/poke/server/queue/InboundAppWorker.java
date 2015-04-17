/*
 * copyright 2015, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.queue;

import io.netty.channel.Channel;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.imageio.ImageIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.Image.Header;
import poke.comm.Image.PayLoad;
import poke.comm.Image.Ping;
import poke.comm.Image.Request;
import poke.server.managers.ConnectionManager;
import poke.server.managers.ElectionManager;
import poke.server.resources.database_connectivity;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

public class InboundAppWorker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("server");

	int workerId;
	PerChannelQueue sq;
	boolean forever = true;
	int imageId = 0;
	database_connectivity db;
	HashMap<Integer, SortedMap<Integer,ByteString>> imgMap; //It stores reference to entire image.
	SortedMap<Integer,ByteString> imgChunkMap; //for storing image chunks
	
	public InboundAppWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
		
		super(tgrp, "inbound-" + workerId);
		this.workerId = workerId;
		this.sq = sq;
		db = new database_connectivity();
		imgMap = new HashMap<Integer, SortedMap<Integer,ByteString>>();
		imgChunkMap = new TreeMap<Integer, ByteString>();
		
		if (sq.inbound == null)
			throw new RuntimeException("connection worker detected null inbound queue");
	}

	@Override
	public void run() {
		Channel conn = sq.getChannel();
		if (conn == null || !conn.isOpen()) {
			logger.error("connection missing, no inbound communication");
			return;
		}

		while (true) {
			if (!forever && sq.inbound.size() == 0)
				break;

			try {
				// block until a message is enqueued
				GeneratedMessage msg = sq.inbound.take();
				// process request and enqueue response
				if (msg instanceof Request) {
					Request req = ((Request) msg);
					
					//Message to be modified
					Integer leaderNode = ElectionManager.getInstance().whoIsTheLeader();
					Integer nodeId = ElectionManager.getInstance().getNodeId();

					if(req.getPayload().hasChunkId())
						System.out.println("Image Chunk Received "+req.getPayload().getChunkId()+" "+req.getPayload().getTotalChunks());
					
					if(req.getPing().getIsPing())
					{	
						return;
					}	

					//If I am the leader,broadcast images to all nodes in cluster
					if(leaderNode != null && leaderNode == nodeId){
						Request.Builder newReq = Request.newBuilder();
						Header.Builder newHeader = Header.newBuilder();
						PayLoad.Builder newPayload = PayLoad.newBuilder();
						Ping.Builder newPing = Ping.newBuilder();
						
						newHeader.setClientId(req.getHeader().getClientId());
						newHeader.setClusterId(req.getHeader().getClusterId());
						newHeader.setCaption(req.getHeader().getCaption());
						newHeader.setIsClient(false);
						
						newPing.setIsPing(req.getPing().getIsPing());
						
						newPayload.setData(req.getPayload().getData());
						
						newReq.setHeader(newHeader);
						newReq.setPing(newPing);
						newReq.setPayload(newPayload);
						
						Request request = newReq.build();
						
						ConnectionManager.broadcast(request);
						System.out.println("Sending to all clusters");
						
						//If the message has come from with in the cluster, only then broadcast image to other clusters.
						if(req.getHeader().getClusterId() == 4)
						ConnectionManager.interClusterBroadcast(request);
						System.out.println("Sent to all clusters");
						
						//Send the messages to all the Nodes with in the cluster.
						ConnectionManager.broadcastToClient(request);
						
						generateImage(req);
						
						
						
						//If the node is a non-leader node, then send the image to the leader for further processing
					}else if(leaderNode != nodeId ){
						//Build new Request
						if(req.getHeader().getIsClient() == true){
							Request.Builder newReq = Request.newBuilder();
							Header.Builder newHeader = Header.newBuilder();
							PayLoad.Builder newPayload = PayLoad.newBuilder();
							Ping.Builder newPing = Ping.newBuilder();
							
							newHeader.setClientId(req.getHeader().getClientId());
							newHeader.setClusterId(req.getHeader().getClusterId());
							newHeader.setCaption(req.getHeader().getCaption());
							newHeader.setIsClient(false);
							
							newPing.setIsPing(false);
							
							newPayload.setData(req.getPayload().getData());
							
							
							newReq.setHeader(newHeader);
							newReq.setPing(newPing);
							newReq.setPayload(newPayload);
							System.out.println("Sending to leader");
							ConnectionManager.unicast(newReq.build());
						}else{
							generateImage(req);
						}
					}
					
					// HEY! if you find yourself here and are tempted to add
					// code to process state or requests then you are in the
					// WRONG place! This is a general routing class, all
					// request specific actions should take place in the
					// resource!

					// handle it locally - we create a new resource per
					// request. This helps in thread isolation however, it
					// creates creation burdens on the server. If
					// we use a pool instead, we can gain some relief.

//					Resource rsc = ResourceFactory.getInstance().resourceInstance(req.getHeader());
//
//					Request reply = null;
//					if (rsc == null) {
//						logger.error("failed to obtain resource for " + req);
//						reply = ResourceUtil
//								.buildError(req.getHeader(), PokeStatus.NORESOURCE, "Request not processed");
//					} else {
//						// message communication can be two-way or one-way.
//						// One-way communication will not produce a response
//						// (reply).
//						reply = rsc.process(req);
//					}
//
//					if (reply != null)
//						sq.enqueueResponse(reply, null);
				}

			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected processing failure", e);
				break;
			}
		}

		if (!forever) {
			logger.info("connection queue closing");
		}
	}
	
	//This function re-generates the image from the individual chunks and stores it to images folder
	public boolean generateImage(Request req) {
		boolean rtn = false;
		BufferedImage img;

		//saving individual image chunks in hashmap
		imgChunkMap.put(req.getPayload().getChunkId(), req.getPayload()
				.getData());
		
		//Attaching the hash map of individual chunks to the image
		imgMap.put(req.getPayload().getImgId(), imgChunkMap);

		int chunkIndex = imgMap.get(req.getPayload().getImgId()).entrySet()
				.size();
		if (chunkIndex == req.getPayload().getTotalChunks()) {
			ByteArrayOutputStream stream = new ByteArrayOutputStream();

			Iterator<Entry<Integer, ByteString>> it = imgMap
					.get(req.getPayload().getImgId()).entrySet().iterator();
			try {
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					ByteString bst = (ByteString) pair.getValue();
					stream.write(bst.toByteArray());
				}
				ByteArrayInputStream stream1 = new ByteArrayInputStream(
						stream.toByteArray());
				img = ImageIO.read(stream1);
				
				//Writing image to file system and img path to the database.
				try {
					ImageIO.write(img, "jpg", new File("../../images/"
							+ imageId + ".jpg"));
					String query = "insert into CMPE_275.Data values ("
							+ ElectionManager.getInstance().getNodeId() + ","
							+ ElectionManager.getInstance().getTermId() + ","
							+ imageId + ", '../../images/" + imageId
							+ ".png','" + req.getHeader().getCaption() + "')";
					db.execute_query(query);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				imageId++;
				rtn = true;
			} catch (IOException e) {
				e.printStackTrace();
				rtn = false;
			}

		}
		return rtn;
	}
}