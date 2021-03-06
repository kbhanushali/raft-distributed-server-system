/*
 * copyright 2014, gash
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
package poke.server.managers;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.Image.Request;
import poke.core.Mgmt.Management;

/**
 * the connection map for server-to-server communication.
 * 
 * Note the connections/channels are initialized through the heartbeat manager
 * as it starts (and maintains) the connections through monitoring of processes.
 * 
 * 
 * TODO refactor to make this the consistent form of communication for the rest
 * of the code
 * 
 * @author gash
 * 
 */
public class ConnectionManager {
	protected static Logger logger = LoggerFactory.getLogger("management");

	/** node ID to channel 
	 * clientConnections holds all the connections from a client to the server.
	 * mgmtConnections has all the intra/inter cluster communication to and from all the nodes in the network.
	 * interClusterConnections is a HashMap that maintains the channel along with the node ID.
	 * Each of the communication channels are a HashMap that hold a mapping of the nodeId and the channel
	 * */
	
	private static HashMap<Integer, Channel> connections = new HashMap<Integer, Channel>();
	private static HashMap<Integer, Channel> mgmtConnections = new HashMap<Integer, Channel>();
	private static HashMap<Integer, Channel> clientConnections = new HashMap<Integer, Channel>();
	private static HashMap<Integer, Channel> interClusterConnections = new HashMap<Integer, Channel>();

	public static void addConnection(Integer nodeId, Channel channel,
			boolean isMgmt) {
		logger.info("ConnectionManager adding connection to " + nodeId);

		if (isMgmt) {
			mgmtConnections.put(nodeId, channel);

		} else
			connections.put(nodeId, channel);
	}

	public static void addClientConnection(Integer clientId, Channel channel) {
		logger.info("ConnectionManager adding connection to client " + clientId);

		System.out.println("adding client connection");
		if (!clientConnections.containsKey(clientId))
			clientConnections.put(clientId, channel);
	}

	// Added for interCluster communication 
	public static void addinterClusterConnection(Integer ClusterId,
			Channel channel) {
		logger.info("ConnectionManager adding connection to Leader of Cluster "
				+ ClusterId);

		System.out.println("adding client connection");
		if (!interClusterConnections.containsKey(ClusterId))
			interClusterConnections.put(ClusterId, channel);
	}

	public static Channel getConnection(Integer nodeId, boolean isMgmt) {

		if (isMgmt)
			return mgmtConnections.get(nodeId);
		else
			return connections.get(nodeId);
	}
	
	public static HashMap<Integer, Channel> getAllConnections(boolean isMgmt) {

		if (isMgmt)
			return mgmtConnections;
		else
			return connections;
	}

	public static Channel getClientConnection(Integer clientId) {
		return clientConnections.get(clientId);
	}

	public synchronized static void removeConnection(Integer nodeId,
			boolean isMgmt) {
		if (isMgmt)
			mgmtConnections.remove(nodeId);
		else
			connections.remove(nodeId);
	}

	public synchronized static void removeClientConnection(Integer clientId) {
		clientConnections.remove(clientId);
	}

	public synchronized static void removeClientConnection(Channel channel) {
		System.out.println("removing client connection");

		if (!clientConnections.containsValue(channel)) {
			return;
		}

		for (Integer nid : clientConnections.keySet()) {
			if (channel == clientConnections.get(nid)) {
				clientConnections.remove(nid);
				break;
			}
		}

	}

	public synchronized static void removeConnection(Channel channel,
			boolean isMgmt) {

		if (isMgmt) {
			if (!mgmtConnections.containsValue(channel)) {
				return;
			}

			for (Integer nid : mgmtConnections.keySet()) {
				if (channel == mgmtConnections.get(nid)) {
					mgmtConnections.remove(nid);
					break;
				}
			}
		} else {
			if (!connections.containsValue(channel)) {
				return;
			}

			for (Integer nid : connections.keySet()) {
				if (channel == connections.get(nid)) {
					connections.remove(nid);
					break;
				}
			}
		}
	}

	public synchronized static void broadcast(Request req) {
		if (req == null)
			return;

		for (Channel ch : connections.values()) {
			ch.write(req);
			ch.flush();
		}
	}
// Sends out the data to the nodes in the interCluster set up
	public synchronized static void interClusterBroadcast(Request req) {
		if (req == null)
			return;
		System.out.println("interClusterConnections size "
				+ interClusterConnections.size());
		for (Channel ch : interClusterConnections.values()) {
			System.out.println("ch port " + ch.toString());
			ch.write(req);
			ch.flush();
		}
	}
// This function broadcasts all the to the client on the appropriate channel
	public synchronized static void broadcastToClient(Request req) {
		if (req == null)
			return;

		Map<Integer, Channel> map = new HashMap<Integer, Channel>();
		for (Map.Entry<Integer, Channel> entry : map.entrySet()) {
			System.out.println("Key = " + entry.getKey() + ", Value = "
					+ entry.getValue());
			Channel ch = entry.getValue();
			if(entry.getKey() != req.getHeader().getClientId()){
				ch.write(req);
				ch.flush();
			}
		}
//		for (Channel ch : clientConnections.values()) {
//			ch.write(req);
//			ch.flush();
//		}
	}
// Leader calls broadcasr to send the data to all the nodes in the cluster.
	public synchronized static void broadcast(Management mgmt) {
		if (mgmt == null)
			return;

		for (Channel ch : mgmtConnections.values())
			ch.write(mgmt);
	}
// In case a node gets the data, it has to be forwarded to the leader for it to be sent out to all other nodes in the cluster. - Therefore Unicast
	// Unicast is called to send the data from a node to the leader.//Shreya
	public synchronized static void unicast(Request req) {
		if (req == null)
			return;

		Integer leaderNode = ElectionManager.getInstance().whoIsTheLeader();
		if (leaderNode != null)
			connections.get(leaderNode).write(req);
		connections.get(leaderNode).flush();
	}
	
	public synchronized static void unicast(Request req, Integer nodeId) {
		if (req == null)
			return;

		//Integer leaderNode = ElectionManager.getInstance().whoIsTheLeader();
		Integer DestNode = nodeId;
		if (DestNode != null)
			connections.get(DestNode).write(req);
		connections.get(DestNode).flush();
	}
	
	

	public static int getNumMgmtConnections() {
		return mgmtConnections.size();
	}
}
