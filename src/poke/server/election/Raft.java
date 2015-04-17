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
package poke.server.election;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.LeaderElection;
import poke.core.Mgmt.LeaderElection.ElectAction;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.VectorClock;
import poke.server.managers.ConnectionManager;
import poke.server.managers.ElectionManager.RState;

public class Raft implements Election{

	protected static Logger logger = LoggerFactory.getLogger("Raft");

	private Integer nodeId;
	private ElectionState current;
	private int maxHops = -1; // unlimited
	private ElectionListener listener;
	private Integer lastSeenTerm; // last seen term to be used for casting max one vote
	private int voteCount = 1;
	private int abstainCount = 0;
	
	
	@Override
	public void setListener(ElectionListener listener) {
		this.listener = listener;
		
	}

	@Override
	public synchronized void clear() {
		current = null;
		
	}

	@Override
	public boolean isElectionInprogress() {
		return current != null;
	}

	@Override
	public Integer getTermId() {
		return listener.getTermId();
	}

	@Override
	public Integer createTermID() {
		return ElectionIDGenerator.nextID();
	}

	@Override
	public Integer getWinner() {
		if (current == null)
			return null;
		else if (current.state.getNumber() == ElectAction.DECLAREELECTION_VALUE)
			return current.candidate;
		else
			return null;
	}

	@Override
	public Management process(Management mgmt) {
		if (!mgmt.hasElection())
			return null;

		LeaderElection req = mgmt.getElection();
		if (req.getExpires() <= System.currentTimeMillis()) {
			// election has expired without a conclusion?
		}

		Management rtn = null;

		if (req.getAction().getNumber() == ElectAction.DECLAREELECTION_VALUE) {
			// an election is declared!

			// required to eliminate duplicate messages - on a declaration,
			// should not happen if the network does not have cycles
			List<VectorClock> rtes = mgmt.getHeader().getPathList();
			for (VectorClock rp : rtes) {
				if (rp.getNodeId() == this.nodeId) {
					// message has already been sent to me, don't use and
					// forward
					return null;
				}
			}

			// I got here because the election is unknown to me

			// this 'if debug is on' should cover the below dozen or so
			// println()s. It is here to help with seeing when an election
			// occurs.
			if (logger.isDebugEnabled()) {
			}

			System.out.println("\n\n*********************************************************");
			System.out.println(" FLOOD MAX ELECTION: Election declared");
			System.out.println("   Term ID:  " + req.getTermId());
			System.out.println("   Last Log Index:  " + req.getLastLogIndex());
			System.out.println("   Rcv from:     Node " + mgmt.getHeader().getOriginator());
			System.out.println("   Expires:      " + new Date(req.getExpires()));
			System.out.println("   Nominates:    Node " + req.getCandidateId());
			System.out.println("   Desc:         " + req.getDesc());
			System.out.print("   Routing tbl:  [");
			for (VectorClock rp : rtes)
				System.out.print("Node " + rp.getNodeId() + " (" + rp.getVersion() + "," + rp.getTime() + "), ");
			System.out.println("]");
			System.out.println("*********************************************************\n\n");

			
			// sync master IDs to current election
			//ElectionIDGenerator.setMasterID(req.getElectId());

			/**
			 * a new election can be declared over an existing election.
			 * 
			 * TODO need to have an monotonically increasing ID that we can test
			 */
			boolean isNew = updateCurrent(req);
			rtn = castVote(mgmt, isNew);

		} else if (req.getAction().getNumber() == ElectAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode
			logger.info("TODO: no one was elected, I am dropping into standby mode");
			this.clear();
			notify(false, null);
		} else if (req.getAction().getNumber() == ElectAction.DECLAREWINNER_VALUE) {
			// some node declared itself the leader
			logger.info("Election " + req.getTermId() + ": Node " + req.getCandidateId() + " is declared the leader");
			updateCurrent(mgmt.getElection());
			listener.setState(RState.Follower);
			current.active = false; // it's over
			notify(true, req.getCandidateId());
		} else if (req.getAction().getNumber() == ElectAction.ABSTAIN_VALUE) {
			abstainCount++;
			if(abstainCount >= ((ConnectionManager.getNumMgmtConnections()+1)/2)+1)
			{
				rtn = abstainCandidature(mgmt);
				notify(false, this.nodeId);
				listener.setState(RState.Follower);
				this.clear();
				voteCount = 1;
				abstainCount = 0;
			}
		} else if (req.getAction().getNumber() == ElectAction.NOMINATE_VALUE) {
			if(req.getCandidateId() == this.nodeId){
				voteCount++;
				if(voteCount >=((ConnectionManager.getNumMgmtConnections()+1)/2)+1){
					rtn = declareWinner(mgmt);
					notify(true, this.nodeId);
					listener.setState(RState.Leader);
					this.clear();
					voteCount = 1;
					abstainCount = 0;
				}
			}
//			boolean isNew = updateCurrent(mgmt.getElection());
//			rtn = castVote(mgmt, isNew);
		} else {
			// this is me!
		}

		return rtn;
	}

	
	private synchronized Management abstainCandidature(Management mgmt){
		LeaderElection req = mgmt.getElection();
		
		LeaderElection.Builder elb = LeaderElection.newBuilder();
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		// reversing path. If I'm the farthest a message can travel, reverse the
		// sending
		if (elb.getHops() == 0)
			mhb.clearPath();
		else
			mhb.addAllPath(mgmt.getHeader().getPathList());

		mhb.setOriginator(mgmt.getHeader().getOriginator());

		elb.setTermId(req.getTermId());
		elb.setAction(ElectAction.DECLAREVOID);
		
		elb.setDesc(req.getDesc());
		elb.setLastLogIndex(req.getLastLogIndex());
		elb.setExpires(req.getExpires());
		elb.setCandidateId(req.getCandidateId());
		if (req.getHops() == -1)
			elb.setHops(-1);
		else
			elb.setHops(req.getHops() - 1);

		if (elb.getHops() == 0) {
			// reverse travel of the message to ensure it gets back to
			// the originator
			elb.setHops(mgmt.getHeader().getPathCount());

			// no clear winner, send back the candidate with the highest
			// known ID. So, if a candidate sees itself, it will
			// declare itself to be the winner (see above).
		} else {
			// forwarding the message on so, keep the history where the
			// message has been
			mhb.addAllPath(mgmt.getHeader().getPathList());
		}
		

		// add myself (may allow duplicate entries, if cycling is allowed)
		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(this.nodeId);
		rpb.setTime(System.currentTimeMillis());
		rpb.setVersion(req.getTermId());
		mhb.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setElection(elb.build());

		return mb.build(); 
	}
	
	private synchronized Management castVote(Management mgmt, boolean isNew) {
		if (!mgmt.hasElection())
			return null;

		if (current == null || !current.isActive()) {
			return null;
		}

		LeaderElection req = mgmt.getElection();
		if (req.getExpires() <= System.currentTimeMillis()) {
			logger.info("Node " + this.nodeId + " says election expired - not voting");
			return null;
		}

		logger.info("casting vote in election for term" + req.getTermId());

		// DANGER! If we return because this node ID is in the list, we have a
		// high chance an election will not converge as the maxHops determines
		// if the graph has been traversed!
		boolean allowCycles = true;

		if (!allowCycles) {
			List<VectorClock> rtes = mgmt.getHeader().getPathList();
			for (VectorClock rp : rtes) {
				if (rp.getNodeId() == this.nodeId) {
					// logger.info("Node " + this.nodeId +
					// " already in the routing path - not voting");
					return null;
				}
			}
		}

		// okay, the message is new (to me) so I want to determine if I should
		// nominate myself

		LeaderElection.Builder elb = LeaderElection.newBuilder();
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		// reversing path. If I'm the farthest a message can travel, reverse the
		// sending
		if (elb.getHops() == 0)
			mhb.clearPath();
		else
			mhb.addAllPath(mgmt.getHeader().getPathList());

		mhb.setOriginator(mgmt.getHeader().getOriginator());

		elb.setTermId(req.getTermId());
		
		if(listener.getTermId() < req.getTermId() && listener.getLastLogIndex() <= req.getLastLogIndex())
		{
			elb.setAction(ElectAction.NOMINATE);
			listener.setTermId(req.getTermId());
			ElectionIDGenerator.setMasterID(req.getTermId());
		}
		else
			elb.setAction(ElectAction.ABSTAIN);
		
		elb.setDesc(req.getDesc());
		elb.setLastLogIndex(req.getLastLogIndex());
		elb.setExpires(req.getExpires());
		elb.setCandidateId(req.getCandidateId());
		if (req.getHops() == -1)
			elb.setHops(-1);
		else
			elb.setHops(req.getHops() - 1);

		if (elb.getHops() == 0) {
			// reverse travel of the message to ensure it gets back to
			// the originator
			elb.setHops(mgmt.getHeader().getPathCount());

			// no clear winner, send back the candidate with the highest
			// known ID. So, if a candidate sees itself, it will
			// declare itself to be the winner (see above).
		} else {
			// forwarding the message on so, keep the history where the
			// message has been
			mhb.addAllPath(mgmt.getHeader().getPathList());
		}
		
		
		// my vote
//		if (req.getCandidateId() == this.nodeId) {
//			// if I am not in the list and the candidate is myself, I can
//			// declare myself to be the leader.
//			//
//			// this is non-deterministic as it assumes the message has
//			// reached all nodes in the network (because we know the
//			// diameter or the number of nodes).
//			//
//			// can end up with a partitioned graph of leaders if hops <
//			// diameter!
//
//			// this notify goes out to on-node listeners and will arrive before
//			// the other nodes receive notice.
//			notify(true, this.nodeId);
//
//			elb.setAction(ElectAction.DECLAREWINNER);
//			elb.setHops(mgmt.getHeader().getPathCount());
//			logger.info("Node " + this.nodeId + " is declaring itself the leader");
//		} else {
//			if (req.getCandidateId() < this.nodeId)
//				elb.setCandidateId(this.nodeId);
//
//			if (req.getHops() == -1)
//				elb.setHops(-1);
//			else
//				elb.setHops(req.getHops() - 1);
//
//			if (elb.getHops() == 0) {
//				// reverse travel of the message to ensure it gets back to
//				// the originator
//				elb.setHops(mgmt.getHeader().getPathCount());
//
//				// no clear winner, send back the candidate with the highest
//				// known ID. So, if a candidate sees itself, it will
//				// declare itself to be the winner (see above).
//			} else {
//				// forwarding the message on so, keep the history where the
//				// message has been
//				mhb.addAllPath(mgmt.getHeader().getPathList());
//			}
//		}

		// add myself (may allow duplicate entries, if cycling is allowed)
		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(this.nodeId);
		rpb.setTime(System.currentTimeMillis());
		rpb.setVersion(req.getTermId());
		mhb.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setElection(elb.build());

		return mb.build();
	}
	
	
	private Management declareWinner(Management mgmt){
		
		LeaderElection req = mgmt.getElection();
		
		LeaderElection.Builder elb = LeaderElection.newBuilder();
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		// reversing path. If I'm the farthest a message can travel, reverse the
		// sending
		if (elb.getHops() == 0)
			mhb.clearPath();
		else
			mhb.addAllPath(mgmt.getHeader().getPathList());

		mhb.setOriginator(mgmt.getHeader().getOriginator());

		elb.setTermId(req.getTermId());
		elb.setAction(ElectAction.DECLAREWINNER);
		
		elb.setDesc(req.getDesc());
		elb.setLastLogIndex(req.getLastLogIndex());
		elb.setExpires(req.getExpires());
		elb.setCandidateId(req.getCandidateId());
		if (req.getHops() == -1)
			elb.setHops(-1);
		else
			elb.setHops(req.getHops() - 1);

		if (elb.getHops() == 0) {
			// reverse travel of the message to ensure it gets back to
			// the originator
			elb.setHops(mgmt.getHeader().getPathCount());

			// no clear winner, send back the candidate with the highest
			// known ID. So, if a candidate sees itself, it will
			// declare itself to be the winner (see above).
		} else {
			// forwarding the message on so, keep the history where the
			// message has been
			mhb.addAllPath(mgmt.getHeader().getPathList());
		}
		

		// add myself (may allow duplicate entries, if cycling is allowed)
		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(this.nodeId);
		rpb.setTime(System.currentTimeMillis());
		rpb.setVersion(req.getTermId());
		mhb.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setElection(elb.build());

		return mb.build();
	}
	
	public Integer getNodeId() {
		return nodeId;
	}
	
	@Override
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
		
	}
	
	private void notify(boolean success, Integer leader) {
		if (listener != null)
			listener.concludeWith(success, leader);
	}

	private boolean updateCurrent(LeaderElection req) {
		boolean isNew = false;

		if (current == null) {
			current = new ElectionState();
			isNew = true;
		}

		//current.electionID = req.getElectId();
		current.candidate = req.getCandidateId();
		current.desc = req.getDesc();
		current.maxDuration = req.getExpires();
		current.startedOn = System.currentTimeMillis();
		current.state = req.getAction();
		current.id = -1; // TODO me or sender?
		current.active = true;

		return isNew;
	}
	
	public void setMaxHops(int maxHops) {
		this.maxHops = maxHops;
	}
}
