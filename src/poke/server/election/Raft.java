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

//We have used the FloodMax algorithm skeleton and modified the code to implement Raft
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
	private ElectionState currentState;
	private int maxHops = -1; // unlimited
	private ElectionListener eListener;
	private Integer lastSeenTerm; // last seen term to be used for casting max one vote
	private int voteCount = 1;
	private int abstainCount = 0;
	
	
	@Override
	public void setListener(ElectionListener listener) {
		this.eListener = listener;
		
	}

	@Override
	public synchronized void clear() {
		currentState = null;
		
	}

	@Override
	public boolean isElectionInprogress() {
		return currentState != null;
	}

	@Override
	public Integer getTermId() {
		return eListener.getTermId();
	}

	@Override
	public Integer createTermID() {
		return ElectionIDGenerator.nextID();
	}

	@Override
	public Integer getWinner() {
		if (currentState == null)
			return null;
		else if (currentState.state.getNumber() == ElectAction.DECLAREELECTION_VALUE)
			return currentState.candidate;
		else
			return null;
	}

	/* ***************************************************
	 * Process management message
	 * Check if the timer has expired
	 * Take appropriate action as per the message received
	 * 
	 * **************************************************** */
	@Override
	public Management process(Management mgmt) {
		if (!mgmt.hasElection())
			return null;

		LeaderElection requset = mgmt.getElection();
		if (requset.getExpires() <= System.currentTimeMillis()) {
			// election has expired without a conclusion?
		}

		Management rtn = null;

		if (requset.getAction().getNumber() == ElectAction.DECLAREELECTION_VALUE) {
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
			
			if (logger.isDebugEnabled()) {
			}

			System.out.println("\n\n*********************************************************");
			System.out.println(" RAFT ELECTION: Election declared");
			System.out.println("   Term ID:  " + requset.getTermId());
			System.out.println("   Last Log Index:  " + requset.getLastLogIndex());
			System.out.println("   Rcv from:     Node " + mgmt.getHeader().getOriginator());
			System.out.println("   Expires:      " + new Date(requset.getExpires()));
			System.out.println("   Nominates:    Node " + requset.getCandidateId());
			System.out.println("   Desc:         " + requset.getDesc());
			System.out.print("   Routing tbl:  [");
			for (VectorClock rp : rtes)
			System.out.print("Node " + rp.getNodeId() + " (" + rp.getVersion() + "," + rp.getTime() + "), ");
			System.out.println("]");
			System.out.println("*********************************************************\n\n");


			boolean isNew = updateCurrent(requset);
			rtn = castVote(mgmt, isNew);

		} else if (requset.getAction().getNumber() == ElectAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode
			logger.info("TODO: no one was elected, I am dropping into standby mode");
			this.clear();
			notify(false, null);
		} else if (requset.getAction().getNumber() == ElectAction.DECLAREWINNER_VALUE) {
			// some node declared itself the leader
			logger.info("Election " + requset.getTermId() + ": Node " + requset.getCandidateId() + " is declared the leader");
			updateCurrent(mgmt.getElection());
			eListener.setState(RState.Follower);
			currentState.active = false; // it's over
			notify(true, requset.getCandidateId());
		} else if (requset.getAction().getNumber() == ElectAction.ABSTAIN_VALUE) {
			abstainCount++;
			if(abstainCount >= ((ConnectionManager.getNumMgmtConnections()+1)/2)+1)
			{
				rtn = abstainCandidature(mgmt);
				notify(false, this.nodeId);
				eListener.setState(RState.Follower);
				this.clear();
				voteCount = 1;
				abstainCount = 0;
			}
		} else if (requset.getAction().getNumber() == ElectAction.NOMINATE_VALUE) {
			if(requset.getCandidateId() == this.nodeId){
				voteCount++;
				if(voteCount >=((ConnectionManager.getNumMgmtConnections()+1)/2)+1){
					rtn = declareWinner(mgmt);
					notify(true, this.nodeId);
					eListener.setState(RState.Leader);
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
		LeaderElection requset = mgmt.getElection();
		
		LeaderElection.Builder ebuilder = LeaderElection.newBuilder();
		MgmtHeader.Builder mgmtBuilder = MgmtHeader.newBuilder();
		mgmtBuilder.setTime(System.currentTimeMillis());
		mgmtBuilder.setSecurityCode(-999);

		// reversing path. If I'm the farthest a message can travel, reverse the
		// sending
		if (ebuilder.getHops() == 0)
			mgmtBuilder.clearPath();
		else
			mgmtBuilder.addAllPath(mgmt.getHeader().getPathList());

		mgmtBuilder.setOriginator(mgmt.getHeader().getOriginator());

		ebuilder.setTermId(requset.getTermId());
		ebuilder.setAction(ElectAction.DECLAREVOID);
		
		ebuilder.setDesc(requset.getDesc());
		ebuilder.setLastLogIndex(requset.getLastLogIndex());
		ebuilder.setExpires(requset.getExpires());
		ebuilder.setCandidateId(requset.getCandidateId());
		if (requset.getHops() == -1)
			ebuilder.setHops(-1);
		else
			ebuilder.setHops(requset.getHops() - 1);

		if (ebuilder.getHops() == 0) {
			// reverse travel of the message to ensure it gets back to
			// the originator
			ebuilder.setHops(mgmt.getHeader().getPathCount());

			// no clear winner, send back the candidate with the highest
			// known ID. So, if a candidate sees itself, it will
			// declare itself to be the winner (see above).
		} else {
			// forwarding the message on so, keep the history where the
			// message has been
			mgmtBuilder.addAllPath(mgmt.getHeader().getPathList());
		}
		

		// add myself (may allow duplicate entries, if cycling is allowed)
		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(this.nodeId);
		rpb.setTime(System.currentTimeMillis());
		rpb.setVersion(requset.getTermId());
		mgmtBuilder.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mgmtBuilder.build());
		mb.setElection(ebuilder.build());

		return mb.build(); 
	}
	
	
	//Cast a vote if not casted already
	private synchronized Management castVote(Management mgmt, boolean isNew) {
		if (!mgmt.hasElection())
			return null;

		if (currentState == null || !currentState.isActive()) {
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
		
		if(eListener.getTermId() < req.getTermId() && eListener.getLastLogIndex() <= req.getLastLogIndex())
		{
			elb.setAction(ElectAction.NOMINATE);
			eListener.setTermId(req.getTermId());
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
		if (eListener != null)
			eListener.concludeWith(success, leader);
	}

	private boolean updateCurrent(LeaderElection req) {
		boolean isNew = false;

		if (currentState == null) {
			currentState = new ElectionState();
			isNew = true;
		}

		//current.electionID = req.getElectId();
		currentState.candidate = req.getCandidateId();
		currentState.desc = req.getDesc();
		currentState.maxDuration = req.getExpires();
		currentState.startedOn = System.currentTimeMillis();
		currentState.state = req.getAction();
		currentState.id = -1; // TODO me or sender?
		currentState.active = true;

		return isNew;
	}
	
	public void setMaxHops(int maxHops) {
		this.maxHops = maxHops;
	}
}
