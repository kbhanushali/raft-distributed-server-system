package poke.server.comm.monitor;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.Image.Request;
import poke.core.Mgmt.Management;
import poke.server.management.ManagementQueue;
import poke.server.managers.HeartbeatData;
import poke.server.monitor.MonitorListener;

public class CommStubListner implements CommMonitorListener {
	protected static Logger logger = LoggerFactory.getLogger("management");


	public CommStubListner() {
		
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.monitor.MonitorListener#getListenerID()
	 */
	@Override
	public Integer getListenerID() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.monitor.MonitorListener#onMessage(eye.Comm.Management)
	 */
	@Override
	public void onMessage(Request msg, ChannelHandlerContext ctx) {
		if (logger.isDebugEnabled())
			logger.debug("Data received ");
		
		System.out.println("Data is" +msg.getHeader().getCaption());
	}

	@Override
	public void connectionClosed() {
		// note a closed management port is likely to indicate the primary port
		// has failed as well
	}

	@Override
	public void connectionReady() {
		// do nothing at the moment
	}
}
