package datacloud.zookeeper.membership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;

import datacloud.zookeeper.ZkClient;
import datacloud.zookeeper.util.ConfConst;

public class ClientMembership extends ZkClient {

	List<String> l = new ArrayList<String>(); 
	
	public ClientMembership(String name, String servers) throws IOException, KeeperException, InterruptedException {
		super(name, servers);
		// TODO Auto-generated constructor stub

	}

	@Override
	public void process(WatchedEvent arg0) {
		if (arg0.getType().equals(EventType.NodeChildrenChanged) || arg0.getType().equals(Event.EventType.NodeCreated)) {
			l=getMembers();
		}
	}
	
	public List<String> getMembers() {
		try {
			l = zk().getChildren(ConfConst.ZNODEIDS, true);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return l;
	}

} 