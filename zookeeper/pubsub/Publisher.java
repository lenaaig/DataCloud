package datacloud.zookeeper.pubsub;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import datacloud.zookeeper.ZkClient;

public class Publisher extends ZkClient{

	public Publisher(String string, String servers) throws IOException, KeeperException, InterruptedException  {
		super(string,servers);
	}

	public void publish(String topic, String message) {
		// TODO Auto-generated method stub
		String t= "/"+topic;
		try {
			Stat s = zk().exists(t, false);

			if(s == null) {
				zk().create(t, message.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}else {
				zk().setData(t, message.getBytes(), s.getVersion());
			} 
			
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		
	}
}
