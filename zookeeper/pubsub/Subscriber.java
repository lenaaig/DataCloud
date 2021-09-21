package datacloud.zookeeper.pubsub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import datacloud.zookeeper.ZkClient;

public class Subscriber extends ZkClient{

	private HashMap<String, List<String>> maptopics;

	public Subscriber(String string, String servers) throws IOException, KeeperException, InterruptedException {
		// TODO Auto-generated constructor stub
		super(string,servers);
		maptopics=new HashMap<>();
	}

	public List<String> received(String topic) {
		// TODO Auto-generated method stub
		if (maptopics.get(topic) ==null) {
			return Collections.emptyList();
		}
		return maptopics.get(topic);
	}

	public void subscribe(String topic) {
		// TODO Auto-generated method stub
		try {
			zk().exists("/"+topic, true); //abonne
			maptopics.put(topic, new ArrayList<String>()); //ajout
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		if(arg0.getType().equals(Event.EventType.NodeDataChanged) || arg0.getType().equals(Event.EventType.NodeCreated)) {
			String[] tmp = arg0.getPath().split("/");
			String topic = tmp[tmp.length-1];
			List<String> msgonetopic = maptopics.get(topic);
			Stat s = new Stat();
			try {
				byte[] data = zk().getData(arg0.getPath(), true, s);
				String message = new String(data);
				if (msgonetopic.contains(message)==false) {
					msgonetopic.add(message); //add dans zookeeper
					maptopics.put(topic, msgonetopic); //add dans ta map
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
