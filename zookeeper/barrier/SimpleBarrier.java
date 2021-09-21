package datacloud.zookeeper.barrier;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;
import datacloud.zookeeper.util.ConfConst;

public class SimpleBarrier {

	private static ZkClient zkc;
	private String chemin_znode;
	
	public SimpleBarrier(ZkClient zkc, String chemin_znode ) {
		this.zkc=zkc;
		this.chemin_znode = chemin_znode;

		try {
			if(zkc.zk().exists(chemin_znode, true) == null) {
				zkc.zk().create(chemin_znode, ConfConst.EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void sync() {
		try {
			synchronized(zkc) {
				while(zkc.zk().exists(chemin_znode, false) != null) {
					zkc.wait();
				}
				zkc.notifyAll();
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
