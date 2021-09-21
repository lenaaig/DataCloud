package datacloud.zookeeper.barrier;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;
import datacloud.zookeeper.util.ConfConst;

public class BoundedBarrier {
	private static ZkClient zkc;
	private String chemin_znode;
	private static int N;

	public BoundedBarrier(ZkClient zkc, String chemin_znode, int N) {
		this.zkc=zkc;
		this.chemin_znode = chemin_znode;
		try {
			if(zkc.zk().exists(chemin_znode, true) == null) {
				zkc.zk().create(chemin_znode, ConfConst.EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
				this.N=N;
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void sync() {
		try {
			synchronized(zkc) {
				String child = zkc.zk().create(chemin_znode + "/child", ConfConst.EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				if (zkc.zk().getChildren(chemin_znode, false).size() < N) {
					try {
						zkc.wait();
					}catch (InterruptedException e1) { // recupere l'exception de l'erreur interrupted exception
						zkc.zk().delete(child, 0); //fils
						if (zkc.zk().getChildren(chemin_znode, false).size() == 0) {
							zkc.zk().delete(chemin_znode, 0); // si  plus de fils, parent
						}
						return; 
					} 
				}
				zkc.notifyAll();
				zkc.zk().delete(child, 0);
				if (zkc.zk().getChildren(chemin_znode, false).size() == 0) {
					zkc.zk().delete(chemin_znode, 0);
				}
			}
		}catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int sizeBarrier() {
		return N;
	}
}
