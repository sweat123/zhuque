package com.laomei.zhuque.util;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.Collections;
import java.util.List;

/**
 * @author luobo
 **/
public class ZkUtil {

    public static String mergePathWith(String path1, String path2) {
        return path1 + "/" + path2;
    }

    public static class ZkLock {

        private static String LOCK = "lock";

        public static String lockPath(String assignmentPath) {
            return assignmentPath + "/" + LOCK;
        }

        /**
         * add assignment lock node
         * @param zkClient CuratorFramework client
         * @param assignmentPath assignment node path
         * @return true if add lock succeed;
         */
        public static boolean addLock(CuratorFramework zkClient, String assignmentPath) {
            try {
                createEphemeralPathWithoutParent(zkClient, lockPath(assignmentPath));
                return true;
            } catch (Exception ignore) {
                return false;
            }
        }

        /**
         * add assignment lock node
         * @param zkClient CuratorFramework client
         * @param assignmentPath assignment node path
         * @return true if add lock succeed;
         */
        public static boolean addLock(CuratorFramework zkClient, String assignmentPath, byte[] data) {
            try {
                createEphemeralPathWithoutParent(zkClient, lockPath(assignmentPath), data);
                return true;
            } catch (Exception ignore) {
                return false;
            }
        }

        /**
         * set data of lock node;
         * @param zkClient CuratorFramework client
         * @param assignmentPath assignment node path
         * @param data new data
         */
        public static void setLockData(CuratorFramework zkClient, String assignmentPath, byte[] data) {
            try {
                setNodeData(zkClient, lockPath(assignmentPath), data);
            } catch (Exception ignore) {
            }
        }

        /**
         * ensure assignment lock node;
         * @param zkClient CuratorFramework client
         * @param assignmentPath assignment node path
         * @return true if lock node is existent;
         */
        public static boolean ensureLock(CuratorFramework zkClient, String assignmentPath) {
            return ensurePath(zkClient, lockPath(assignmentPath));
        }

        /**
         * delete assignment lock node
         * @param zkClient CuratorFramework client
         * @param assignmentPath assignment node path
         * @return true if removing lock node succeed;
         */
        public static boolean deleteLock(CuratorFramework zkClient, String assignmentPath) {
            return deletePathWithoutChildren(zkClient, lockPath(assignmentPath));
        }
    }

    public static boolean ensurePath(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.checkExists().forPath(path) != null;
        } catch (Exception ignore){
            return false;
        }
    }

    public static boolean createPersistentPathWithoutParent(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().withMode(CreateMode.PERSISTENT).forPath(path) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean createPersistentPathWithoutParent(CuratorFramework zkCli, String path, byte[] data) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().withMode(CreateMode.PERSISTENT).forPath(path, data) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean createPersistentPathWithParent(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean createPersistentPathWithParent(CuratorFramework zkCli, String path, byte[] data) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean createEphemeralPathWithoutParent(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().withMode(CreateMode.EPHEMERAL).forPath(path) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean createEphemeralPathWithoutParent(CuratorFramework zkCli, String path, byte[] data) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().withMode(CreateMode.EPHEMERAL).forPath(path, data) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean createEphemeralPathWithParent(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean createEphemeralPathWithParent(CuratorFramework zkCli, String path, byte[] data) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static byte[] getNodeData(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.getData().forPath(path);
        } catch (Exception ignore) {
            return null;
        }
    }

    public static boolean setNodeData(CuratorFramework zkCli, String path, byte[] data) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        Preconditions.checkNotNull(data);
        try {
            return zkCli.setData().forPath(path, data) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static List<String> getChildren(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.getChildren().forPath(path);
        } catch (Exception ignore) {
            return Collections.emptyList();
        }
    }

    public static boolean deletePathWithChildren(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            zkCli.delete().deletingChildrenIfNeeded().forPath(path);
            return true;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean deletePathWithoutChildren(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            zkCli.delete().forPath(path);
            return true;
        } catch (Exception ignore) {
            return false;
        }
    }
}
