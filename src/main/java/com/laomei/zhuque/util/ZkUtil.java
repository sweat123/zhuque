package com.laomei.zhuque.util;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;

import java.util.Collections;
import java.util.List;

/**
 * @author luobo
 **/
public class ZkUtil {

    public static boolean ensurePath(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.checkExists().forPath(path) != null;
        } catch (Exception ignore){
            return false;
        }
    }

    public static boolean createPathWithParent(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().creatingParentsIfNeeded().forPath(path) != null;
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean createPathWithoutParent(CuratorFramework zkCli, String path) {
        Preconditions.checkNotNull(zkCli);
        Preconditions.checkNotNull(path);
        try {
            return zkCli.create().forPath(path) != null;
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

    public static List<String> getAllChildren(CuratorFramework zkCli, String path) {
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
