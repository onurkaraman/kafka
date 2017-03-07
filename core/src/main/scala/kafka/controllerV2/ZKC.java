package kafka.controllerV2;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class ZKC implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(ZKC.class);
    private final String _connectString;
    private final int _sessionTimeoutMs;
    private final int _connectionTimeoutMs;
    private final EventHandler _eventHandler;
    private final ConcurrentHashMap<String, Set<ZNodeChangeListener>> _zNodeChangeListenersPerPath;
    private ZooKeeper _zookeeper;

    public ZKC(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, EventHandler eventHandler) {
        _connectString = connectString;
        _sessionTimeoutMs = sessionTimeoutMs;
        _connectionTimeoutMs = connectionTimeoutMs;
        _eventHandler = eventHandler;
        _zNodeChangeListenersPerPath = new ConcurrentHashMap<>();
        _zookeeper = null;
    }

    public synchronized void create(String path, byte[] data, List<ACL> acls, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
        _zookeeper.create(path, data, acls, createMode, wrapStringCallback(cb), ctx);
    }
    public synchronized void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx) {
        _zookeeper.delete(path, version, wrapVoidCallback(cb), ctx);
    }
    public synchronized void exists(String path, AsyncCallback.StatCallback cb, Object ctx) {
        _zookeeper.exists(path, this, wrapStatCallback(cb), ctx);
    }
    public synchronized void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) {
        _zookeeper.sync(path, wrapVoidCallback(cb), ctx);
    }
    public synchronized void getData(String path, AsyncCallback.DataCallback cb, Object ctx) {
        _zookeeper.getData(path, this, wrapDataCallback(cb), ctx);
    }
    public synchronized void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx) {
        _zookeeper.setData(path, data, version, wrapStatCallback(cb), ctx);
    }
    public synchronized void getChildren(String path, AsyncCallback.Children2Callback cb, Object ctx) {
        _zookeeper.getChildren(path, this, wrapChildren2Callback(cb), ctx);
    }
    public synchronized void getACL(String path, AsyncCallback.ACLCallback cb, Object ctx) {
        _zookeeper.getACL(path, null, wrapACLCallback(cb), ctx);
    }
    public synchronized void setACL(String path, List<ACL> acls, int version, AsyncCallback.StatCallback cb, Object ctx) {
        _zookeeper.setACL(path, acls, version, wrapStatCallback(cb), ctx);
    }

    public synchronized void multi(Iterable<Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
        _zookeeper.multi(ops, wrapMultiCallback(cb), ctx);
    }

    public void registerZNodeChangeListener(String path, ZNodeChangeListener zNodeChangeListener) {
        synchronized (_zNodeChangeListenersPerPath) {
            if (!_zNodeChangeListenersPerPath.containsKey(path)) {
                _zNodeChangeListenersPerPath.put(path, new CopyOnWriteArraySet<ZNodeChangeListener>());
            }
            _zNodeChangeListenersPerPath.get(path).add(zNodeChangeListener);
        }
    }

    public void unregisterZNodeChangeListener(String path, ZNodeChangeListener zNodeChangeListener) {
        synchronized (_zNodeChangeListenersPerPath) {
            if (_zNodeChangeListenersPerPath.containsKey(path)) {
                _zNodeChangeListenersPerPath.get(path).remove(zNodeChangeListener);
                if (_zNodeChangeListenersPerPath.get(path).isEmpty()) {
                    _zNodeChangeListenersPerPath.remove(path);
                }
            }
        }
    }

    @Override
    public void process(final WatchedEvent event) {
        if (isStateChangeWatchedEvent(event)) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                _eventHandler.unpause();
            } else if (event.getState() == Event.KeeperState.Disconnected) {
                _eventHandler.pause();
            } else if (event.getState() == Event.KeeperState.Expired) {
                _eventHandler.pause();
                _eventHandler.clear();
                try {
                    reinitSession();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else if (isZnodeChangeWatchedEvent(event)) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (ZNodeChangeListener zNodeChangeListener : _zNodeChangeListenersPerPath.get(event.getPath())) {
                        triggerZNodeChangeListener(zNodeChangeListener, event);
                    }
                }
            };
            _eventHandler.add(runnable);
        }
    }

    private boolean isStateChangeWatchedEvent(WatchedEvent event) {
        return event.getPath() == null;
    }

    private boolean isZnodeChangeWatchedEvent(WatchedEvent event) {
        return event.getType() == Event.EventType.NodeCreated ||
                event.getType() == Event.EventType.NodeDeleted ||
                event.getType() == Event.EventType.NodeDataChanged ||
                event.getType() == Event.EventType.NodeChildrenChanged;
    }

    private void triggerZNodeChangeListener(ZNodeChangeListener zNodeChangeListener, WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeCreated) {
            zNodeChangeListener.onZNodeCreation(event.getPath());
        } else if (event.getType() == Event.EventType.NodeDeleted) {
            zNodeChangeListener.onZNodeDeletion(event.getPath());
        } else if (event.getType() == Event.EventType.NodeDataChanged) {
            zNodeChangeListener.onZNodeDataChange(event.getPath());
        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
            zNodeChangeListener.onZNodeChildrenChange(event.getPath());
        }
    }

    public synchronized void reinitSession() throws IOException, InterruptedException {
        if (_zookeeper != null) {
            _zookeeper.close();
        }
        _zookeeper = new ZooKeeper(_connectString, _sessionTimeoutMs, this);
    }

    public synchronized void close() throws InterruptedException {
        _zNodeChangeListenersPerPath.clear();
        _zookeeper.close();
        _eventHandler.close();
        _zookeeper = null;
    }

    private AsyncCallback.StatCallback wrapStatCallback(final AsyncCallback.StatCallback statCallback) {
        if (statCallback == null) return null;
        return new AsyncCallback.StatCallback() {
            @Override
            public void processResult(final int rc, final String path, final Object ctx, final Stat stat) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        statCallback.processResult(rc, path, ctx, stat);
                    }
                };
                enqueueRunnable(rc, runnable);
            }
        };
    }

    private AsyncCallback.DataCallback wrapDataCallback(final AsyncCallback.DataCallback dataCallback) {
        if (dataCallback == null) return null;
        return new AsyncCallback.DataCallback() {
            @Override
            public void processResult(final int rc, final String path, final Object ctx, final byte[] data, final Stat stat) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        dataCallback.processResult(rc, path, ctx, data, stat);
                    }
                };
                enqueueRunnable(rc, runnable);
            }
        };
    }

    private AsyncCallback.ACLCallback wrapACLCallback(final AsyncCallback.ACLCallback aclCallback) {
        if (aclCallback == null) return null;
        return new AsyncCallback.ACLCallback() {
            @Override
            public void processResult(final int rc, final String path, final Object ctx, final List<ACL> acl, final Stat stat) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        aclCallback.processResult(rc, path, ctx, acl, stat);
                    }
                };
                enqueueRunnable(rc, runnable);
            }
        };
    }

    private AsyncCallback.Children2Callback wrapChildren2Callback(final AsyncCallback.Children2Callback children2Callback) {
        if (children2Callback == null) return null;
        return new AsyncCallback.Children2Callback() {
            @Override
            public void processResult(final int rc, final String path, final Object ctx, final List<String> children, final Stat stat) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        children2Callback.processResult(rc, path, ctx, children, stat);
                    }
                };
                enqueueRunnable(rc, runnable);
            }
        };
    }

    private AsyncCallback.StringCallback wrapStringCallback(final AsyncCallback.StringCallback stringCallback) {
        if (stringCallback == null) return null;
        return new AsyncCallback.StringCallback() {
            @Override
            public void processResult(final int rc, final String path, final Object ctx, final String name) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        stringCallback.processResult(rc, path, ctx, name);
                    }
                };
                enqueueRunnable(rc, runnable);
            }
        };
    }

    private AsyncCallback.VoidCallback wrapVoidCallback(final AsyncCallback.VoidCallback voidCallback) {
        if (voidCallback == null) return null;
        return new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(final int rc, final String path, final Object ctx) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        voidCallback.processResult(rc, path, ctx);
                    }
                };
                enqueueRunnable(rc, runnable);
            }
        };
    }

    private AsyncCallback.MultiCallback wrapMultiCallback(final AsyncCallback.MultiCallback multiCallback) {
        if (multiCallback == null) return null;
        return new AsyncCallback.MultiCallback() {
            @Override
            public void processResult(final int rc, final String path, final Object ctx, final List<OpResult> opResults) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        multiCallback.processResult(rc, path, ctx, opResults);
                    }
                };
                enqueueRunnable(rc, runnable);
            }
        };
    }

    private void enqueueRunnable(int rc, Runnable runnable) {
        boolean connectionLoss = KeeperException.Code.get(rc) == KeeperException.Code.CONNECTIONLOSS;
        boolean sessionExpired = KeeperException.Code.get(rc) == KeeperException.Code.SESSIONEXPIRED;
        if (connectionLoss) {
            _eventHandler.pause();
            _eventHandler.add(runnable);
        } else if (sessionExpired) {
            _eventHandler.pause();
            _eventHandler.clear();
            try {
                reinitSession();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            _eventHandler.unpause();
            _eventHandler.add(runnable);
        }
    }
}

interface ZNodeChangeListener {
    void onZNodeCreation(String path);
    void onZNodeDeletion(String path);
    void onZNodeDataChange(String path); // TODO: provide the data? how to handle errors from getData?
    void onZNodeChildrenChange(String path); // TODO: provide the children? how to handle errors from getChildren?
}