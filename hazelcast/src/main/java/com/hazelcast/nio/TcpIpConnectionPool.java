package com.hazelcast.nio;

import com.hazelcast.nio.tcp.TcpIpConnection;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

public class TcpIpConnectionPool implements Iterable<TcpIpConnection> {

    private final Set<TcpIpConnection> connections = new CopyOnWriteArraySet<>();

    public void add(TcpIpConnection connection) {
        this.connections.add(connection);
    }

    public int size() {
        return connections.size();
    }

    public TcpIpConnection get() {
        return (TcpIpConnection) this.connections.toArray()[(int) Thread.currentThread().getId() % connections.size()];
    }

    public Collection<TcpIpConnection> getAll() {
        return Collections.unmodifiableSet(connections);
    }

    @Override
    public Iterator<TcpIpConnection> iterator() {
        return connections.iterator();
    }
}
