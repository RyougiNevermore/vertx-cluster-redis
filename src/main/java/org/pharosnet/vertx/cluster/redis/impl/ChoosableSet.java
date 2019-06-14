package org.pharosnet.vertx.cluster.redis.impl;

import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.spi.cluster.ChoosableIterable;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class ChoosableSet<T> implements ChoosableIterable<T>, Serializable {

    public ChoosableSet(int initialSize) {
        ids = new ConcurrentHashSet<>(initialSize);
    }

    private final Set<T> ids;
    private volatile Iterator<T> iter;

    public Set<T> getIds() {
        return ids;
    }

    public int size() {
        return ids.size();
    }

    public void add(T elem) {
        ids.add(elem);
    }

    public void remove(T elem) {
        ids.remove(elem);
    }

    public void merge(ChoosableSet<T> toMerge) {
        ids.addAll(toMerge.ids);
    }

    public boolean isEmpty() {
        return ids.isEmpty();
    }

    public boolean contains(T elem) {
        return ids.contains(elem);
    }

    @Override
    public Iterator<T> iterator() {
        return ids.iterator();
    }

    public synchronized T choose() {
        if (!ids.isEmpty()) {
            if (iter == null || !iter.hasNext()) {
                iter = ids.iterator();
            }
            try {
                return iter.next();
            } catch (NoSuchElementException e) {
                return null;
            }
        } else {
            return null;
        }
    }

}
