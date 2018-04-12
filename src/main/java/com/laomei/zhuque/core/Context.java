package com.laomei.zhuque.core;

import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author luobo
 **/
public class Context {

    /**
     * 表示context属于哪个topic
     */
    @Getter
    @Setter
    private String topic;

    @Getter
    @Setter
    private int partition;

    @Getter
    @Setter
    private long offset;

    private Map<String, Object> ctx;

    public static Context copyOf(Context context) {
        return new Context(context.topic, context.partition, context.offset, context.ctx);
    }

    public static Context emptyCtx(Context context) {
        return new Context(context.topic, context.partition, context.offset);
    }

    public Context(String topic, int partition, long offset) {
        this(topic, partition, offset, new HashMap<>());
    }

    private Context(String ctxId, int partition, long offset, Map<String, Object> ctx) {
        this.topic = ctxId;
        this.partition = partition;
        this.offset = offset;
        this.ctx = new HashMap<>(ctx);
    }

    public Map<String, Object> putAll(Map<String, Object> ctx) {
        this.ctx.putAll(ctx);
        return ctx;
    }

    public Object put(String key, Object value) {
        return ctx.put(key, value);
    }

    public Object get(String key) {
        return ctx.get(key);
    }

    public int size() {
        return ctx.size();
    }

    public boolean isEmpty() {
        return ctx.isEmpty();
    }

    public void clear() {
        ctx.clear();
    }

    public Set<String> keySet() {
        return ctx.keySet();
    }

    public boolean containsKey(String key) {
        return ctx.containsKey(key);
    }

    public Map<String, Object> getUnmodifiableCtx() {
        return Collections.unmodifiableMap(new HashMap<>(ctx));
    }
}
