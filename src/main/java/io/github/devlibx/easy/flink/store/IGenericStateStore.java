package io.github.devlibx.easy.flink.store;

public interface IGenericStateStore {
    /**
     * Persist the state to Store
     */
    void persist(Key key, GenericState state);

    /**
     * Get generic state from Store
     */
    GenericState get(Key key);
}
