package com.kvstore.storage;

import java.io.IOException;

public interface StateMachine {
    public void apply(String command) throws IOException;

    public String get(String key);
}
