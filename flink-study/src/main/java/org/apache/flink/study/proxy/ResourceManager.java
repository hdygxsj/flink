package org.apache.flink.study.proxy;

public class ResourceManager implements ResourceManagerGateway{
    @Override
    public void registerTaskExecutor() {
        System.out.println("注册registerTaskExecutor");
    }
}
