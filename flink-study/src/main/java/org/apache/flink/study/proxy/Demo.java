package org.apache.flink.study.proxy;

import java.lang.reflect.Proxy;

public class Demo {
    public static void main(String[] args) {
        ResourceManager resourceManager = new ResourceManager();
        PekkoInvocationHandler pekkoInvocationHandler = new PekkoInvocationHandler(resourceManager);
        ResourceManagerGateway proxy = (ResourceManagerGateway) Proxy.newProxyInstance(
                ResourceManagerGateway.class.getClassLoader(),
                new Class<?>[]{ResourceManagerGateway.class},
                pekkoInvocationHandler);
        proxy.registerTaskExecutor();
    }
}
