package org.apache.flink.study.proxy;


import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class PekkoInvocationHandler implements InvocationHandler {

    private Object target;

    public PekkoInvocationHandler(Object object){
        this.target = object;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
        return invokeRpc(method,args);
    }

    private Object invokeRpc(Method method,Object[] args) throws Exception {
        System.out.println("调用pekko ");
        Object res = method.invoke(target, args);
        System.out.println("调用结束");
        return res;
    }
}
