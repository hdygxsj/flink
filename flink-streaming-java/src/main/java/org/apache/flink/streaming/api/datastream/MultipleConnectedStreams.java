/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;

import static java.util.Objects.requireNonNull;

/**
* 对于连接多个输入 {@link DataStream}s 的类来说，这是一个非常基本和粗糙的存根
 * 合二为一，使用 {@link MultipleInputStreamOperator}。
 */
@Experimental
public class MultipleConnectedStreams {

    protected final StreamExecutionEnvironment environment;

    public MultipleConnectedStreams(StreamExecutionEnvironment env) {
        this.environment = requireNonNull(env);
    }

    public StreamExecutionEnvironment getExecutionEnvironment() {
        return environment;
    }

    public <OUT> SingleOutputStreamOperator<OUT> transform(
            AbstractMultipleInputTransformation<OUT> transform) {
        return new SingleOutputStreamOperator<>(environment, transform);
    }
}
