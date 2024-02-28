/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
* 用于将类标记为公共、稳定接口的注释。
 *
 * <p>带有此注解的类、方法和字段在次要版本（1.0、1.1、
 * 1.2).换句话说，使用@Public带注释类的应用程序将针对较新的类进行编译
 * 同一主要版本的版本。
 *
 * <p>只有主要版本（1.0、2.0、3.0）才能使用此注解破坏接口。
 */
@Documented
@Target(ElementType.TYPE)
@Public
public @interface Public {}
