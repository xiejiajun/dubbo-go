/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package extension

import (
	"github.com/apache/dubbo-go/cluster/router"
)

var (
	routers = make(map[string]func() router.RouterFactory)
)

// SetRouterFactory Set create router factory function by name
func SetRouterFactory(name string, fun func() router.RouterFactory) {
	routers[name] = fun
}

// GetRouterFactory Get create router factory function by name
func GetRouterFactory(name string) router.RouterFactory {
	if routers[name] == nil {
		panic("router_factory for " + name + " is not existing, make sure you have import the package.")
	}
	return routers[name]()
}

// GetRouterFactories Get all create router factory function
func GetRouterFactories() map[string]func() router.RouterFactory {
	return routers
}
