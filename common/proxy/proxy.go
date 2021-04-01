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

package proxy

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"
)

import (
	"github.com/apache/dubbo-go-hessian2/java_exception"
	perrors "github.com/pkg/errors"
)

import (
	"github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/constant"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/apache/dubbo-go/protocol"
	invocation_impl "github.com/apache/dubbo-go/protocol/invocation"
)

// nolint
type Proxy struct {
	rpc         common.RPCService
	invoke      protocol.Invoker
	callback    interface{}
	attachments map[string]string
	implement   ImplementFunc
	once        sync.Once
}

type (
	// ProxyOption a function to init Proxy with options
	ProxyOption func(p *Proxy)
	// ImplementFunc function for proxy impl of RPCService functions
	ImplementFunc func(p *Proxy, v common.RPCService)
)

var (
	typError = reflect.Zero(reflect.TypeOf((*error)(nil)).Elem()).Type()
)

// NewProxy create service proxy.
func NewProxy(invoke protocol.Invoker, callback interface{}, attachments map[string]string) *Proxy {
	return NewProxyWithOptions(invoke, callback, attachments,
		WithProxyImplementFunc(DefaultProxyImplementFunc))
}

// NewProxyWithOptions create service proxy with options.
func NewProxyWithOptions(invoke protocol.Invoker, callback interface{}, attachments map[string]string, opts ...ProxyOption) *Proxy {
	p := &Proxy{
		invoke:      invoke,
		callback:    callback,
		attachments: attachments,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// WithProxyImplementFunc an option function to setup proxy.ImplementFunc
func WithProxyImplementFunc(f ImplementFunc) ProxyOption {
	return func(p *Proxy) {
		p.implement = f
	}
}

// Implement
// proxy implement
// In consumer, RPCService like:
// 		type XxxProvider struct {
//  		Yyy func(ctx context.Context, args []interface{}, rsp *Zzz) error
// 		}
func (p *Proxy) Implement(v common.RPCService) {
	p.once.Do(func() {
		p.implement(p, v)
		p.rpc = v
	})
}

// Get gets rpc service instance.
func (p *Proxy) Get() common.RPCService {
	return p.rpc
}

// GetCallback gets callback.
func (p *Proxy) GetCallback() interface{} {
	return p.callback
}

// GetInvoker gets Invoker.
func (p *Proxy) GetInvoker() protocol.Invoker {
	return p.invoke
}

// DefaultProxyImplementFunc the default function for proxy impl
func DefaultProxyImplementFunc(p *Proxy, v common.RPCService) {
	// check parameters, incoming interface must be a elem's pointer.
	valueOf := reflect.ValueOf(v)
	logger.Debugf("[Implement] reflect.TypeOf: %s", valueOf.String())

	valueOfElem := valueOf.Elem()
	typeOf := valueOfElem.Type()

	// check incoming interface, incoming interface's elem must be a struct.
	if typeOf.Kind() != reflect.Struct {
		logger.Errorf("%s must be a struct ptr", valueOf.String())
		return
	}

	makeDubboCallProxy := func(methodName string, outs []reflect.Type) func(in []reflect.Value) []reflect.Value {
		return func(in []reflect.Value) []reflect.Value {
			var (
				err    error
				inv    *invocation_impl.RPCInvocation
				inIArr []interface{}
				inVArr []reflect.Value
				reply  reflect.Value
			)
			if methodName == "Echo" {
				methodName = "$echo"
			}

			if len(outs) == 2 {
				if outs[0].Kind() == reflect.Ptr {
					reply = reflect.New(outs[0].Elem())
				} else {
					reply = reflect.New(outs[0])
				}
			} else {
				reply = valueOf
			}

			start := 0
			end := len(in)
			invCtx := context.Background()
			if end > 0 {
				if in[0].Type().String() == "context.Context" {
					if !in[0].IsNil() {
						// the user declared context as method's parameter
						invCtx = in[0].Interface().(context.Context)
					}
					start += 1
				}
				if len(outs) == 1 && in[end-1].Type().Kind() == reflect.Ptr {
					end -= 1
					reply = in[len(in)-1]
				}
			}

			if end-start <= 0 {
				inIArr = []interface{}{}
				inVArr = []reflect.Value{}
			} else if v, ok := in[start].Interface().([]interface{}); ok && end-start == 1 {
				inIArr = v
				inVArr = []reflect.Value{in[start]}
			} else {
				inIArr = make([]interface{}, end-start)
				inVArr = make([]reflect.Value, end-start)
				index := 0
				for i := start; i < end; i++ {
					inIArr[index] = in[i].Interface()
					inVArr[index] = in[i]
					index++
				}
			}

			inv = invocation_impl.NewRPCInvocationWithOptions(invocation_impl.WithMethodName(methodName),
				invocation_impl.WithArguments(inIArr), invocation_impl.WithReply(reply.Interface()),
				invocation_impl.WithCallBack(p.callback), invocation_impl.WithParameterValues(inVArr))

			for k, value := range p.attachments {
				inv.SetAttachments(k, value)
			}

			// add user setAttachment. It is compatibility with previous versions.
			atm := invCtx.Value(constant.AttachmentKey)
			if m, ok := atm.(map[string]string); ok {
				for k, value := range m {
					inv.SetAttachments(k, value)
				}
			} else if m2, ok2 := atm.(map[string]interface{}); ok2 {
				// it is support to transfer map[string]interface{}. It refers to dubbo-java 2.7.
				for k, value := range m2 {
					inv.SetAttachments(k, value)
				}
			}
			// TODO 尽量等待dubbo初始化完成
			ensureStarted(p.invoke)

			// TODO 发起远程调用(层层追溯上去可以找到p.invoke为具体的Protocol实现，比如grpc / dubbo /jsonrpc）
			result := p.invoke.Invoke(invCtx, inv)
			err = result.Error()
			if err != nil {
				// the cause reason
				err = perrors.Cause(err)
				// if some error happened, it should be log some info in the separate file.
				if throwabler, ok := err.(java_exception.Throwabler); ok {
					logger.Warnf("invoke service throw exception: %v , stackTraceElements: %v", err.Error(), throwabler.GetStackTrace())
				} else {
					logger.Warnf("result err: %v", err)
				}
			} else {
				logger.Debugf("[makeDubboCallProxy] result: %v, err: %v", result.Result(), err)
			}
			if len(outs) == 1 {
				return []reflect.Value{reflect.ValueOf(&err).Elem()}
			}
			if len(outs) == 2 && outs[0].Kind() != reflect.Ptr {
				return []reflect.Value{reply.Elem(), reflect.ValueOf(&err).Elem()}
			}
			return []reflect.Value{reply, reflect.ValueOf(&err).Elem()}
		}
	}

	numField := valueOfElem.NumField()
	for i := 0; i < numField; i++ {
		t := typeOf.Field(i)
		methodName := t.Tag.Get("dubbo")
		if methodName == "" {
			methodName = t.Name
		}
		f := valueOfElem.Field(i)
		// TODO 为将要调用的服务(例如：dubbo-go-samples/helloworld/go-client/pkg/user.go:UserProvider)的每一个Func属性都创建动态代理，
		//  并通过reflect.Value.Set函数用创建的动态代理对象替换掉这些Func
		if f.Kind() == reflect.Func && f.IsValid() && f.CanSet() {
			outNum := t.Type.NumOut()

			if outNum != 1 && outNum != 2 {
				logger.Warnf("method %s of mtype %v has wrong number of in out parameters %d; needs exactly 1/2",
					t.Name, t.Type.String(), outNum)
				continue
			}

			// The latest return type of the method must be error.
			if returnType := t.Type.Out(outNum - 1); returnType != typError {
				logger.Warnf("the latest return type %s of method %q is not error", returnType, t.Name)
				continue
			}

			var funcOuts = make([]reflect.Type, outNum)
			for i := 0; i < outNum; i++ {
				funcOuts[i] = t.Type.Out(i)
			}

			// do method proxy here:
			// TODO 通过reflect.MakeFunc实现动态代理， 类似Java的动态代理实现方案
			f.Set(reflect.MakeFunc(f.Type(), makeDubboCallProxy(methodName, funcOuts)))
			logger.Debugf("set method [%s]", methodName)
		}
	}

}

func ensureStarted(invoker protocol.Invoker) {
	var count int
	maxWait := 3
	for {
		// TODO 这里用IsAvailable判断是否初始化完成是不可取的(因为Service注销这里也会变为false)，我们应该为Invoker接口新增一个IsStarted方法用于判断服务是否初始化完成
		//  （或者借助sync.Cond来保证接口config.Load函数执行玩之后dubbogo一定初始化完成?)
		if invoker.IsAvailable() {
			break
		}
		if count > maxWait {
			errMsg := fmt.Sprintf("Failed to check the status of the service %v . No provider available for the service to the consumer use dubbo version %v", refconfig.InterfaceName, constant.Version)
			logger.Error(errMsg)
			break
		}
		time.Sleep(time.Second * 1)
		count++
	}
}
