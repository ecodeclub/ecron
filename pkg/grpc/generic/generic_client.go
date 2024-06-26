package generic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ecodeclub/ecron/pkg/grpc/generic/desc_source"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"log"
	"sync"
	"time"
)

type GrpcGenericClient struct {
	serviceName string
	conn        *grpc.ClientConn

	descSource  desc_source.DescriptorSource
	serviceDesc *desc.ServiceDescriptor
	callerCache *sync.Map // 缓存调用方法，避免每次反射获取
}

func NewGpcGenericClient(serviceName string, conn *grpc.ClientConn) *GrpcGenericClient {
	return &GrpcGenericClient{
		serviceName: serviceName,
		conn:        conn,
	}
}

func (c *GrpcGenericClient) Init(ctx context.Context) (err error) {
	// fetch service description
	refClient := grpcreflect.NewClientV1Alpha(ctx, grpc_reflection_v1alpha.NewServerReflectionClient(c.conn))
	c.descSource = desc_source.DescriptorSourceFromServer(ctx, refClient)
	dsc, e1 := c.descSource.FindSymbol(c.serviceName)
	if e1 != nil {
		err = fmt.Errorf("service %s not found", c.serviceName)
		if desc_source.IsNotFoundError(e1) {
			log.Println("target server not expose service %q in FindSymbol", c.serviceName)
			return
		}
		log.Println("failed to query for service descriptor %q: %v", c.serviceName, e1)
		return
	}

	sd, ok := dsc.(*desc.ServiceDescriptor)
	if !ok {
		err = fmt.Errorf("service %s not found", c.serviceName)
		log.Println("target server not expose service %q", c.serviceName)
		return
	}
	c.serviceDesc = sd
	c.callerCache = &sync.Map{}
	return
}

func (c *GrpcGenericClient) ServiceName() string {
	return c.serviceName
}

type methodCaller struct {
	Mtd        *desc.MethodDescriptor
	MsgFactory *dynamic.MessageFactory
	Stub       grpcdynamic.Stub
}

func (c *GrpcGenericClient) InvokeUnary(ctx context.Context, method string, reqBytes []byte, opts ...grpc.CallOption) (resp *dynamic.Message, err error) {
	// cache method desc
	caller, err := c.getMethodCaller(method)
	if err != nil {
		return
	}

	reqMessage := caller.MsgFactory.NewMessage(caller.Mtd.GetInputType())
	if reqDynamic, ok := reqMessage.(*dynamic.Message); ok {
		if err = reqDynamic.Unmarshal(reqBytes); err != nil {
			err = fmt.Errorf("unmarshal req bytes error: %s", err.Error())
			return
		}
	}
	ms := time.Now().UnixMilli()
	resp, err = c.invokeUnary0(ctx, method, reqMessage, opts...)
	if delay := time.Now().UnixMilli() - ms; delay > 1000 {
		log.Println("api %s process use %sms\n", method, delay)
	}
	return
}

func (c *GrpcGenericClient) InvokeUnaryJson(ctx context.Context, method string, body map[string]interface{}, opts ...grpc.CallOption) (resp *dynamic.Message, err error) {
	// cache method desc
	caller, err := c.getMethodCaller(method)
	if err != nil {
		return
	}

	reqMessage := caller.MsgFactory.NewMessage(caller.Mtd.GetInputType())
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		return
	}

	if err = reqMessage.(*dynamic.Message).UnmarshalJSON(jsonBytes); err != nil {
		err = fmt.Errorf("unmarshal req bytes error: %s", err.Error())
		return
	}

	return c.invokeUnary0(ctx, method, reqMessage, opts...)
}

func (c *GrpcGenericClient) invokeUnary0(ctx context.Context, method string, request proto.Message, opts ...grpc.CallOption) (resp *dynamic.Message, err error) {
	// cache method desc
	caller, err := c.getMethodCaller(method)
	if err != nil {
		return
	}
	res, err := caller.Stub.InvokeRpc(ctx, caller.Mtd, request, opts...)
	if err != nil {
		return
	}
	if res == nil {
		return
	}
	if r, ok := res.(*dynamic.Message); ok {
		resp = r
		return
	}
	return
}

// getMethodCaller load generic resource from cache
func (c *GrpcGenericClient) getMethodCaller(method string) (caller *methodCaller, err error) {
	val, ok := c.callerCache.Load(method)
	if ok {
		caller = val.(*methodCaller)
	} else {
		// method desc
		caller = &methodCaller{}
		caller.Mtd = c.serviceDesc.FindMethodByName(method)
		if caller.Mtd == nil {
			log.Println("service %q does not include a method named %q", c.serviceName, method)
			err = fmt.Errorf("method %s not found", method)
			return
		}

		// message factory
		var ext dynamic.ExtensionRegistry
		if err = c.fetchAllExtensions(&ext, caller.Mtd.GetInputType()); err != nil {
			return
		}
		if err = c.fetchAllExtensions(&ext, caller.Mtd.GetOutputType()); err != nil {
			return
		}
		caller.MsgFactory = dynamic.NewMessageFactoryWithExtensionRegistry(&ext)

		// stub
		caller.Stub = grpcdynamic.NewStubWithMessageFactory(c.conn, caller.MsgFactory)
		c.callerCache.Store(method, caller)
	}
	return
}

func (c *GrpcGenericClient) fetchAllExtensions(ext *dynamic.ExtensionRegistry, md *desc.MessageDescriptor) (err error) {
	msgTypeName := md.GetFullyQualifiedName()
	if len(md.GetExtensionRanges()) > 0 {
		fds, err := c.descSource.AllExtensionsForType(msgTypeName)
		if err != nil {
			return fmt.Errorf("failed to query for extensions of type %s: %v", msgTypeName, err)
		}
		for _, fd := range fds {
			if err := ext.AddExtension(fd); err != nil {
				return fmt.Errorf("could not register extension %s of type %s: %v", fd.GetFullyQualifiedName(), msgTypeName, err)
			}
		}
	}
	// recursively fetch extensions for the types of any message fields
	for _, fd := range md.GetFields() {
		if fd.GetMessageType() != nil {
			err := c.fetchAllExtensions(ext, fd.GetMessageType())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
