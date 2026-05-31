package workflow

// TypedRuntime 用类型化名称包装 Runtime，减少业务接入时手写字符串。
type TypedRuntime struct {
	runtime *Runtime
}

// NewTypedRuntime 创建类型化运行时包装。
func NewTypedRuntime(runtime *Runtime) *TypedRuntime {
	return &TypedRuntime{runtime: runtime}
}

// RegisterMachine 注册类型化构建器生成的流程。
func (r *TypedRuntime) RegisterMachine(machine *Machine) {
	r.runtime.RegisterMachine(machine)
}

// RegisterTask 用类型化处理器名注册任务。
func (r *TypedRuntime) RegisterTask(name HandlerName, handler TaskHandler) {
	r.runtime.RegisterTask(string(name), handler)
}

// Runtime 返回底层运行时。需要访问低层能力时可以使用它。
func (r *TypedRuntime) Runtime() *Runtime {
	return r.runtime
}
