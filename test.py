import threading

# 定义函数 B，需要一个参数 test，返回一个结果 out
def func_B(test):
    # do something with test
    out = test * 2
    return out

# 定义函数 A，创建一个线程，处理函数 B
def func_A():
    # 创建一个子线程对象，target 为函数 B，args 为函数 B 的参数
    t = threading.Thread(target=func_B, args=(10,))
    # 启动子线程
    t.start()
    # 等待子线程结束
    t.join()
    # 获取子线程对象的 result 属性，即函数 B 的返回值
    out = t.result
    # 打印结果
    print(out)

# 调用函数 A
func_A()
