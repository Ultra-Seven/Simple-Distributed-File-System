package sdfs.Server;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by lenovo on 2016/10/22.
 */
public class Invocation implements Serializable{
    private static final long serialVersionUID = 1L;
    private String interfaceName;
    private String methodName;
    private Class<?>[] parameterType;
    private Object[] parameters;
    private Object result;

    public String getInterfaceName() {
        return interfaceName;
    }

    public void setInterfaceName(String interfaceName) {
        this.interfaceName = interfaceName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }

    public Class<?>[] getParameterType() {
        return parameterType;
    }

    public void setParameterType(Class<?>[] parameterType) {
        this.parameterType = parameterType;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    @Override
    public String toString() {
        return getInterfaceName() + "." + getMethodName() + "(" + Arrays.toString(getParameters()) + ")";
    }
}
