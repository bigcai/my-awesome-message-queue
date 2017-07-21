package io.openmessaging.util;

/**
 *
 * 通用对象工具类
 *
 * @author jason.shang
 */
public class ObjectUtil {

    /**
     * 判断目标对象{@code target}是否为null。
     * @param target 被判断对象是否为null。
     * @return true 目标是null， false 目标对象不为null。
     */
    public static boolean isNull(Object target){
        return target == null;
    }

}
