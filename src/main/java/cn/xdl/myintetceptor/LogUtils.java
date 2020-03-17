package cn.xdl.myintetceptor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

public  class LogUtils {

    public static Boolean dataStart(String log){
        if(log==null){
            return false;
        }
        if(!log.startsWith("{")||!log.endsWith("}")){
            return false;
        }
        return true;
    }

    public static Boolean dataEvent(String log){
        String[] split = log.split("\\|");
        if(split.length!=2){
            return false;
        }
        if(split[0].length()!=18|| NumberUtils.isDigits(split[0])){
            return false;
        }
        if(!split[1].trim().startsWith("{")||!split[1].trim().startsWith("}")){
            return false;
        }
        return false;
    }
}
