package cn.xdl.myintetceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        try {
            String log = new String(body, "utf-8");
            if(log.contains("start")){
                if(LogUtils.dataStart(log)){
                    return event;
                }
            }else {
                if(LogUtils.dataEvent(log)){
                    return event;
                }
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> events = new ArrayList<>();
        for(Event event:list){
            Event intercepted = intercept(event);
            events.add(intercepted);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static  class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
